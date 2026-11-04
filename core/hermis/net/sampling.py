import os
import re
import time
import csv
import subprocess
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from threading import Thread, Event
from os.path import expanduser
from shared_utils.extract import extract_loop_to_csv, get_wifi_role, clear_mac_stats, get_last_mcs, get_interface_bytes
from shared_utils.iperf_utils import sync_start, run_iperf_udp
from shared_utils.net_utils import net_conf_check, get_ip_address
import shared_utils.net_utils as model
from shared_utils.log_utils import info, warning, error, success, debug
from config import *
from dataclasses import dataclass

@dataclass
class RuntimeCtx:
    role: str
    interface: str
    local_ip: str
    remote_ip: str
    shelly_ip: str
    shelly_gen: str
    results_dir: str

fieldnames = [
    'timestamp',
    'role',
    'scenario',
    'tx_bytes',
    'rx_bytes',
    'throughput_mbps_tx',
    'throughput_mbps_rx',
    'tx_power',
    'rssi',
    'link_load',
    'cpu_util',
    'cpu_power',
    'p_other',
    'network_power', 
    'energy_per_bit'
]

# Configure transmission power level for the NRC chipset
def set_tx_power(mode, value, cli_path='~/nrc_pkg/script/cli_app'):
    if mode not in ['auto', 'limit', 'fixed']:
        raise ValueError("mode must be one of: 'auto', 'limit', 'fixed'")
    if not (1 <= value <= 30):
        raise ValueError("value must be between 1 and 30")

    cli = expanduser(cli_path)
    subprocess.check_call([cli, 'set', 'txpwr', mode, str(value)])
    info(f"Set TX power to mode='{mode}', value={value}")

# Enable or disable Rate Control (RC)
def set_rc(state, cli_path='~/nrc_pkg/script/cli_app'):
    cli = expanduser(cli_path)
    subprocess.check_call([cli, 'set', 'rc', str(state)])
    info(f"Set Rate Control (RC) to {state}")

def is_peer_reachable(ip, count=1, timeout=1):
    try:
        subprocess.check_output(["ping", "-c", str(count), "-W", str(timeout), ip])
        return True
    except subprocess.CalledProcessError:
        return False
    
def measure_peak_throughput(direction, iperf_role, tx_power, ctx: RuntimeCtx,
                            duration=30, pacing_timer_us=500,
                            low_bw=0.5, high_bw=4.0, resolution=0.1,
                            loss_threshold=1, max_iters=30):

    mult = {"K": 1e-3, "M": 1.0, "G": 1e3}

    # -------------------- helpers --------------------
    _cache = {}  # bw(float) -> (thr_mbps, loss_pct)

    info(f"[PEAKFINDER] dir={direction} role={iperf_role} txpwr={tx_power} "
         f"seed=[{low_bw},{high_bw}] res={resolution}Mbps loss_thr={loss_threshold}% iters<=#{max_iters}")
    
    def _cached_run(bw_mbps, label):
        bw_mbps = float(bw_mbps)
        if bw_mbps in _cache:
            debug(f"[CACHE] Reusing result for {bw_mbps:.3f} Mbps")
            return _cache[bw_mbps]
        r = _run_once(bw_mbps, label=label)
        _cache[bw_mbps] = r
        return r

    def _run_once(bw_mbps, label):
        info(f"[Trial{label}] Test @ {bw_mbps:.2f} Mbps (dir={direction}, role={iperf_role}, txpwr={tx_power})")
        tmp_log = os.path.join(ctx.results_dir, f"peak_{label}_{bw_mbps:.2f}_{direction}_tx{tx_power}.log")

        if not sync_start(iperf_role, ctx.remote_ip):
            error("Sync failed before iperf start.")
            return None, None

        if direction == "tx":
            time.sleep(2)
            cmd = ["iperf3", "-B", ctx.local_ip, "-c", ctx.remote_ip, "-t", str(duration),
                   "-u", "-b", f"{bw_mbps}M", "--pacing-timer", str(pacing_timer_us)]
            parse_tag = "sender"
        elif direction == "rx":
            cmd = ["iperf3", "-B", ctx.local_ip, "-s", "-1"]
            parse_tag = "receiver"
        else:
            error("Invalid direction (use 'tx' or 'rx').")
            return None, None

        debug(f"Executing iperf command: {' '.join(cmd)}")
        with open(tmp_log, "w") as logfile:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in iter(p.stdout.readline, ''):
                print(f"[IPERF] {line.strip()}")
                logfile.write(line)
            p.stdout.close()
            p.wait()

        try:
            with open(tmp_log, "r") as f:
                out = f.read()

            # Throughput parsing
            m = re.findall(r"\[\s*\d+\].*?([\d\.]+)\s+([KMG])bits/sec.*?"+parse_tag, out, flags=re.IGNORECASE)
            if not m:
                error("Could not parse throughput.")
                return None, None
            val, unit = m[-1]
            thr_mbps = float(val) * mult[unit.upper()]
            debug(f"[PARSE] {parse_tag.capitalize()} throughput: {thr_mbps:.3f} Mbps")

            # Loss parsing
            loss_pct = None
            if parse_tag == "sender":
                lm = re.findall(r"(\d+)/(\d+)\s+\(([\d\.]+)%\)", out)
                if lm:
                    try:
                        loss_pct = float(lm[-1][2])
                        debug(f"[PARSE] Packet loss: {loss_pct:.2f}%")
                    except:
                        pass
            else:
                lm = re.search(r"(\d+)/(\d+)\s+\(([\d\.]+)%\)\s+receiver", out)
                if lm:
                    try:
                        loss_pct = float(lm.group(3))
                        debug(f"[PARSE] Receiver packet loss: {loss_pct:.2f}%")
                    except:
                        pass

            return thr_mbps, loss_pct

        except Exception as e:
            error(f"Parsing error: {e}")
            return None, None

    def _ok(loss):
        return (loss is None) or (loss <= loss_threshold)

    def _find_bracket_exponential(seed_bw, max_bw):
        """
        Ξεκινά από seed_bw και διπλασιάζει μέχρι FAIL ή μέχρι max_bw.
        Επιστρέφει (low_ok_bw, high_fail_bw). Αν ποτέ δεν αποτύχει, high=max_bw.
        """
        bw = float(seed_bw)
        # πρώτο probe στο seed
        thr, loss = _cached_run(bw, label=f"bracket_{bw:.3f}")
        if thr is None:
            info("Seed probe failed to parse/complete.")
            return None, None, None, None

        if not _ok(loss):
            # seed ήδη FAIL → κατέβα εκθετικά μέχρι να βρεις OK (προαιρετικό)
            while bw > 0.05:
                nbw = round(bw / 2.0, 3)
                thr2, loss2 = _cached_run(nbw, label=f"bracket_{nbw:.3f}")
                if thr2 is not None and _ok(loss2):
                    return nbw, bw, thr2, loss2  # low OK, high FAIL
                bw = nbw
            return None, None, None, None

        # seed OK → ανέβα εκθετικά
        low_bw_ok = bw
        low_thr_ok, low_loss_ok = thr, loss
        high_bw_fail = None

        while bw < max_bw:
            nbw = round(min(max_bw, bw * 2.0), 3)
            thr2, loss2 = _cached_run(nbw, label=f"bracket_{nbw:.3f}")
            if thr2 is None:
                # Συντηρητικά θεωρούμε FAIL στο nbw
                high_bw_fail = nbw
                break
            if _ok(loss2):
                low_bw_ok, low_thr_ok, low_loss_ok = nbw, thr2, loss2
                bw = nbw
                if nbw >= max_bw:
                    break
            else:
                high_bw_fail = nbw
                break

        if high_bw_fail is None:
            # Δεν είδαμε FAIL μέχρι max_bw → το high είναι το max_bw
            high_bw_fail = max_bw
        return low_bw_ok, high_bw_fail, low_thr_ok, low_loss_ok
    # ------------------ end helpers ------------------

    # guards
    if high_bw <= low_bw:
        warning(f"Invalid search range: low_bw={low_bw}, high_bw={high_bw}.")
        return None
    if resolution <= 0:
        warning(f"Invalid resolution: {resolution}")
        return None

    # --------- 1) Exponential bracketing πάνω στο [low_bw, high_bw] ----------
    seed = float(low_bw)
    lo, hi, thr_lo, loss_lo = _find_bracket_exponential(seed, float(high_bw))
    if lo is None or hi is None:
        info("[PEAKFINDER] Could not establish a valid bracket.")
        return None

    info(f"[PEAKFINDER] bracket: lo_ok={lo} Mbps, hi_fail={hi} Mbps; "
        f"thr@lo={thr_lo}, loss@lo={loss_lo}")

    # Αν το high (== high_bw) έτυχε να είναι επίσης OK και δεν είδαμε FAIL,
    # κάνε early-exit με το thr@high.
    thr_hi, loss_hi = _cache.get(hi, (None, None))
    ok_hi = (thr_hi is not None) and _ok(loss_hi)
    if ok_hi and hi >= float(high_bw):
        info(f"[PEAKFINDER] early-exit: high bound {high_bw} Mbps ήταν OK (no FAIL seen).")
        info(f"✅ Practical peak ({direction.upper()} @ TX={tx_power}): {thr_hi:.2f} Mbps (hit at high bound)")
        return thr_hi

    # αρχικοποίηση για το search
    best_ok = thr_lo if thr_lo is not None else 0.0
    iters = 0

    # --------- 2) Binary search μόνο πάνω στο [lo, hi] ----------
    while (hi - lo) >= resolution and iters < max_iters:
        mid = round((lo + hi) / 2.0, 3)
        thr_mid, loss_mid = _cached_run(mid, label="mid")
        if thr_mid is None:
            hi = mid
            iters += 1
            continue

        if _ok(loss_mid):
            lo = mid
            if thr_mid > best_ok:
                best_ok = thr_mid
        else:
            hi = mid

        iters += 1
        time.sleep(1)

    tried = sorted(_cache.keys())
    debug(f"[PEAKFINDER] tried {len(tried)} bw points: {tried[:10]}{' ...' if len(tried)>10 else ''}")
    info(f"✅ Practical peak ({direction.upper()} @ TX={tx_power}): {best_ok:.2f} Mbps "
        f"(range [{low_bw:.2f}, {high_bw:.2f}], resolution={resolution:.2f} Mbps)")
    return best_ok

# def measure_peak_throughput(direction, iperf_role, tx_power, ctx: RuntimeCtx,
#                             duration=20, pacing_timer_us=500,
#                             low_bw=0.5, high_bw=3.0, resolution=0.1,
#                             loss_threshold=1, max_iters=30):

#     mult = {"K": 1e-3, "M": 1.0, "G": 1e3}

#     def _run_once(bw_mbps, label):
#         info(f"[Trial{label}] Test @ {bw_mbps:.2f} Mbps (dir={direction}, role={iperf_role}, txpwr={tx_power})")
#         tmp_log = os.path.join(ctx.results_dir, f"peak_{label}_{bw_mbps:.2f}_{direction}_tx{tx_power}.log")

#         if not sync_start(iperf_role, ctx.remote_ip):
#             error("Sync failed before iperf start.")
#             return None, None

#         if direction == "tx":
#             time.sleep(2)
#             cmd = ["iperf3", "-B", ctx.local_ip, "-c", ctx.remote_ip, "-t", str(duration),
#                    "-u", "-b", f"{bw_mbps}M", "--pacing-timer", str(pacing_timer_us)]
#             parse_tag = "sender"
#         elif direction == "rx":
#             cmd = ["iperf3", "-B", ctx.local_ip, "-s", "-1"]
#             parse_tag = "receiver"
#         else:
#             error("Invalid direction (use 'tx' or 'rx').")
#             return None, None

#         debug(f"Executing iperf command: {' '.join(cmd)}")
#         with open(tmp_log, "w") as logfile:
#             p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
#             for line in iter(p.stdout.readline, ''):
#                 print(f"[IPERF] {line.strip()}")
#                 logfile.write(line)
#             p.stdout.close()
#             p.wait()

#         try:
#             with open(tmp_log, "r") as f:
#                 out = f.read()

#             # Throughput parsing
#             m = re.findall(r"\[\s*\d+\].*?([\d\.]+)\s+([KMG])bits/sec.*?"+parse_tag, out, flags=re.IGNORECASE)
#             if not m:
#                 error("Could not parse throughput.")
#                 return None, None
#             val, unit = m[-1]
#             thr_mbps = float(val) * mult[unit.upper()]
#             debug(f"[PARSE] {parse_tag.capitalize()} throughput: {thr_mbps:.3f} Mbps")

#             # Loss parsing
#             loss_pct = None
#             if parse_tag == "sender":
#                 lm = re.findall(r"(\d+)/(\d+)\s+\(([\d\.]+)%\)", out)
#                 if lm:
#                     try:
#                         loss_pct = float(lm[-1][2])
#                         debug(f"[PARSE] Packet loss: {loss_pct:.2f}%")
#                     except:
#                         pass
#             else:
#                 lm = re.search(r"(\d+)/(\d+)\s+\(([\d\.]+)%\)\s+receiver", out)
#                 if lm:
#                     try:
#                         loss_pct = float(lm.group(3))
#                         debug(f"[PARSE] Receiver packet loss: {loss_pct:.2f}%")
#                     except:
#                         pass

#             return thr_mbps, loss_pct

#         except Exception as e:
#             error(f"Parsing error: {e}")
#             return None, None

#     if high_bw <= low_bw:
#         warning(f"Invalid search range: low_bw={low_bw}, high_bw={high_bw}.")
#         return None
#     if resolution <= 0:
#         warning(f"Invalid resolution: {resolution}")
#         return None

#     thr_low, loss_low = _run_once(low_bw, label="low")
#     ok_low = (thr_low is not None) and (loss_low is None or loss_low <= loss_threshold)
#     if not ok_low:
#         info(f"Low bound {low_bw:.2f} Mbps is NOT OK (loss={loss_low}). Aborting search.")
#         return None

#     thr_high, loss_high = _run_once(high_bw, label="high")
#     ok_high = (thr_high is not None) and (loss_high is None or loss_high <= loss_threshold)

#     best_ok = thr_low if ok_low else 0.0

#     lo, hi = low_bw, high_bw
#     iters = 0
#     while (hi - lo) >= resolution and iters < max_iters:
#         mid = round((lo + hi) / 2.0, 3)
#         thr_mid, loss_mid = _run_once(mid, label="mid")
#         if thr_mid is None:
#             hi = mid
#             iters += 1
#             continue

#         ok_mid = (loss_mid is None) or (loss_mid <= loss_threshold)

#         if ok_mid:
#             lo = mid
#             if thr_mid > best_ok:
#                 best_ok = thr_mid
#         else:
#             hi = mid

#         iters += 1
#         time.sleep(1)

#     info(f"✅ Practical peak ({direction.upper()} @ TX={tx_power}): {best_ok:.2f} Mbps "
#          f"(range [{low_bw:.2f}, {high_bw:.2f}], resolution={resolution:.2f} Mbps)")
#     return best_ok

def robust_measure_peak(direction, iperf_role, tx_power, ctx: RuntimeCtx):
    peak1 = measure_peak_throughput(direction, iperf_role, tx_power, ctx)
    time.sleep(2)
    peak2 = measure_peak_throughput(direction, iperf_role, tx_power, ctx)
    if peak1 is None:
        return peak2
    if peak2 is None:
        return peak1
    return max(peak1, peak2)

def run_idle_phase(ctx: RuntimeCtx, duration=IDLE_DURATION):
    """
    Collects power/CPU/throughput stats with no traffic (IDLE).
    Writes a dedicated CSV with the same columns.
    """
    csv_path = os.path.join(ctx.results_dir, f"{ctx.role.lower()}_idle_baseline.csv")
    info(f"\n=== IDLE phase: duration={duration}s (no iperf) ===")
    with open(csv_path, "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        info(f"[THREAD] idle_sampling args: duration={duration}, "
             f"power_ip={ctx.shelly_ip}, power_type={ctx.shelly_gen}, peak=1.0, "
             f"interface={ctx.interface}, sample_interval={SAMPLE_INTERVAL}")

        sampling_thread = Thread(
            target=extract_loop_to_csv,
            args=(writer, duration, "idle", ctx.shelly_ip, ctx.shelly_gen, 1.0, ctx.interface, SAMPLE_INTERVAL)
        )
        sampling_thread.start()
        sampling_thread.join()

    success("IDLE phase complete.")

def run_full_sweep(phase, iperf_role, ctx: RuntimeCtx):
    csv_path = os.path.join(ctx.results_dir, f"{ctx.role.lower()}_sweep_{phase}.csv")
    with open(csv_path, "w", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        set_rc("on")
        direction = phase

        for tx_power in TX_POWER_LIST:
            set_tx_power("fixed", tx_power)
            info(f"\n=== [SWEEP] TX_POWER={tx_power} dBm | dir={direction.upper()} | role={iperf_role} ===")

            peak = robust_measure_peak(direction, iperf_role, tx_power, ctx)
            if peak is None:
                warning(f"[SWEEP] peak=None → SKIP tx_power={tx_power}")
                continue
            info(f"[SWEEP] measured_peak={peak:.2f} Mbps; loads={BANDWIDTH_PERCENTAGES}")

            for pct in BANDWIDTH_PERCENTAGES:
                rate = round(peak * pct, 2) if peak else None
                info(f"→ Load: {pct*100:.1f}%, Bandwidth={rate} Mbps")

                for attempt in range(3):
                    if is_peer_reachable(ctx.remote_ip): break
                    warning(f"[SWEEP] peer unreachable (try {attempt+1}/3) → backoff 1s")
                    time.sleep(1)
                else:
                    warning("[SWEEP] peer unreachable (final) → SKIP this load")
                    continue

                ok=False
                for attempt in range(3):
                    if sync_start(iperf_role, ctx.remote_ip): 
                        ok = True
                        break
                    warning(f"[SWEEP] sync failed (try {attempt+1}/3) → backoff 1s")
                    time.sleep(1)
                if not ok:
                    warning("[SWEEP] sync failed (final) → SKIP this load")
                    continue

                info(f"[THREAD] iperf_thread args: role={iperf_role}, remote_ip={ctx.remote_ip}, "
                     f"local_ip={ctx.local_ip}, duration={DURATION}, bandwidth={rate} Mbps")

                iperf_args = (iperf_role, ctx.remote_ip, ctx.local_ip, DURATION)
                iperf_kwargs = {'bandwidth': f"{rate}M" if rate else None, 'log_prefix': 'iperf'}

                iperf_thread = Thread(target=run_iperf_udp, args=iperf_args, kwargs=iperf_kwargs)
                iperf_thread.start()

                info(f"Warm-up: sleeping for {WARMUP}s")
                time.sleep(WARMUP)

                info(f"[THREAD] sampling_thread args: duration={DURATION - WARMUP}, direction={direction}, "
                     f"power_ip={ctx.shelly_ip}, power_type={ctx.shelly_gen}, peak={peak}, "
                     f"interface={ctx.interface}, sample_interval={SAMPLE_INTERVAL}")

                sampling_thread = Thread(target=extract_loop_to_csv, args=(
                    writer, DURATION - WARMUP, direction, ctx.shelly_ip, ctx.shelly_gen, peak,
                    ctx.interface, SAMPLE_INTERVAL
                ))
                sampling_thread.start()

                iperf_thread.join()
                sampling_thread.join()

                info(f"Cooldown: sleeping for {COOLDOWN}s")
                time.sleep(COOLDOWN)
                
# --- main entrypoint (no modes, full sampling workflow) ----------------------
def main():
    # Ensure results directory exists
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Clear NIC statistics and load CPU model
    clear_mac_stats()  # reset NIC counters before starting
    model.load_cpu_model(CPU_MODEL_JSON)

    # Bind current device to MQTT/Shelly mapping (from config)
    MQTT_TOPIC, SHELLY_IP, SHELLY_GEN = net_conf_check()
    if not all([MQTT_TOPIC, SHELLY_IP, SHELLY_GEN]):
        error("Shelly binding failed (mapping/detection). Exiting.")
        raise SystemExit(1)

    # Determine role (AP or STA) and set remote peer IP
    LOCAL_IP = get_ip_address(INTERFACE)
    role = get_wifi_role(INTERFACE)  # returns 'AP' or 'STA'
    if role == "AP":
        REMOTE_IP = IP_STA
    elif role == "STA":
        REMOTE_IP = IP_AP
    else:
        error(f"Unknown role for IP {LOCAL_IP} on iface {INTERFACE}")
        raise SystemExit(1)

    info(f"Role={role} LocalIP={LOCAL_IP} RemoteIP={REMOTE_IP} Iface={INTERFACE}")

    ctx = RuntimeCtx(
        role=role,
        interface=INTERFACE,
        local_ip=LOCAL_IP,
        remote_ip=REMOTE_IP,
        shelly_ip=SHELLY_IP,
        shelly_gen=SHELLY_GEN,
        results_dir=RESULTS_DIR
    )

    # Track total execution time
    start_total = time.time()

    # 1) Idle baseline phase
    #    Collects background power consumption without traffic
    run_idle_phase(ctx)

    # 2) Traffic sweeps for both TX and RX directions
    #    The sequence depends on whether the current device is AP or STA
    if role == "AP":
        # TX sweep: AP sends traffic to STA
        info("Starting sweep: AP → STA (TX)")
        t0 = time.time()
        run_full_sweep("tx", "client", ctx)
        info(f"Sweep TX took {time.time()-t0:.2f}s")

        # RX sweep: AP receives traffic from STA
        info("Starting sweep: STA → AP (RX)")
        t0 = time.time()
        run_full_sweep("rx", "server", ctx)
        info(f"Sweep RX took {time.time()-t0:.2f}s")

    else:  # STA
        # RX sweep: STA receives traffic from AP
        info("Starting sweep: AP → STA (RX)")
        t0 = time.time()
        run_full_sweep("rx", "server", ctx)
        info(f"Sweep RX took {time.time()-t0:.2f}s")

        # TX sweep: STA sends traffic to AP
        info("Starting sweep: STA → AP (TX)")
        t0 = time.time()
        run_full_sweep("tx", "client", ctx)
        info(f"Sweep TX took {time.time()-t0:.2f}s")

    # Print total execution time
    dur = time.time() - start_total
    success("Bidirectional sweep complete.")
    info(f"Total duration: {dur:.2f} seconds ({int(dur//60)} min {int(dur%60)} sec)")

# Entry point guard
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        warning("Interrupted by user (Ctrl+C). Exiting.")
        raise SystemExit(130)
