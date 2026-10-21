import os
import time
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import threading
import datetime as _dt
import csv
import socket
import fcntl
import struct
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional
from influxdb import InfluxDBClient as _InfluxDBClient
from zoneinfo import ZoneInfo

from config import (
    SAMPLE_DURATION,
    SAMPLES_PER_LEVEL,
    CPU_LEVELS,
    OUTPUT_DIR,
    MODEL_DIR,
    RASPBERRY_MAPPING,
)
from shared_utils.cpu_utils import (
    detect_device,
    read_jetson_power_tool,
    read_upboard_power_tool,
    get_ip_address,
    stress_cpu_custom,
    stop_stress,
    get_cpu_utilization,
    collect_samples_freq,
)
from shared_utils.regression import save_model, fit_through_origin
from sensors.shelly import detect_shelly_generation
from shared_utils.log_utils import info, success, warning, error
from dotenv import load_dotenv
env_path ='/home/greendigit/app/greendigit-l3-iot-uth-deployment/.env'
load_dotenv(env_path)

# ========= Schema & types =========
class CsvSchema:
    LEVEL = "Level_%"
    PWR = "AvgPower_W"
    FREQ = "AvgFreq_MHz"
    VOLT = "AvgVolt_V"
    TEMP = "AvgTemp_C"
    N = "Samples_Kept"

@dataclass
class LevelStats:
    avg_power_w: float
    avg_freq_khz: Optional[float]
    n: int


StatsPerLevel = Dict[int, LevelStats]
# ==================================

# ---- Influx v1 setup (βάλε password μέσω ENV) --------------------------------
INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_PORT = int(os.getenv("INFLUX_PORT"))
INFLUX_DB   = os.getenv("INFLUX_DB_POWER")
INFLUX_USER = os.getenv("INFLUX_USER")
INFLUX_PASS = os.getenv("INFLUX_PASS")

MEAS        = os.getenv("MEAS", "mqtt_consumer")
CPU_FIELD   = os.getenv("CPU_FIELD", "cpu_usage")

# Μένουμε αυστηρά σε *bitrate* fields (όχι mW)
TX_FIELDS   = os.getenv("TX_FIELDS", "tx_bitrate_mbps,tx_bitrate_kbps").split(",")
RX_FIELDS   = os.getenv("RX_FIELDS", "rx_bitrate_mbps,rx_bitrate_kbps").split(",")
PWR_FIELDS  = os.getenv("POWER_FIELDS","power,total_consumption_watts").split(",")
CPUF_FIELDS = os.getenv("CPUF_FIELDS", "cpu_freq").split(",")


# ---- Μικρό logger που συνεργάζεται με τα info/warning/error/success αν υπάρχουν
def _mklogger():
    def _wrap(name, fallback):
        fn = globals().get(name)
        def _log(msg):
            try:
                if callable(fn):
                    fn(msg)
                else:
                    print(fallback + msg)
            except Exception:
                print(fallback + msg)
        return _log
    return (
        _wrap("info",    "[INFO] "),
        _wrap("warning", "[WARN] "),
        _wrap("error",   "[ERROR] "),
        _wrap("success", "[ OK ] "),
        _wrap("debug",   "[DBG] ")
    )
_linfo, _lwarn, _lerr, _lok, _ldbg = _mklogger()

# ---- Helpers -----------------------------------------------------------------
def _field_keys(client, measurement):
    res = client.query(f'SHOW FIELD KEYS FROM "{measurement}"')
    keys = set()
    for _m, rows in (res.items() if res else []):
        for r in rows:
            k = r.get("fieldKey")
            if k: keys.add(k)
    return keys


def _first_present(keys, candidates):
    for c in candidates:
        if c in keys: return c
    return None

def _to_mbps(val, fieldname):
    # Αν είναι *_kbps → /1000, αν *_mbps → όπως είναι
    f = fieldname.lower()
    v = float(val)
    if f.endswith("_kbps"): return v / 1000.0
    return v  # *_mbps ή οτιδήποτε άλλο θεωρούμε ήδη Mbps

def _q_tag(s: str) -> str:
    # Escape μονού αποστρόφου για ασφαλές InfluxQL quoting
    return s.replace("'", r"\'")



def get_idle_power_and_freq_from_influx(
    sample_count=50,
    lookback="30d",
    win_s=5,                 # παράθυρο ±win_s γύρω από CPU point
    cpu_thresh=1.0,          # CPU < 1 (%)
    rate_thresh_mbps=0.015,   # TX/RX < 0.01 Mbps
    prefer="nearest",        # ή "minrate"
    export_csv_path=None,
    mqtt_topic_filter : str = None
):
    if _InfluxDBClient is None:
        raise RuntimeError("Missing 'influxdb' client. Run: pip install influxdb")
    if not INFLUX_PASS:
        raise RuntimeError("Set INFLUX_PASS (export INFLUX_PASS='...').")

    client = _InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT,
                             username=INFLUX_USER, password=INFLUX_PASS,
                             database=INFLUX_DB, timeout=5)

    fkeys = _field_keys(client, MEAS)

    tx_field    = _first_present(fkeys, TX_FIELDS)
    rx_field    = _first_present(fkeys, RX_FIELDS)
    power_field = _first_present(fkeys, PWR_FIELDS) or PWR_FIELDS[0]
    freq_field   = _first_present(fkeys, CPUF_FIELDS) or CPUF_FIELDS[0]


    if not tx_field or not rx_field:
        raise RuntimeError(f"TX/RX fields not found in '{MEAS}'. (tx={tx_field}, rx={rx_field})")
    
    topic_tag = "topic"
    tag_filter = ""
    if mqtt_topic_filter:
        tag_filter = f' AND "{topic_tag}" = \'{_q_tag(mqtt_topic_filter)}\''
        _linfo(f"[PIDLE] Tag filter: {topic_tag}='{mqtt_topic_filter}'")

    _linfo(f"[PIDLE] Using fields: CPU={CPU_FIELD}, TX={tx_field}, RX={rx_field}, POWER={power_field}, FREQ={freq_field}")
    _linfo(f"[PIDLE] Thresholds: CPU<{cpu_thresh} %, TX/RX<{rate_thresh_mbps} Mbps, window=±{win_s}s")

    # 1) Πάρε ΠΟΛΛΑ CPU points (με την ίδια σειρά που βλέπεις στο Grafana)
    q_cpu = (
        f'SELECT time,"{CPU_FIELD}" FROM "{MEAS}" '
        f'WHERE "{CPU_FIELD}" < {cpu_thresh} AND time > now()-{lookback}{tag_filter} '
        f'ORDER BY time DESC LIMIT {sample_count*100}'
    )
    res = client.query(q_cpu)
    cpu_pts = list(res.get_points()) if res else []
    _linfo(f"[PIDLE] Candidate CPU points fetched: {len(cpu_pts)}")

    LOCAL_TZ = ZoneInfo("Europe/Athens")
    def _to_dt(s): return _dt.datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(LOCAL_TZ)

    picked, powers, freqs_Mhz = [], [], []

    for i, row in enumerate(cpu_pts):
        t_iso = row["time"]
        cpu_v = float(row.get(CPU_FIELD, 0) or 0)
        _ldbg(f"[CPU OK] #{i+1} t=[{_to_dt(t_iso):%Y-%m-%d %H:%M:%S %Z}] cpu={cpu_v:.4f}% (<{cpu_thresh}%)")


        # 2) Παράθυρο γύρω από t
        q_w = (
            f'SELECT time,"{tx_field}","{rx_field}","{power_field}","{freq_field}" FROM "{MEAS}" '
            f'WHERE time >= \'{t_iso}\' - {win_s}s AND time <= \'{t_iso}\' + {win_s}s{tag_filter} '
            f'ORDER BY time ASC'
        )
        r = client.query(q_w)
        win = list(r.get_points()) if r else []
        if not win:
            _ldbg(f"[SKIP] No points in window for t={t_iso}")
            continue

        # 3) Υποψήφια με TX/RX κάτω από thresholds (όλα σε Mbps)
        cands = []
        for p in win:
            tx_mbps = p.get(tx_field)
            rx_mbps = p.get(rx_field)
            pw = p.get(power_field)
            fq = p.get(freq_field)
            if tx_mbps is None or rx_mbps is None or pw is None or fq is None:
                continue
            if (abs(tx_mbps) < rate_thresh_mbps) and (abs(rx_mbps) < rate_thresh_mbps):
                cands.append(p)
        if not cands:
            _ldbg(f"[NET FAIL] No TX/RX<{rate_thresh_mbps} Mbps point in ±{win_s}s for t={t_iso}")
            continue

        # 4) Διάλεξε σημείο (πλησιέστερο ή με ελάχιστο rate)
        if prefer == "minrate":
            best = min(cands, key=lambda p: abs(_to_mbps(p.get(tx_field,0), tx_field)) +
                                     abs(_to_mbps(p.get(rx_field,0), rx_field)))
        else:
            t_cpu = _to_dt(t_iso)
            best = min(cands, key=lambda p: abs(_to_dt(p["time"]) - t_cpu))

        tx_b = float(best.get(tx_field, 0))
        rx_b = float(best.get(rx_field, 0))
        pw   = float(best[power_field])
        fk   = float(best[freq_field])
        dt_s = abs((_to_dt(best["time"]) - _to_dt(t_iso)).total_seconds())


        _ldbg(f"[NET OK]  t_pick= [{_to_dt(best['time']):%Y-%m-%d %H:%M:%S %Z}] "
              f"Δt={dt_s:.2f}s  tx={tx_b:.5f} Mbps, rx={rx_b:.5f} Mbps, f_idle≈{fk:.2f} MHz")
        _linfo(f"[SAVE]    CPU power sample = {pw:.4f} W | f={fk:.2f} MHz "
               f"@ {best['time']} [{_to_dt(best['time']):%Y-%m-%d %H:%M:%S %Z}]")

        picked.append({
            "t_cpu": t_iso,
            "t_pick": best["time"],
            "power_w": pw,
            "freq_khz": fk,
            "tx_mbps": tx_b,
            "rx_mbps": rx_b,
            "cpu_pct": cpu_v,
            "dt_s": dt_s,
        })
        powers.append(pw)
        freqs_Mhz.append(fk)

        if len(picked) >= sample_count:
            break

    if not picked:
        _lerr("[PIDLE] Failed: no samples met CPU/NET thresholds inside windows.")
        raise RuntimeError("Δεν βρέθηκαν δείγματα με τα δοθέντα κριτήρια.")

    if len(picked) < sample_count:
        _lwarn(f"[PIDLE] Collected only {len(picked)}/{sample_count} samples. Proceeding with mean anyway.")

    p_idle_mean = float(np.mean(powers))
    f_idle_mean_Mhz = float(np.mean(freqs_Mhz)) if freqs_Mhz else None
    _lok(f"[PIDLE] mean={p_idle_mean:.4f} W | f_idle≈{(f_idle_mean_Mhz or 0):.2f} MHz "
         f"from {len(picked)} samples")

    if export_csv_path:
        try:
            with open(export_csv_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["t_cpu","t_pick","dt_s","cpu_pct","tx_mbps","rx_mbps","power_w","freq_khz"])
                w.writeheader()
                w.writerows(picked)
            _linfo(f"[PIDLE] Saved samples CSV → {export_csv_path}")
        except Exception as e:
            _lwarn(f"[PIDLE] Failed to write CSV ({export_csv_path}): {e}")

    return p_idle_mean, f_idle_mean_Mhz, picked

def configure_cpu_governor(governor: str = "ondemand") -> None:
    cpu_dirs = sorted(Path("/sys/devices/system/cpu").glob("cpu[0-9]*"))
    cpu_dirs = [d for d in cpu_dirs if (d / "cpufreq").exists()]
    if not cpu_dirs:
        warning("No cpufreq paths found. Are you running as root?")
        return

    policy0 = Path("/sys/devices/system/cpu/cpufreq/policy0")
    if policy0.exists():
        min_f = int((policy0 / "cpuinfo_min_freq").read_text().strip())
        max_f = int((policy0 / "cpuinfo_max_freq").read_text().strip())
    else:
        min_f = int((cpu_dirs[0] / "cpufreq/cpuinfo_min_freq").read_text().strip())
        max_f = int((cpu_dirs[0] / "cpufreq/cpuinfo_max_freq").read_text().strip())

    for d in cpu_dirs:
        gov = d / "cpufreq/scaling_governor"
        minp = d / "cpufreq/scaling_min_freq"
        maxp = d / "cpufreq/scaling_max_freq"
        try:
            gov.write_text(governor + "\n")
            minp.write_text(str(min_f) + "\n")
            maxp.write_text(str(max_f) + "\n")
        except PermissionError:
            error("Permission denied. Run the script as root.")
            return

    success(f"Set governor={governor}, range={min_f/1000:.0f}–{max_f/1000:.0f} MHz")


def sample_cpu_and_power(duration, power_reader):
    """Measure CPU% and Power over the same window."""
    result = {}

    def collect_cpu():
        result["cpu"] = get_cpu_utilization(duration)

    def collect_power():
        result["power"] = power_reader()

    t1 = threading.Thread(target=collect_cpu)
    t2 = threading.Thread(target=collect_power)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    return result.get("cpu"), result.get("power")


def validate_with_tools(
    a: float,
    b: float,
    levels,
    samples_per_level: int,
    sample_duration: int,
    tolerance: float,
) -> None:
    """
    Validate: P_total ≈ P_idle + a * CPU(%)
    Saves validation CSV and prints MAE/RMSE/R2.
    """
    info("\n=== Validation (tegrastats/turbostat) ===")

    device = detect_device()
    if device == "jetson":
        power_fn = lambda: read_jetson_power_tool()
    elif device == "upboard":
        power_fn = lambda: read_upboard_power_tool()
    else:
        warning("Unknown device; skipping validation.")
        return

    procs, p_act, p_pred = [], [], []

    for level in levels:
        stress = None
        if level == 0:
            info("→ IDLE validation")
        else:
            info(f"→ Stress {level}%")
            stress = stress_cpu_custom(level)
            time.sleep(3)

        collected = attempts = 0
        while collected < samples_per_level and attempts < samples_per_level * 3:
            attempts += 1
            cpu, pw = sample_cpu_and_power(sample_duration, power_fn)
            if pw is None:
                warning("Skipped: power read failed")
                continue

            ok = (cpu is not None) and (level - tolerance <= cpu <= level + tolerance)
            if ok:
                pred = b + a * cpu
                info(
                    f"level={level} sample={collected+1}/{samples_per_level} "
                    f"cpu={cpu:.2f}% p_act={pw:.3f}W p_pred={pred:.3f}W status=accepted"
                )
                procs.append(cpu)
                p_act.append(pw)
                p_pred.append(pred)
                collected += 1
            else:
                warning(
                    f"level={level} cpu={cpu:.2f}% status=rejected reason=outside_tolerance"
                )

        if stress:
            stop_stress(stress)

    if p_act:
        df_val = pd.DataFrame(
            {
                "CPU_Usage_Percent": procs,
                "Power_Actual_W": p_act,
                "Power_Pred_W": p_pred,
            }
        )
        val_path = os.path.join(OUTPUT_DIR, "validation_results.csv")
        df_val.to_csv(val_path, index=False)
        success(f"Saved validation results: {val_path}")

        err = np.array(p_act) - np.array(p_pred)
        mae = float(np.mean(np.abs(err)))
        rmse = float(np.sqrt(np.mean(err**2)))
        ss_res = float(np.sum(err**2))
        ss_tot = float(np.sum((np.array(p_act) - np.mean(p_act)) ** 2))
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        info("\n=== Validation Metrics ===")
        info(f"MAE  : {mae:.4f} W")
        info(f"RMSE : {rmse:.4f} W")
        info(f"R²   : {r2:.4f}")
    else:
        warning("Validation: no valid samples were collected.")

def get_ip_address(ifname):
    """Get IP address assigned to a specific network interface."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ip_addr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )[20:24])
        return ip_addr
    except OSError:
        return None


def main() -> None:
    # Folders
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)

    my_ip = get_ip_address('eth0')

    # Bias
    BIAS = 0

    info("Detecting Device...")
    DEVICE = detect_device()
    success(f"Device detected: {DEVICE}")

    info("Checking network configuration...")
    interface = "enp2s0" if DEVICE == "upboard" else "eth0"
    my_ip = get_ip_address(interface)
    if not my_ip:
        error(f"Could not determine IP address for interface '{interface}'. Exiting.")
        raise RuntimeError("Missing IP address")
    success(f"Detected IP address: {my_ip}")

    if my_ip not in RASPBERRY_MAPPING:
        warning(f"IP address {my_ip} not found in mapping. Exiting.")
        raise RuntimeError("IP not in RASPBERRY_MAPPING")

    mqtt_topic, shelly_ip = RASPBERRY_MAPPING[my_ip]
    shelly_gen = detect_shelly_generation(shelly_ip)
    if not shelly_gen:
        error("Could not detect Shelly plug generation. Exiting.")
        raise RuntimeError("Missing Shelly generation")

    info("=== Node Configuration ===")
    info(f"  Local IP       : {my_ip}")
    info(f"  MQTT Topic     : {mqtt_topic}")
    info(f"  Smart Meter IP : {shelly_ip}")
    info(f"  Shelly Version : {shelly_gen}")
    info("===========================")

    configure_cpu_governor("ondemand")

    info("Sleeping for 5 seconds...")
    time.sleep(5)
    info("Estimating PIdle from idle sampling...")


    try:
        p_idle, f_idle_Mhz, pts = get_idle_power_and_freq_from_influx(
            sample_count=50,
            lookback="30d",
            win_s=5,                 # παράθυρο ±5s
            cpu_thresh=1.0,          # CPU < 1%
            rate_thresh_mbps=0.015,   # TX/RX < 0.01 Mbps
            prefer="nearest",        # ή "minrate"
            export_csv_path="/tmp/pidle_points.csv",  # προαιρετικά
            mqtt_topic_filter=mqtt_topic,
        )
        success(
            f"PIdle estimated: {p_idle:.4f} W"
            + (f" | Idle Avg Freq: {f_idle_Mhz:.0f} MHz" if f_idle_Mhz else "")
        )
    except Exception as e:
        error(f"PIdle derivation failed: {e}")  # ή print(...)


    # Main sampling
    info("Starting CPU power modeling experiment...")
    start_time = time.perf_counter()

    TOLERANCE = 2.5
    info(f"Started sampling on node: {my_ip} with Shelly @ {shelly_ip}")
    # X, y_total = collect_samples(CPU_LEVELS, SAMPLES_PER_LEVEL, SAMPLE_DURATION, TOLERANCE, shelly_ip, shelly_gen)
    X, y_total, f_khz, v_v, t_c, stats_per_level = collect_samples_freq(
        CPU_LEVELS, SAMPLES_PER_LEVEL, SAMPLE_DURATION, TOLERANCE, shelly_ip, shelly_gen
    )

    y_cpu = y_total - p_idle

    # Save raw samples
    csv_path = os.path.join(OUTPUT_DIR, "cpu_power_samples.csv")
    try:
        with open(csv_path, "w", newline="") as f:
            pd.DataFrame(
                {
                    "CPU_Usage_Percent": X,
                    "Ptotal_W": y_total,
                    "Pcpu_W": y_total - p_idle,
                    "AvgFreq_MHz": f_khz / 1000.0,
                    "Voltage_V": v_v,                # NEW
                    "Temp_C": t_c,                   # NEW
                }
            ).to_csv(csv_path, index=False)
        success(f"Saved {len(X)} samples to: {csv_path}")
    except Exception as e:
        error(f"Error saving CSV file: {e}")

    # Ratio from in-memory stats at a reference load
    LOAD_FOR_RATIO = 100
    if not stats_per_level or LOAD_FOR_RATIO not in stats_per_level:
        raise RuntimeError(
            f"[ERROR] No per-level stats for load={LOAD_FOR_RATIO}%"
        )

    avg_power = stats_per_level[LOAD_FOR_RATIO]["avg_power_w"]
    avg_freq_khz = stats_per_level[LOAD_FOR_RATIO]["avg_freq_khz"]

    if avg_power is None:
        raise RuntimeError(
            f"[ERROR] Missing AvgPower_W for load={LOAD_FOR_RATIO}%"
        )
    if f_idle_Mhz is None:
        raise RuntimeError("[ERROR] Missing f_idle_Mhz from idle sampling")
    if not avg_freq_khz or avg_freq_khz <= 0:
        raise RuntimeError(
            f"[ERROR] Invalid avg_freq_khz={avg_freq_khz} at load={LOAD_FOR_RATIO}%"
        )

    avg_freq_mhz = avg_freq_khz / 1000.0
    # ratio = (f_idle_mhz / avg_freq_mhz) * (avg_power - p_idle)
    info(f"Ratio inputs: f_idle_mhz={f_idle_Mhz:.2f}, "
        f"avg_freq_mhz={avg_freq_mhz:.2f}, "
        f"avg_power={avg_power:.4f} W, "
        f"p_idle={p_idle:.4f} W")

    offset = (f_idle_Mhz / avg_freq_mhz) * (avg_power - p_idle)
    info(f"Offset calculated: {offset:.6f}")

    # Fit model (intercept = p_idle)
    a = fit_through_origin(X, y_cpu)
    b = offset - BIAS  # keep if you need to pass around; otherwise you can drop 'b'

    # Save per-level aggregates
    try:
        if stats_per_level:
            rows = []
            for lvl, s in sorted(stats_per_level.items()):
                rows.append(
                    {
                        CsvSchema.LEVEL: lvl,
                        CsvSchema.PWR: s["avg_power_w"],
                        CsvSchema.FREQ: (s["avg_freq_khz"] / 1000.0) if s["avg_freq_khz"] else None,
                        CsvSchema.VOLT: s.get("avg_volt_v"),
                        CsvSchema.TEMP: s.get("avg_temp_c"),
                        CsvSchema.N: s["n"],
                    }
                )
            df_lvl = pd.DataFrame(rows)
            sum_path = os.path.join(OUTPUT_DIR, "per_level_summary.csv")
            df_lvl.to_csv(sum_path, index=False)
            success(f"Saved per-level summary: {sum_path}")
        else:
            warning("No per-level stats to save (empty stats_per_level).")
    except Exception as e:
        error(f"Failed to save per-level summary: {e}")

    # Preview
    info("\n=== Final Model Summary ===")

    # Decompose total power the way you want it printed
    pvar = p_idle - b  # Pvar = Pidle - b

    info(f"Pvar  = {pvar:.4f} W")
    info(f"Pcpu  = {a:.4f}*CPU(%) + {b:.4f}")

    # Save model JSON
    p_other = round(pvar, 4)
    model_path = os.path.join(MODEL_DIR, "cpu_info.json")
    try:
        save_model(a, b, p_other, model_path)
    except Exception as e:
        error(f"Failed to save model: {e}")

    def estimate_cpu_power(cpu_percent: float) -> float:
        # Pcpu = a*CPU(%) + b
        return a * cpu_percent + b

    def estimate_total_power(cpu_percent: float) -> float:
        # Ptotal = Pcpu + Pvar, with Pvar = Pidle - b
        return estimate_cpu_power(cpu_percent) + pvar

    for cpu in [0, 25, 50, 75, 100]:
        p_cpu = estimate_cpu_power(cpu)
        p_tot = estimate_total_power(cpu)
        info(f"@ {cpu:>3}%  -> Pcpu: {p_cpu:.3f} W | Ptotal: {p_tot:.3f} W")

    success("\nCPU power modeling process completed successfully.")

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    minutes, seconds = divmod(elapsed, 60)
    success(f"Total execution time: {int(minutes)}m {seconds:.2f}s")

    # Validation
    if DEVICE != "raspberrypi":
        info("Starting CPU power validation experiment...")
        VALIDATION_LEVELS = list(range(0, 101, 1))
        validate_with_tools(
            a=a,
            b=b,
            levels=VALIDATION_LEVELS,
            samples_per_level=10,
            sample_duration=SAMPLE_DURATION,
            tolerance=TOLERANCE,
        )
    else:
        info("Skipping validation on Raspberry Pi.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error(f"Fatal: {e}")
        raise
