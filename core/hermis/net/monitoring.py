"""
HERMIS — Monitoring (single script)
- Init: paths/config, Shelly binding, CPU model, role/IPs, RC on, TX power auto, RSSI,
        LinkCapacityProvider, energy/throughput models, idle_mean.
- Loop (every 4s): bytes→throughput, scenario, CPU util (no MA), MCS→TX power (autotxgain),
                   RSSI, capacity (TX by TX power / RX by RSSI), link_load, E/bit predictions,
                   P_idle, P_tx/P_rx, P_total (Shelly), P_cpu, P_net(measured), model comparison.
"""

from dataclasses import dataclass
from typing import Optional, Any, Dict, Tuple
from collections import deque
import os, time, re, csv, argparse, signal, subprocess
import pandas as pd # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')

# --- Project config & utils ---
from config import (RESULTS_DIR, DATA_DIR, MODELS_DIR, INTERFACE, IP_AP, IP_STA, SYNC_SLEEP, CPU_MODEL_JSON, SAMPLE_INTERVAL)
from shared_utils.log_utils import info, warning, error, success, debug
from shared_utils.net_utils import (net_conf_check, get_ip_address, get_cpu_utilization,get_power, estimate_cpu_power)
import shared_utils.net_utils as cpu_model
import shared_utils.net_utils as nu
from shared_utils.extract import (get_wifi_role, get_rssi_from_iw, get_interface_bytes,infer_activity, get_last_mcs, clear_mac_stats, get_tx_power_from_cli)
from sampling import set_rc, set_tx_power

# Link capacity provider (Q95: TX keyed by TX power, RX keyed by RSSI)
from shared_utils.link_capacity import LinkCapacityProvider

# Προσπαθούμε joblib πρώτα για .pkl models
try:
    from joblib import load as joblib_load # type: ignore
except Exception:
    joblib_load = None
import pickle

# --- Alerting thresholds / config ---
ALERT_CFG = {
    "RSSI_TOL_DB": 15.0,       # RSSI baseline tolerance (± dB)
    "THR_REL_ERR": 25.00,      # Throughput relative error tolerance (± fraction)
    "SUSTAINED": 3,           # Stop after this many alert "events" (across metrics)
    "K_OF_N": (5, 10),         # Trigger one event if >=K out of last N samples are out-of-bounds
    "COOLDOWN_THR": 5,        # After a TP event, skip TP checks for N samples
    "COOLDOWN_RSSI": 5,       # After an RSSI event, skip RSSI checks for N samples
    "MIN_TOBS_Mbps": 0.01,    # Ignore very small observed throughput (noise floor)
}

# ------------------------ Dataclass για το state ----------------------------

@dataclass
class MonitoringState:
    # --- περιβάλλον ---
    role: str
    interface: str
    local_ip: str
    remote_ip: str
    shelly_ip: str
    shelly_gen: str
    sample_interval_s: int

    # --- radio state ---
    rc_state: str
    tx_power_mode: str
    tx_power_arg: int
    current_rssi_dbm: Optional[int]

    # --- providers/models ---
    capacity_provider: Any
    energy_tx_model: Any
    energy_rx_model: Any
    idle_mean_W: Optional[float]
    throughput_tx_model: Any
    throughput_rx_model: Any

    # --- paths/meta ---
    results_dir: str
    data_dir: str
    models_dir: str

# -------------------------- Helpers (artifacts) -----------------------------

SELECTED_DIR = os.path.join(MODELS_DIR, "selected")

def _read_idle_mean(path_txt: str) -> Optional[float]:
    """Διαβάζει το idle_mean.txt (saved από modeling.py)."""
    try:
        with open(path_txt, "r", encoding="utf-8") as f:
            txt = f.read()
        m = re.search(r"=([\d\.Ee\-+]+)", txt)
        return float(m.group(1)) if m else None
    except Exception as e:
        warning(f"[INIT] idle_mean read failed: {e}")
        return None

def _load_pkl(path_pkl: str):
    """Φορτώνει .pkl με joblib ή pickle ως fallback."""
    if not os.path.exists(path_pkl):
        warning(f"[INIT] missing artifact: {path_pkl}")
        return None
    try:
        if joblib_load is not None:
            return joblib_load(path_pkl)
        with open(path_pkl, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        error(f"[INIT] failed to load model {os.path.basename(path_pkl)}: {e}")
        return None


# --------------------------- Helpers (RF/CLI) -------------------------------

def predict_throughput_mbps(
    state,
    scenario: str,
    rssi_dbm: Optional[float],
    tx_power: Optional[float],
    link_load: Optional[float],
) -> Optional[float]:
    """
    Επιστρέφει πρόβλεψη throughput (Mbps) για ενεργή κατεύθυνση, αλλιώς None.
    Βασίζεται στα ήδη φορτωμένα models στο `state` (TX/RX).
    """
    if link_load is None or rssi_dbm is None:
        return None

    try:
        if scenario == "tx" and state.throughput_tx_model is not None and tx_power is not None:
            X_thr = pd.DataFrame([[float(rssi_dbm), float(tx_power), float(link_load)]],
                                 columns=["rssi", "tx_power", "link_load"])
            return float(state.throughput_tx_model.predict(X_thr)[0])

        if scenario == "rx" and state.throughput_rx_model is not None:
            X_thr = pd.DataFrame([[float(rssi_dbm), float(link_load)]],
                                 columns=["rssi", "link_load"])
            return float(state.throughput_rx_model.predict(X_thr)[0])

        return None
    except Exception as e:
        # ίδιο logger με το υπόλοιπο script
        warning(f"throughput predict error: {e}")
        return None

def _radio_init(interface: str, sync_sleep: int = 2) -> Dict[str, Any]:
    """1) RC=on  2) TX power=auto(arg=30)  3) RSSI (median 2–3 reads)."""
    set_rc("on")
    set_tx_power("auto", 30)
    time.sleep(max(0, sync_sleep))

    # RSSI: 3 αναγνώσεις → median
    samples = []
    for _ in range(3):
        rssi = get_rssi_from_iw(interface=interface)
        if rssi is not None:
            samples.append(int(rssi))
        time.sleep(0.5)
    samples.sort()
    med = samples[len(samples)//2] if samples else None

    info(f"[INIT] Radio: RC=on, TXPWR=auto(arg=30), RSSI={med} dBm")
    return {"rc_state": "on", "tx_power_mode": "auto", "tx_power_arg": 30, "current_rssi_dbm": med}

def read_autotxgain_table(cli_path="~/nrc_pkg/script/cli_app") -> dict:
    """
    Διαβάζει τον πίνακα Auto TX Gain (MCS -> tx power index) μία φορά.
    Επιστρέφει dict π.χ. {0:20,1:19,2:19,3:16,...,7:15,10:20}
    """
    cli = os.path.expanduser(cli_path)
    try:
        out = subprocess.check_output([cli, "show", "autotxgain"], text=True)
        pairs = re.findall(r"Tx power index for MCS\s+(\d+)\s*:\s*(\d+)", out)
        d = {int(mcs): int(idx) for mcs, idx in pairs}
        if not d:
            warning("[MON] autotxgain table empty; falling back to init arg.")
        return d
    except Exception as e:
        warning(f"[MON] autotxgain read failed: {e}; using empty map.")
        return {}


# ---------------------------- Initialization --------------------------------

def init_monitoring(sample_interval_s: int = 4) -> MonitoringState:
    """Στήνει πλήρως το monitoring context (χωρίς loop) και επιστρέφει MonitoringState."""

    # A1: paths/config
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MODELS_DIR, exist_ok=True)

    # A2: Shelly binding + A4: CPU model load
    MQTT_TOPIC, SHELLY_IP, SHELLY_GEN = net_conf_check()
    if not all([MQTT_TOPIC, SHELLY_IP, SHELLY_GEN]):
        error("[INIT] Shelly binding failed.")
        raise SystemExit(1)
    cpu_model.load_cpu_model(CPU_MODEL_JSON)
    success("[INIT] CPU model loaded & Shelly binding ok")

    # A3: ρόλος & IPs (AP/STA)
    LOCAL_IP = get_ip_address(INTERFACE)
    role = get_wifi_role(INTERFACE)  # "AP" ή "STA"
    if role == "AP":
        REMOTE_IP = IP_STA
    elif role == "STA":
        REMOTE_IP = IP_AP
    else:
        error(f"[INIT] Unknown role for iface {INTERFACE} (IP={LOCAL_IP})")
        raise SystemExit(1)
    info(f"[INIT] Role={role} LocalIP={LOCAL_IP} RemoteIP={REMOTE_IP} Iface={INTERFACE}")

    # B: Radio init
    radio = _radio_init(INTERFACE, SYNC_SLEEP)

    print("[DEBUG] TX CSV path:", os.path.join(DATA_DIR, "tx.csv"))
    print("[DEBUG] RX CSV path:", os.path.join(DATA_DIR, "rx.csv"))

    # C1: Link capacity provider (TX keys by TX power, RX keys by RSSI)
    cap = LinkCapacityProvider(
        tx_csv=os.path.join(DATA_DIR, "tx.csv"),
        rx_csv=os.path.join(DATA_DIR, "rx.csv"),
    )
    info(f"[INIT] Capacity Q95 loaded (TX keys [dBm]={sorted(cap.tx_peaks.keys())}, "
         f"RX keys [RSSI dBm]={sorted(cap.rx_peaks.keys())})")

    # C2: Φόρτωση selected artifacts (energy + throughput + idle)
    sel_tx_energy = os.path.join(SELECTED_DIR, "energy_tx.pkl")
    sel_rx_energy = os.path.join(SELECTED_DIR, "energy_rx.pkl")
    sel_idle      = os.path.join(SELECTED_DIR, "idle_mean.txt")
    sel_tp_tx     = os.path.join(SELECTED_DIR, "throughput_tx.pkl")
    sel_tp_rx     = os.path.join(SELECTED_DIR, "throughput_rx.pkl")

    m_tx = _load_pkl(sel_tx_energy)
    m_rx = _load_pkl(sel_rx_energy)
    idle_mean = _read_idle_mean(sel_idle)
    m_tp_tx = _load_pkl(sel_tp_tx)
    m_tp_rx = _load_pkl(sel_tp_rx)

    info(f"[INIT] Models: energy(TX/RX)={[bool(m_tx), bool(m_rx)]}, "
         f"throughput(TX/RX)={[bool(m_tp_tx), bool(m_tp_rx)]}, idle_mean_W={idle_mean}")

    st = MonitoringState(
        role=role,
        interface=INTERFACE,
        local_ip=LOCAL_IP,
        remote_ip=REMOTE_IP,
        shelly_ip=SHELLY_IP,
        shelly_gen=SHELLY_GEN,
        sample_interval_s=sample_interval_s,
        rc_state=radio["rc_state"],
        tx_power_mode=radio["tx_power_mode"],
        tx_power_arg=radio["tx_power_arg"],
        current_rssi_dbm=radio["current_rssi_dbm"],
        capacity_provider=cap,
        energy_tx_model=m_tx,
        energy_rx_model=m_rx,
        idle_mean_W=idle_mean,
        throughput_tx_model=m_tp_tx,
        throughput_rx_model=m_tp_rx,
        results_dir=RESULTS_DIR,
        data_dir=DATA_DIR,
        models_dir=MODELS_DIR,
    )

    info(f"[INIT] ALERT_CFG={ALERT_CFG}")
    success("[INIT] Monitoring context is ready.")
    return st

# --------------------------- 1 sample (4 sec) -------------------------------
def check_sample_alerts(row, state, counters, cfg):
    sustained   = int(cfg.get("SUSTAINED", 3))
    rssi_tol    = float(cfg.get("RSSI_TOL_DB", 10.0))
    thr_tol     = float(cfg.get("THR_REL_ERR", 0.15))
    k, n        = cfg.get("K_OF_N", (3, 5))
    # --- (1) Sanity για K-of-N ---
    try:
        k = int(k); n = int(n)
    except Exception:
        k, n = 3, 5
    if n <= 0 or k <= 0 or k > n:
        # safe fallback
        debug(f"[ALERT] invalid K_OF_N=({k},{n}) → fallback to (3,5)")
        k, n = 3, 5

    cd_thr      = int(cfg.get("COOLDOWN_THR", 5))
    cd_rssi     = int(cfg.get("COOLDOWN_RSSI", 5))
    min_tobs    = float(cfg.get("MIN_TOBS_Mbps", 0.01))

    scen      = (row.get("scenario") or "").lower()   # --- (4) ασφάλεια ---
    rssi_now  = row.get("rssi_dbm")
    rssi_base = state.current_rssi_dbm

    tpred = row.get("thr_pred_mbps")
    tobs  = row.get("throughput_mbps_tx") if scen == "tx" else (
            row.get("throughput_mbps_rx") if scen == "rx" else None)

    # helpers για παράθυρα (2)
    def ensure_window(name: str):
        win = counters.get(name)
        if not isinstance(win, deque) or win.maxlen != n:
            counters[name] = deque(maxlen=n)
        return counters[name]

    # -------- RSSI: K-of-N + cooldown --------
    if counters.get("rssi_cooldown", 0) > 0:
        counters["rssi_cooldown"] -= 1
        debug(f"[ALERT|RSSI] cooldown… {counters['rssi_cooldown']} left")  # (3)
    else:
        valid_rssi = (rssi_now is not None) and (rssi_base is not None)
        if valid_rssi:
            rssi_hit = 1 if abs(float(rssi_now) - float(rssi_base)) > rssi_tol else 0
            win = ensure_window("rssi_hits")
            win.append(rssi_hit)

            if sum(win) >= k:
                counters["rssi_dev"] = counters.get("rssi_dev", 0) + 1
                warning(
                    f"RSSI alert {counters['rssi_dev']}/{sustained}: "
                    f"now={rssi_now} dBm, baseline={rssi_base} dBm, "
                    f"tol±{rssi_tol} dB, K-of-N={sum(win)}/{n}"
                )
                win.clear()                 # reset RSSI window after an event
                counters["rssi_cooldown"] = cd_rssi
        else:
            debug(f"[ALERT|RSSI] skip: rssi_now={rssi_now}, baseline={rssi_base}")
            w = counters.get("rssi_hits")
            if isinstance(w, deque): w.clear()

    # -------- Throughput: K-of-N + cooldown + ignore tiny obs --------
    if counters.get("thr_cooldown", 0) > 0:
        counters["thr_cooldown"] -= 1
        debug(f"[ALERT|TP] cooldown… {counters['thr_cooldown']} left")      # (3)
    else:
        valid_tp = (tobs is not None and float(tobs) >= min_tobs and
                    tpred is not None and float(tpred) > 0.0)
        if valid_tp:
            tobs_f, tpred_f = float(tobs), float(tpred)
            rel_err = abs(tobs_f - tpred_f) / tpred_f
            thr_hit = 1 if rel_err > thr_tol else 0

            win = ensure_window("thr_hits")
            win.append(thr_hit)

            if sum(win) >= k:
                counters["thr_dev"] = counters.get("thr_dev", 0) + 1
                warning(
                    f"Throughput alert {counters['thr_dev']}/{sustained}: "
                    f"obs={tobs_f:.3f} Mbps, pred={tpred_f:.3f} Mbps, "
                    f"rel_err={rel_err:.1%} (> {int(thr_tol*100)}%), "
                    f"K-of-N={sum(win)}/{n}"
                )
                win.clear()                # reset TP window after an event
                counters["thr_cooldown"] = cd_thr
        else:
            debug(f"[ALERT|TP] skip: tobs={tobs}, tpred={tpred}, min={min_tobs}")
            w = counters.get("thr_hits")
            if isinstance(w, deque): w.clear()

    # -------- Stop conditions (SUSTAINED events) --------
    if counters.get("rssi_dev", 0) >= sustained:
        w = counters.get("rssi_hits");  w.clear() if isinstance(w, deque) else None
        w = counters.get("thr_hits");   w.clear() if isinstance(w, deque) else None
        return True, f"RSSI out of bounds for {sustained} events (K-of-N). Retrain."

    if counters.get("thr_dev", 0) >= sustained:
        w = counters.get("rssi_hits");  w.clear() if isinstance(w, deque) else None
        w = counters.get("thr_hits");   w.clear() if isinstance(w, deque) else None
        return True, f"Throughput out of bounds for {sustained} events (K-of-N, tol±{int(thr_tol*100)}%). Retrain."

    return False, ""

def monitor_once(state: MonitoringState) -> dict:
    """
    Take ONE sample of length `state.sample_interval_s` seconds (e.g., 4s).
    Timing sequence (no CPU moving average):
      1) Snapshot (rx1, tx1) at the start of the window
      2) Block while measuring CPU utilization over the window
      3) Snapshot (rx2, tx2) at the end of the window

    Computes:
      - Observed TX/RX throughput (Mbps) from byte deltas
      - Active scenario ("tx" or "rx") inferred from deltas
      - Last MCS and TX power index (from autotx_map; fallback to init arg)
      - Current RSSI
      - Capacity estimate (TX keyed by TX power; RX keyed by RSSI)
      - Link load and predicted throughput for the active direction
      - Energy-per-bit prediction and directional power (TX/RX)
      - Total power (Shelly), CPU power, and modeled net power

    Returns:
      A dict with all fields to be written in the CSV / logs.
    """

    try:
        autotx_map = read_autotxgain_table()
    except Exception as e:
        warning(f"[MON] autotxgain refresh failed: {e}")
        autotx_map = {}

    # 1) Πρώτο snapshot bytes (ΠΡΟΣΟΧΗ: get_interface_bytes() -> (rx, tx))
    rx1, tx1 = get_interface_bytes(state.interface)

    # 3) CPU util (μπλοκάρει όλο το παράθυρο)
    cpu_util_pct = float(get_cpu_utilization(sample_duration=state.sample_interval_s))

    # 1) Δεύτερο snapshot bytes (τέλος παραθύρου)
    rx2, tx2 = get_interface_bytes(state.interface)
    dt = float(state.sample_interval_s)

    delta_rx = max(0, rx2 - rx1)
    delta_tx = max(0, tx2 - tx1)

    # Mbps (1 byte = 8 bits, 1 Mbps = 1e6 bits/s)
    tx_throughput_obs = (delta_tx * 8.0) / dt / 1e6
    rx_throughput_obs = (delta_rx * 8.0) / dt / 1e6

    # 2) Σενάριο από deltas
    scenario = infer_activity(prev_rx=rx1, prev_tx=tx1, curr_rx=rx2, curr_tx=tx2)
    if scenario == "tx_rx":
        scenario = "tx" if tx_throughput_obs >= rx_throughput_obs else "rx"

    # 4) Τρέχον MCS (με βάση role/scenario αν το helper το απαιτεί)
    mcs = get_last_mcs(state.role, scenario)

    # 5) MCS -> TX power index (fallback στην init τιμή αν δεν βρεθεί)
    tx_power = autotx_map.get(mcs) if mcs is not None else None
    if tx_power is None:
        warning("[TX-Power]: Getting from cli...")
        tx_power = get_tx_power_from_cli()  # π.χ. 30 (τυπικό arg στο auto)

    # 6) RSSI
    rssi_dbm = get_rssi_from_iw(state.interface)

    # 7) Capacity (TX: by TX power, RX: by RSSI)
    capacity_mbps = None
    try:
        if scenario == "tx":
            capacity_mbps = float(state.capacity_provider.capacity("tx", float(tx_power)))
        elif scenario == "rx" and rssi_dbm is not None:
            capacity_mbps = float(state.capacity_provider.capacity("rx", float(rssi_dbm)))
        else:
            capacity_mbps = 0.0  # idle
    except Exception as e:
        warning(f"capacity lookup error: {e}")
        capacity_mbps = None

    # 8) Link load από παρατηρούμενο throughput
    if capacity_mbps is None or capacity_mbps <= 0:
        link_load_raw = None
        link_load = None
        thr_active = 0.0
    else:
        thr_active = tx_throughput_obs if scenario == "tx" else (rx_throughput_obs if scenario == "rx" else 0.0)
        link_load_raw = thr_active / capacity_mbps
        link_load = min(max(link_load_raw, 0.0), 1.0)

        # --- (ΝΕΟ) Πρόβλεψη Throughput για την ΕΝΕΡΓΗ κατεύθυνση (χρησιμοποιεί το ίδιο link_load) ---
    thr_pred_mbps = predict_throughput_mbps(
        state=state,
        scenario=scenario,
        rssi_dbm=rssi_dbm,
        tx_power=tx_power,
        link_load=link_load,
    )

    # 9) Πρόβλεψη Energy per bit (μJ/bit) + P_idle
    energy_uJ_per_bit = None
    if link_load is not None and rssi_dbm is not None:
        try:
            if scenario == "tx" and state.energy_tx_model is not None:
                X = pd.DataFrame([[float(rssi_dbm), float(tx_power), float(link_load)]],
                                 columns=["rssi", "tx_power", "link_load"])
                energy_uJ_per_bit = float(state.energy_tx_model.predict(X)[0])
            elif scenario == "rx" and state.energy_rx_model is not None:
                X = pd.DataFrame([[float(rssi_dbm), float(link_load)]],
                                 columns=["rssi", "link_load"])
                energy_uJ_per_bit = float(state.energy_rx_model.predict(X)[0])
        except Exception as e:
            warning(f"energy predict error: {e}")

    P_idle_W = state.idle_mean_W if state.idle_mean_W is not None else 0.0

    # 10) E_per_bit → Power (W) για ενεργή κατεύθυνση
    P_tx_W = 0.0
    P_rx_W = 0.0
    if energy_uJ_per_bit is not None and thr_active > 0:
        thr_bits_per_s = thr_active * 1e6
        P_dir_W = (energy_uJ_per_bit * thr_bits_per_s) / 1e6  # μJ/bit × bits/s / 1e6 = W
        if scenario == "tx":
            P_tx_W = P_dir_W
        elif scenario == "rx":
            P_rx_W = P_dir_W

    # 11) P_total (Shelly), P_cpu, P_net(measured)
    try:
        P_total_W = float(get_power(state.shelly_ip, state.shelly_gen))
    except Exception:
        P_total_W = None

    try:
        P_cpu_W = float(estimate_cpu_power(cpu_util_pct))
    except Exception:
        P_cpu_W = None

    # Μοντελοποιημένη καθαρή ισχύς δικτύου
    if scenario == "idle": 
        P_net_model_W = P_idle_W
    elif scenario == "tx":
        P_net_model_W = P_tx_W
    else: 
        P_net_model_W = P_rx_W

    P_total_pred = P_net_model_W + P_cpu_W + nu.P_OTHER

    debug(
        "Summary:\n"
        f"  before_rx_bytes   = {rx1}\n"
        f"  before_tx_bytes   = {tx1}\n"
        f"  cpu_util_pct      = {cpu_util_pct}\n"
        f"  after_rx_bytes    = {rx2} (Δ{delta_rx})\n"
        f"  after_tx_bytes    = {tx2} (Δ{delta_tx})\n"
        f"  dt_s              = {dt:.3f}\n"
        f"  thr_tx_mbps       = {tx_throughput_obs:.3f}\n"
        f"  thr_rx_mbps       = {rx_throughput_obs:.3f}\n"
        f"  ŷ_thr_mbps        = {None if thr_pred_mbps is None else round(float(thr_pred_mbps), 3)}\n"
        f"  scen              = {scenario}\n"
        f"  mcs               = {mcs}\n"
        f"  txp               = {tx_power}\n"
        f"  rssi              = {rssi_dbm}\n"
        f"  C_mbps            = {None if capacity_mbps is None else round(float(capacity_mbps), 3)}\n"
        f"  load              = {None if link_load is None else round(float(link_load), 3)}\n"
        f"  EμJ_per_bit       = {energy_uJ_per_bit}\n"
        f"  Pidle_W           = {P_idle_W}\n"
        f"  Ptx_W             = {round(P_tx_W, 3)}\n"
        f"  Prx_W             = {round(P_rx_W, 3)}\n"
        f"  Ptotal_W          = {P_total_W}\n"
        f"  Pcpu_W            = {P_cpu_W}\n"
        f"  P_other_W         = {nu.P_OTHER}\n"
        f"  Pnet_model_W      = {round(P_net_model_W, 3)}"
    )

    return {
        "timestamp": int(time.time()),
        "role": state.role, "scenario": scenario, "interface": state.interface,
        "rx_bytes": delta_rx, "tx_bytes": delta_tx,
        "throughput_mbps_rx": rx_throughput_obs, "throughput_mbps_tx": tx_throughput_obs, "thr_pred_mbps": thr_pred_mbps,
        "mcs": mcs, "tx_power": tx_power, "rssi_dbm": rssi_dbm,
        "capacity_mbps": capacity_mbps, "link_load_raw": link_load_raw, "link_load": link_load,
        "energy_pred_uJ_per_bit": energy_uJ_per_bit, "P_idle_W": P_idle_W,
        "P_tx_W": P_tx_W, "P_rx_W": P_rx_W,
        "P_total_W": P_total_W, "P_cpu_W": P_cpu_W, "P_other_W": nu.P_OTHER,
        "P_net_model_W": P_net_model_W
    }

# ------------------------------ Main loop -----------------------------------

def run_monitoring(out_csv: Optional[str] = None, total_minutes: Optional[float] = None):
    """Run the monitoring loop and write a CSV with all fields."""
    state: MonitoringState = init_monitoring(sample_interval_s=SAMPLE_INTERVAL)

    counters = {
        "rssi_dev": 0, "thr_dev": 0,
        "rssi_hits": deque(maxlen=ALERT_CFG["K_OF_N"][1]),
        "thr_hits":  deque(maxlen=ALERT_CFG["K_OF_N"][1]),
        "rssi_cooldown": 0,
        "thr_cooldown":  0,
    }

    # CSV path
    if out_csv is None:
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_csv = os.path.join(RESULTS_DIR, f"monitoring_{ts}.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    headers = [
        "timestamp","role","scenario","interface",
        "rx_bytes","tx_bytes","throughput_mbps_rx","throughput_mbps_tx", "thr_pred_mbps",
        "mcs","tx_power","rssi_dbm",
        "capacity_mbps","link_load_raw","link_load",
        "energy_pred_uJ_per_bit","P_idle_W","P_tx_W","P_rx_W",
        "P_total_W","P_cpu_W","P_other_W","P_net_model_W",
    ]

    stop_flag = False
    def _sigint(sig, frame):
        nonlocal stop_flag
        stop_flag = True
    signal.signal(signal.SIGINT, _sigint)

    end_time = None
    if total_minutes and total_minutes > 0:
        end_time = time.time() + total_minutes * 60

    info(f"[MON] writing to {out_csv}")
    with open(out_csv, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=headers)
        wr.writeheader()

        sample_idx = 0
        while not stop_flag:
            if end_time and time.time() >= end_time:
                break

            row = monitor_once(state)
            wr.writerow({k: (None if isinstance(v, float) and (v != v) else v) for k, v in row.items()})

            # Per-sample alert checks (K-of-N, cooldown, min obs threshold)
            should_stop, reason = check_sample_alerts(row, state, counters, ALERT_CFG)
            if should_stop:
                error(reason)
                # εδώ μπορείς να καλέσεις retrain αν θέλεις
                break

            sample_idx += 1
            if sample_idx % max(1, int(60 / state.sample_interval_s)) == 0:
                info(f"[MON] scen={row['scenario']} thr_tx={row['throughput_mbps_tx']:.2f} "
                    f"thr_rx={row['throughput_mbps_rx']:.2f} C={row['capacity_mbps']} "
                    f"load={row['link_load']} Pnet(model)={row['P_net_model_W']}")

    success("[MON] Monitoring loop finished.")


# ------------------------------ CLI entry -----------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--outfile", type=str, default=None, help="CSV path (default: results/monitoring_<ts>.csv)")
    p.add_argument("--minutes", type=float, default=None, help="Run duration in minutes (default: until Ctrl+C)")
    args = p.parse_args()
    
    clear_mac_stats()
    run_monitoring(out_csv=args.outfile, total_minutes=args.minutes)
