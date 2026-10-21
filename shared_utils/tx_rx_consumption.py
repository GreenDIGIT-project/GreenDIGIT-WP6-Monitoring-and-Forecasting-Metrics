from dataclasses import dataclass
from typing import Optional
import os, re, subprocess, sys
import pandas as pd # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')

# --- Project config & utils ---
from shared_utils.extract import (get_wifi_role, get_rssi_from_iw, get_last_mcs, get_tx_power_from_cli)
from shared_utils.log_utils import info

# Link capacity provider (Q95: TX keyed by TX power, RX keyed by RSSI)
from shared_utils.link_capacity import LinkCapacityProvider

# Προσπαθούμε joblib πρώτα για .pkl models
try:
    from joblib import load as joblib_load # type: ignore
except Exception:
    joblib_load = None
import pickle
# -------------------------- Helpers (artifacts) -----------------------------

DATA_DIR    = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/net/data"
MODELS_DIR  = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/net/models"
INTERFACE = "wlan0"


def _read_idle_mean(path_txt: str) -> Optional[float]:
    """Διαβάζει το idle_mean.txt (saved από modeling.py)."""
    try:
        with open(path_txt, "r", encoding="utf-8") as f:
            txt = f.read()
        m = re.search(r"=([\d\.Ee\-+]+)", txt)
        return float(m.group(1)) if m else None
    except Exception as e:

        return None

def _load_pkl(path_pkl: str):
    """Φορτώνει .pkl με joblib ή pickle ως fallback."""
    if not os.path.exists(path_pkl):
        return None
    try:
        if joblib_load is not None:
            return joblib_load(path_pkl)
        with open(path_pkl, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        return None




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
        return d
    except Exception as e:
        return {}


# ---------------------------- Initialization --------------------------------

def monitor_once(tx_throughput, rx_throughput):

    interface = INTERFACE

    try:
        autotx_map = read_autotxgain_table()
    except Exception as e:
        autotx_map = {}

    if tx_throughput>0.0032 or rx_throughput>0.0032:
        scenario = "tx_rx"
    else:
        scenario = "idle"
    if scenario == "tx_rx":
        scenario = "tx" if tx_throughput >= rx_throughput else "rx"

    # 4) Τρέχον MCS (με βάση role/scenario αν το helper το απαιτεί)
    role = get_wifi_role(interface)  # "AP" ή "STA"
    mcs = get_last_mcs(role, scenario)

    # 5) MCS -> TX power index (fallback στην init τιμή αν δεν βρεθεί)
    tx_power = autotx_map.get(mcs) if mcs is not None else None
    if tx_power is None:
        tx_power = get_tx_power_from_cli()  # π.χ. 30 (τυπικό arg στο auto)

    # 6) RSSI
    rssi_dbm = get_rssi_from_iw(interface)

    # 7) Capacity (TX: by TX power, RX: by RSSI)
    capacity_mbps = None
    cap = LinkCapacityProvider(
        tx_csv=os.path.join(DATA_DIR, "tx.csv"),
        rx_csv=os.path.join(DATA_DIR, "rx.csv"),
    )
    try:
        if scenario == "tx":
            capacity_mbps = float(cap.capacity("tx", float(tx_power)))
        elif scenario == "rx" and rssi_dbm is not None:
            capacity_mbps = float(cap.capacity("rx", float(rssi_dbm)))
        else:
            capacity_mbps = 0.0  # idle
    except Exception as e:
        capacity_mbps = None

    # 8) Link load από παρατηρούμενο throughput
    if capacity_mbps is None or capacity_mbps <= 0:
        link_load_raw = None
        link_load = None
        thr_active = 0.0
    else:
        thr_active = tx_throughput if scenario == "tx" else (rx_throughput if scenario == "rx" else 0.0)
        link_load_raw = thr_active / capacity_mbps
        link_load = min(max(link_load_raw, 0.0), 1.0)


    # 9) Πρόβλεψη Energy per bit (μJ/bit) + P_idle
    energy_uJ_per_bit = None
    directory_pkl = os.path.join(MODELS_DIR, "selected")
    sel_tx_energy = os.path.join(directory_pkl, "energy_tx.pkl")
    sel_rx_energy = os.path.join(directory_pkl, "energy_rx.pkl")
    sel_idle      = os.path.join(directory_pkl, "idle_mean.txt")
    tx_model = _load_pkl(sel_tx_energy)
    rx_model = _load_pkl(sel_rx_energy)
    if link_load is not None and rssi_dbm is not None:
        if scenario == "tx" and tx_model is not None:
            X = pd.DataFrame([[float(rssi_dbm), float(tx_power), float(link_load)]],
                                columns=["rssi", "tx_power", "link_load"])
            energy_uJ_per_bit = float(tx_model.predict(X)[0])
        elif scenario == "rx" and rx_model is not None:
            X = pd.DataFrame([[float(rssi_dbm), float(link_load)]],
                                columns=["rssi", "link_load"])
            energy_uJ_per_bit = float(rx_model.predict(X)[0])
    
    idle_mean = _read_idle_mean(sel_idle)
    P_idle_W = idle_mean if idle_mean is not None else 0.0

    # 10) E_per_bit → Power (W) για ενεργή κατεύθυνση
    P_tx_W = 0.0
    P_rx_W = 0.0
    if energy_uJ_per_bit is not None and thr_active > 0:
        thr_bits_per_s = thr_active * 1e6
        P_dir_W = (energy_uJ_per_bit * thr_bits_per_s) / 1e3  # μJ/bit × bits/s / 1e6 = mW
        if scenario == "tx":
            P_tx_W = P_dir_W
        elif scenario == "rx":
            P_rx_W = P_dir_W

    # 11) P_net(measured)
    # Μοντελοποιημένη καθαρή ισχύς δικτύου
    if scenario == "idle": 
        P_net_model_W = P_idle_W
    elif scenario == "tx":
        P_net_model_W = P_tx_W
    else: 
        P_net_model_W = P_rx_W
    return energy_uJ_per_bit, P_net_model_W, scenario
