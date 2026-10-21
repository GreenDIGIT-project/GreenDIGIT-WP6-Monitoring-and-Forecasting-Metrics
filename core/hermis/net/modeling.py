#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HERMIS — Train Selected Models (Full dataset, no evaluation)
------------------------------------------------------------
Goal: train **only** the chosen models (SVR/GBDT/Ridge/LGBM per CONFIG)
using the **entire** dataset (no train/test split, no metrics), then save them for
use during the monitoring phase.

Inputs:  data/tx.csv, data/rx.csv (from make_data.py) and optionally data/idle.csv
Outputs: models/selected/{energy_tx.pkl, energy_rx.pkl, idle_mean.txt, throughput_tx.pkl, throughput_rx.pkl}
         models/HERMIS_runs/<timestamp>/{tx,rx,idle,throughput_tx,throughput_rx}/ (copies + manifest)

All configuration is inside the CONFIG block below.
"""

import os, sys, json, warnings
from datetime import datetime
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from typing import List, Optional

# ------------------------------------------------------------------
# Project config & logger (fallbacks to print if imports fail)
# ------------------------------------------------------------------
CUR_DIR = os.path.dirname(os.path.abspath(__file__))
if CUR_DIR not in sys.path:
    sys.path.insert(0, CUR_DIR)

try:
    from config import DATA_DIR, MODELS_DIR
    from shared_utils.log_utils import info, success, warning, error, debug
except Exception as e:
    print("Falling back to defaults because imports failed:", e)
    DATA_DIR, MODELS_DIR = "data", "models"
    class _Dummy:
        def info(self, m): print(m)
        def success(self, m): print(m)
        def warning(self, m): print(m)
        def error(self, m): print(m)
        def debug(self, m): print(m)
    info=_Dummy().info; success=_Dummy().success; warning=_Dummy().warning; error=_Dummy().error; debug=_Dummy().debug

warnings.filterwarnings("ignore")

# ------------------------------------------------------------------
# CONFIG — tweak here instead of passing CLI args
# ------------------------------------------------------------------
CONFIG = {
    # --- Model selection per task ---
    # valid: "svr", "sklearn_gbdt", "ridge", "lgbm"
    "MODEL_SELECT": {
        "throughput_rx": "extratrees",   # <-- αλλάζεις σε extratrees
        "throughput_tx": "svr",          # <-- αλλάζεις σε svr
        "energy_rx": "svr",
        "energy_tx": "extratrees", 
    },

    # --- Hyperparams per selected model ---
    # Throughput_RX -> SVR_RBF_C2.0_eps0.005_gscale
    "THROUGHPUT_RX_SVR": {"C": 2.0, "epsilon": 0.005, "gamma": "scale"},

    # Throughput_TX -> SklearnGBDT_ne300_md3_lr0.08_ss0.9_losshuber
    "THROUGHPUT_TX_GBDT": {
        "n_estimators": 300,
        "max_depth": 3,
        "learning_rate": 0.08,
        "subsample": 0.9,
        "loss": "huber",
        "random_state": 42
    },

    "THROUGHPUT_RX_EXTRATREES": {
        "n_estimators": 200,
        "max_depth": 6,
        "min_samples_leaf": 1,
        "bootstrap": False,
        "random_state": 42,
        "n_jobs": -1
    },

    "THROUGHPUT_TX_SVR": {"C": 0.6, "epsilon": 0.05, "gamma": "scale"},

    "ENERGY_RX_SVR": {"C": 0.1, "epsilon": 0.1, "gamma": "scale"},

    # Energy_RX -> Ridge_alpha10.0
    "ENERGY_RX_RIDGE": {"alpha": 10.0, "random_state": None},

    # Energy_TX -> LightGBM_md7_lr0.07_ne800_ss0.8_cs0.8_l11.0_l20.0
    "ENERGY_TX_LGBM": {
        "n_estimators": 800,
        "learning_rate": 0.07,
        "max_depth": 7,
        "num_leaves": 64,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 1.0,   # L1
        "reg_lambda": 0.0,  # L2
        "random_state": 42,
        "n_jobs": -1
    },

    # Energy_TX -> ExtraTrees_n200_md6_msl5
    "ENERGY_TX_EXTRATREES": {
        "n_estimators": 200,
        "max_depth": 6,
        "min_samples_leaf": 5,
        "bootstrap": False,
        "random_state": 42,
        "n_jobs": -1
    },

    # Optional outlier clipping
    "CLIP_OUTLIERS": True,
    "Q_LO": 0.005,
    "Q_HI": 0.995,

    # Export small debug CSV snapshots
    "EXPORT_DEBUG_SNAPSHOTS": True,
}

# ------------------------------------------------------------------
# Feature/target definitions per phase (explicitly requested)
# NOTE: we will only use columns that actually exist in each CSV.
# ------------------------------------------------------------------
TX_FEATURES = ["rssi", "tx_power", "link_load"]
RX_FEATURES = ["rssi", "link_load"]

TX_TARGET = "energy_per_bit_uJ"   # if only energy_per_bit exists → convert ×1e6
RX_TARGET = "energy_per_bit_uJ"
IDLE_TARGET = "network_power"     # IDLE baseline is the mean network power

# Throughput models — kept configurable
THROUGHPUT_FEATURES = ["rssi", "tx_power", "link_load"]
THROUGHPUT_TARGET_TX = "throughput_mbps_tx"
THROUGHPUT_TARGET_RX = "throughput_mbps_rx"

# ------------------------------------------------------------------
# ML tools
# ------------------------------------------------------------------
from sklearn.pipeline import Pipeline  # type: ignore
from sklearn.preprocessing import StandardScaler  # type: ignore
from sklearn.svm import SVR  # type: ignore
from sklearn.ensemble import ExtraTreesRegressor   # type: ignore
from joblib import dump  # type: ignore
from sklearn.ensemble import GradientBoostingRegressor  # type: ignore # sklearn GBDT
from sklearn.linear_model import Ridge                  # type: ignore # Ridge
try:
    import lightgbm as lgb                              # type: ignore # LightGBM
except Exception as _e:
    lgb = None

RUN_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_ROOT = os.path.join(MODELS_DIR, "HERMIS_runs", RUN_TAG)
OUT_TX   = os.path.join(OUT_ROOT, "tx")
OUT_RX   = os.path.join(OUT_ROOT, "rx")
OUT_IDLE = os.path.join(OUT_ROOT, "idle")
OUT_TP_TX = os.path.join(OUT_ROOT, "throughput_tx")
OUT_TP_RX = os.path.join(OUT_ROOT, "throughput_rx")
SEL_DIR  = os.path.join(MODELS_DIR, "selected")

# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------

def to_uJ_column(df: pd.DataFrame, base_col: str = "energy_per_bit", out_col: str = "energy_per_bit_uJ") -> pd.DataFrame:
    """Ensure μJ/bit column exists; if not, create it from Joule/bit × 1e6."""
    if out_col not in df.columns:
        if base_col in df.columns:
            df[out_col] = pd.to_numeric(df[base_col], errors='coerce') * 1e6
        else:
            raise ValueError(f"Missing energy column: '{base_col}' or '{out_col}' not found.")
    return df


def clip_outliers(df: pd.DataFrame, cols: List[str], q_lo=0.005, q_hi=0.995) -> pd.DataFrame:
    """Optionally clip extreme values for stability based on quantiles [q_lo, q_hi]."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            lo, hi = df[c].quantile([q_lo, q_hi])
            df[c] = df[c].clip(lo, hi)
            debug(f"[CLIP] {c}: q{q_lo}={lo:.4g}, q{q_hi}={hi:.4g}")
    return df


def make_pipe(model, needs_scaling: bool) -> Pipeline:
    """Προσθέτει StandardScaler μόνο όπου χρειάζεται (SVR/Ridge)."""
    steps = []
    if needs_scaling:
        steps.append(("scaler", StandardScaler()))
    steps.append(("model", model))
    return Pipeline(steps)


def build_model(tag: str, params: dict):
    """
    tag ∈ {"svr", "sklearn_gbdt", "ridge", "lgbm"}
    Επιστρέφει (estimator, needs_scaling_bool)
    """
    if tag == "svr":
        base = {"kernel": "rbf"}
        base.update(params or {})
        return SVR(**base), True

    if tag == "sklearn_gbdt":
        return GradientBoostingRegressor(**(params or {})), False
    
    if tag == "extratrees":
        return ExtraTreesRegressor(**(params or {})), False  # δέν χρειάζεται scaling
    
    if tag == "ridge":
        return Ridge(**(params or {})), True

    if tag == "lgbm":
        if lgb is None:
            warning("[LGBM] lightgbm δεν είναι εγκατεστημένο — fallback σε SVR (C=0.6, eps=0.02, gamma='scale').")
            return SVR(kernel="rbf", C=0.6, epsilon=0.02, gamma="scale"), True
        return lgb.LGBMRegressor(**(params or {})), False

    raise ValueError(f"Unknown model tag: {tag}")


def model_name(est) -> str:
    try:
        return est.__class__.__name__
    except Exception:
        return str(est)


def load_phase_csv(path: str, phase: str) -> Optional[pd.DataFrame]:
    """Load a CSV for a given phase with basic validation and logging."""
    if not os.path.exists(path):
        warning(f"[{phase}] Missing {path} — skipping.")
        return None
    df = pd.read_csv(path)
    if df.empty:
        warning(f"[{phase}] Empty dataset.")
        return None
    debug(f"[{phase}] Loaded CSV with columns: {list(df.columns)} (rows={len(df)})")
    return df


def _debug_matrix(phase: str, features: List[str], target: str, X: pd.DataFrame, y: pd.Series):
    """Emit detailed debug info about the design matrix used for fitting."""
    debug(f"[{phase}] Features used: {features}")
    debug(f"[{phase}] Target: {target}")
    debug(f"[{phase}] X shape: {X.shape}, y shape: {y.shape}")
    try:
        debug(f"[{phase}] First 5 rows of X:{X.head().to_string(index=False)}")
    except Exception:
        debug(f"[{phase}] First 5 rows of X:{X.head()}")
    debug(f"[{phase}] First 5 y values: {y[:5].tolist()}")


def _export_debug_snapshots(out_dir: str, X: pd.DataFrame, y: pd.Series, prefix: str):
    """Optionally export tiny CSV snapshots with the data used for training."""
    if not CONFIG.get("EXPORT_DEBUG_SNAPSHOTS", False):
        return
    os.makedirs(out_dir, exist_ok=True)
    X.head(50).to_csv(os.path.join(out_dir, f"{prefix}_X_head.csv"), index=False)
    pd.DataFrame({"y": y[:50]}).to_csv(os.path.join(out_dir, f"{prefix}_y_head.csv"), index=False)

# ------------------------------------------------------------------
# Training per phase (fit on FULL dataset, NO evaluation)
# ------------------------------------------------------------------

def train_throughput_tx(df: pd.DataFrame):
    """Train Throughput-TX model (full dataset) και σώσε artifacts."""
    requested = list(THROUGHPUT_FEATURES)
    target = THROUGHPUT_TARGET_TX
    features = [c for c in requested if c in df.columns]
    missing = [c for c in requested if c not in df.columns]
    if missing:
        debug(f"[TP_TX] Missing requested features (ignored): {missing}")
    if not features or target not in df.columns:
        error("[TP_TX] No usable features or missing target.")
        return None

    n0 = len(df)
    df = df.dropna(subset=features + [target])
    n1 = len(df)
    debug(f"[TP_TX] Dropped NaNs: {n0-n1} rows (remaining={n1})")

    if CONFIG.get("CLIP_OUTLIERS", False):
        debug(f"[TP_TX] Clipping on {len(features)+1} columns")
        df = clip_outliers(df, features + [target], CONFIG['Q_LO'], CONFIG['Q_HI'])

    X = df[features].astype(float)
    y = pd.to_numeric(df[target], errors='coerce').astype(float)

    _debug_matrix("TP_TX", features, target, X, y)
    _export_debug_snapshots(OUT_TP_TX, X, y, prefix="tp_tx")

    model_tag = CONFIG["MODEL_SELECT"]["throughput_tx"]
    if model_tag == "svr":
        params = CONFIG["THROUGHPUT_TX_SVR"]
    elif model_tag == "sklearn_gbdt":
        params = CONFIG["THROUGHPUT_TX_GBDT"]
    else:
        params = {}
    est, need_scale = build_model(model_tag, params)
    pipe = make_pipe(est, need_scale)

    info(f"[TP_TX] Training {model_name(est)} on full dataset (rows={len(X)}) ...")
    pipe.fit(X, y)

    os.makedirs(OUT_TP_TX, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    best_path = os.path.join(OUT_TP_TX, "throughput_tx.pkl")
    dump(pipe, best_path)
    dump(pipe, os.path.join(SEL_DIR, "throughput_tx.pkl"))

    meta = {"phase": "throughput_tx", "model": model_name(est), "params": params,
            "features": features, "target": target, "rows": int(len(X))}
    with open(os.path.join(OUT_TP_TX, "tp_tx_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    success(f"[TP_TX] Saved → {best_path} & models/selected/throughput_tx.pkl")
    return meta


def train_throughput_rx(df: pd.DataFrame):
    """Train Throughput-RX model (full dataset) και σώσε artifacts."""
    requested = list([c for c in THROUGHPUT_FEATURES if c != "tx_power"])  # RX συνήθως χωρίς tx_power
    target = THROUGHPUT_TARGET_RX
    features = [c for c in requested if c in df.columns]
    missing = [c for c in requested if c not in df.columns]
    if missing:
        debug(f"[TP_RX] Missing requested features (ignored): {missing}")
    if not features or target not in df.columns:
        error("[TP_RX] No usable features or missing target.")
        return None

    n0 = len(df)
    df = df.dropna(subset=features + [target])
    n1 = len(df)
    debug(f"[TP_RX] Dropped NaNs: {n0-n1} rows (remaining={n1})")

    if CONFIG.get("CLIP_OUTLIERS", False):
        debug(f"[TP_RX] Clipping on {len(features)+1} columns")
        df = clip_outliers(df, features + [target], CONFIG['Q_LO'], CONFIG['Q_HI'])

    X = df[features].astype(float)
    y = pd.to_numeric(df[target], errors='coerce').astype(float)

    _debug_matrix("TP_RX", features, target, X, y)
    _export_debug_snapshots(OUT_TP_RX, X, y, prefix="tp_rx")

    model_tag = CONFIG["MODEL_SELECT"]["throughput_rx"]
    if model_tag == "extratrees":
        params = CONFIG["THROUGHPUT_RX_EXTRATREES"]
    elif model_tag == "svr":
        params = CONFIG["THROUGHPUT_RX_SVR"]
    else:
        params = {}
    est, need_scale = build_model(model_tag, params)
    pipe = make_pipe(est, need_scale)

    info(f"[TP_RX] Training {model_name(est)} on full dataset (rows={len(X)}) ...")
    pipe.fit(X, y)

    os.makedirs(OUT_TP_RX, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    best_path = os.path.join(OUT_TP_RX, "throughput_rx.pkl")
    dump(pipe, best_path)
    dump(pipe, os.path.join(SEL_DIR, "throughput_rx.pkl"))

    meta = {"phase": "throughput_rx", "model": model_name(est), "params": params,
            "features": features, "target": target, "rows": int(len(X))}
    with open(os.path.join(OUT_TP_RX, "tp_rx_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    success(f"[TP_RX] Saved → {best_path} & models/selected/throughput_rx.pkl")
    return meta


def train_tx(df: pd.DataFrame):
    """Train Energy-TX model on the entire dataset and save artifacts."""
    df = to_uJ_column(df)

    requested = list(TX_FEATURES)
    features = [c for c in requested if c in df.columns]
    missing = [c for c in requested if c not in df.columns]
    if missing:
        debug(f"[TX] Missing requested features (ignored): {missing}")
    target = TX_TARGET

    if not features:
        error("[TX] No usable features were found in the CSV.")
        return None

    n0 = len(df)
    df = df.dropna(subset=features + [target])
    n1 = len(df)
    debug(f"[TX] Dropped NaNs: {n0-n1} rows (remaining={n1})")

    if CONFIG.get("CLIP_OUTLIERS", False):
        debug(f"[TX] Applying clipping (q_lo={CONFIG['Q_LO']}, q_hi={CONFIG['Q_HI']}) on {len(features)+1} columns")
        df = clip_outliers(df, features + [target], CONFIG['Q_LO'], CONFIG['Q_HI'])

    X = df[features].astype(float)
    y = pd.to_numeric(df[target], errors='coerce').astype(float)

    _debug_matrix("TX", features, target, X, y)
    _export_debug_snapshots(OUT_TX, X, y, prefix="tx")

    model_tag = CONFIG["MODEL_SELECT"]["energy_tx"]  # default: "lgbm"
    if model_tag == "extratrees":
        params = CONFIG["ENERGY_TX_EXTRATREES"]
    elif model_tag == "lgbm":
        params = CONFIG["ENERGY_TX_LGBM"]
    else:
        params = {}
    est, need_scale = build_model(model_tag, params)
    pipe = make_pipe(est, need_scale)

    info(f"[TX] Training {model_name(est)} on the full dataset (rows={len(X)}) ...")
    pipe.fit(X, y)

    os.makedirs(OUT_TX, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    best_path = os.path.join(OUT_TX, "tx_model.pkl")
    dump(pipe, best_path)
    dump(pipe, os.path.join(SEL_DIR, "energy_tx.pkl"))

    meta = {"phase": "tx", "model": model_name(est), "params": params,
            "features": features, "target": target, "rows": int(len(X))}
    with open(os.path.join(OUT_TX, "tx_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    success(f"[TX] Saved → {best_path} & models/selected/energy_tx.pkl")
    return meta


def train_rx(df: pd.DataFrame):
    """Train Energy-RX model on the entire dataset and save artifacts."""
    df = to_uJ_column(df)

    requested = list(RX_FEATURES)
    features = [c for c in requested if c in df.columns]
    missing = [c for c in requested if c not in df.columns]
    if missing:
        debug(f"[RX] Missing requested features (ignored): {missing}")
    target = RX_TARGET

    if not features:
        error("[RX] No usable features were found in the CSV.")
        return None

    n0 = len(df)
    df = df.dropna(subset=features + [target])
    n1 = len(df)
    debug(f"[RX] Dropped NaNs: {n0-n1} rows (remaining={n1})")

    if CONFIG.get("CLIP_OUTLIERS", False):
        debug(f"[RX] Applying clipping (q_lo={CONFIG['Q_LO']}, q_hi={CONFIG['Q_HI']}) on {len(features)+1} columns")
        df = clip_outliers(df, features + [target], CONFIG['Q_LO'], CONFIG['Q_HI'])

    X = df[features].astype(float)
    y = pd.to_numeric(df[target], errors='coerce').astype(float)

    _debug_matrix("RX", features, target, X, y)
    _export_debug_snapshots(OUT_RX, X, y, prefix="rx")

    model_tag = CONFIG["MODEL_SELECT"]["energy_rx"]  # default: "ridge"
    if model_tag == "svr":
        params = CONFIG["ENERGY_RX_SVR"]
    elif model_tag == "ridge":
        params = CONFIG["ENERGY_RX_RIDGE"]
    else:
        params = {}
    est, need_scale = build_model(model_tag, params)
    pipe = make_pipe(est, need_scale)

    info(f"[RX] Training {model_name(est)} on the full dataset (rows={len(X)}) ...")
    pipe.fit(X, y)

    os.makedirs(OUT_RX, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    best_path = os.path.join(OUT_RX, "rx_model.pkl")
    dump(pipe, best_path)
    dump(pipe, os.path.join(SEL_DIR, "energy_rx.pkl"))

    meta = {"phase": "rx", "model": model_name(est), "params": params,
            "features": features, "target": target, "rows": int(len(X))}
    with open(os.path.join(OUT_RX, "rx_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    success(f"[RX] Saved → {best_path} & models/selected/energy_rx.pkl")
    return meta


def train_idle(df_idle: pd.DataFrame):
    """Compute IDLE baseline as mean(network_power) and save artifacts."""
    if df_idle is None or df_idle.empty:
        warning("[IDLE] Missing idle.csv — skipping.")
        return None
    if IDLE_TARGET not in df_idle.columns:
        warning(f"[IDLE] '{IDLE_TARGET}' not found in idle.csv — skipping.")
        return None

    d = df_idle.dropna(subset=[IDLE_TARGET])
    nums = pd.to_numeric(d[IDLE_TARGET], errors='coerce').dropna()
    debug(f"[IDLE] Valid numeric values: {len(nums)} (first 5): {nums.head().tolist()}")
    mean_val = float(nums.mean())

    os.makedirs(OUT_IDLE, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    with open(os.path.join(OUT_IDLE, "idle_mean.txt"), "w", encoding="utf-8") as f:
        f.write(f"mean_{IDLE_TARGET}={mean_val}")
    with open(os.path.join(SEL_DIR, "idle_mean.txt"), "w", encoding="utf-8") as f:
        f.write(f"mean_{IDLE_TARGET}={mean_val}")

    success("[IDLE] Saved mean baseline.")
    return {"phase": "idle", "target": IDLE_TARGET, "mean": mean_val}

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    # Prepare folders
    os.makedirs(OUT_ROOT, exist_ok=True)
    os.makedirs(SEL_DIR, exist_ok=True)

    # Data paths
    tx_p = os.path.join(DATA_DIR, "tx.csv")
    rx_p = os.path.join(DATA_DIR, "rx.csv")
    idle_p = os.path.join(DATA_DIR, "idle.csv")

    # Load CSVs
    tx_df = load_phase_csv(tx_p, "TX")
    rx_df = load_phase_csv(rx_p, "RX")
    idle_df = load_phase_csv(idle_p, "IDLE")

    # Train (no eval)
    res_tx = train_tx(tx_df) if tx_df is not None else None
    res_rx = train_rx(rx_df) if rx_df is not None else None
    res_idle = train_idle(idle_df) if idle_df is not None else None
    res_tp_tx = train_throughput_tx(tx_df) if tx_df is not None else None
    res_tp_rx = train_throughput_rx(rx_df) if rx_df is not None else None

    # Manifest used by monitoring
    manifest = {
        "run_tag": RUN_TAG,
        "selected_artifacts": {
            "energy_tx": os.path.join(SEL_DIR, "energy_tx.pkl"),
            "energy_rx": os.path.join(SEL_DIR, "energy_rx.pkl"),
            "idle_mean": os.path.join(SEL_DIR, "idle_mean.txt"),
            "throughput_tx": os.path.join(SEL_DIR, "throughput_tx.pkl"),
            "throughput_rx": os.path.join(SEL_DIR, "throughput_rx.pkl"),
        },
        "config": {
            "CONFIG": CONFIG,
            "TX_FEATURES": TX_FEATURES,
            "RX_FEATURES": RX_FEATURES,
            "TX_TARGET": TX_TARGET,
            "RX_TARGET": RX_TARGET,
            "IDLE_TARGET": IDLE_TARGET,
            "THROUGHPUT_FEATURES": THROUGHPUT_FEATURES,
            "THROUGHPUT_TARGET_TX": THROUGHPUT_TARGET_TX,
            "THROUGHPUT_TARGET_RX": THROUGHPUT_TARGET_RX
        },
        "tx": res_tx,
        "rx": res_rx,
        "idle": res_idle,
        "throughput_tx": res_tp_tx,
        "throughput_rx": res_tp_rx
    }

    with open(os.path.join(OUT_ROOT, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    success(f"Done. Artifacts under {OUT_ROOT} and selected/ under {SEL_DIR}")

if __name__ == "__main__":
    main()
