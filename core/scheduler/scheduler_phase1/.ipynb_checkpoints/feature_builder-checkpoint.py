import os
from datetime import datetime
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import joblib


# =========================================================
# LOAD MODELS
# =========================================================

BASE_DIR = os.path.dirname(__file__)
MODEL_DIR = os.path.join(BASE_DIR, "models")

THROUGHPUT_MODEL = joblib.load(os.path.join(MODEL_DIR, "throughput_model.pkl"))
MB_MODEL = joblib.load(os.path.join(MODEL_DIR, "mb_model.pkl"))
ENERGY_MODEL = joblib.load(os.path.join(MODEL_DIR, "energy_model.pkl"))
QOS_MODEL = joblib.load(os.path.join(MODEL_DIR, "qos_model.pkl"))

BASE_FEATURE_COLUMNS = joblib.load(os.path.join(MODEL_DIR, "feature_columns.pkl"))
ENERGY_FEATURE_COLUMNS = joblib.load(os.path.join(MODEL_DIR, "energy_feature_columns.pkl"))
QOS_FEATURE_COLUMNS = joblib.load(os.path.join(MODEL_DIR, "qos_feature_columns.pkl"))


# =========================================================
# NODE / LINK ENUMS
# =========================================================

ALL_NODES = [
    "node01","node02","node03","node04","node05","node06",
    "node07","node08","node09","node10","node11","node12",
]

ALL_LINKS = ["link1","link2","link3","link4","link5","link6"]


# =========================================================
# TEMPORAL FEATURES
# =========================================================

def build_temporal_features(timestamp: datetime) -> Dict[str, float]:

    hour = timestamp.hour + timestamp.minute / 60.0

    return {
        "day_of_week": timestamp.weekday(),
        "is_weekend": 1 if timestamp.weekday() >= 5 else 0,
        "hour_sin": np.sin(2 * np.pi * hour / 24),
        "hour_cos": np.cos(2 * np.pi * hour / 24),
    }


# =========================================================
# BASE FEATURE ROW
# =========================================================

def build_base_feature_row(candidate: Dict[str, Any], workload) -> Dict[str, Any]:

    temporal = build_temporal_features(candidate["start_time"])

    return {
        **temporal,

        # workload
        "data_amount_mb": workload.data_amount_mb,
        "bandwidth_req_mbps": workload.bandwidth_req_mbps,

        # topology
        "src_node": candidate["src_node"],
        "dst_node": candidate["dst_node"],
        "link_id": candidate["link_id"],

        # concurrency
        "effective_concurrency": candidate["effective_concurrency"],
    }


# =========================================================
# OPTIMIZED BATCH AUXILIARY FUNCTIONS
# =========================================================

def align_features_batch(df_input: pd.DataFrame, expected_columns: List[str]) -> pd.DataFrame:
    """ High speed allocation using empty templates rather than tracking string flags in a loop """
    df_aligned = pd.DataFrame(0.0, index=df_input.index, columns=expected_columns)
    common_cols = df_input.columns.intersection(expected_columns)
    df_aligned[common_cols] = df_input[common_cols]
    return df_aligned


def predict_qos_probability_batch(X_qos: pd.DataFrame) -> np.ndarray:
    """ Vectorized array dot-product replacing multi-class prob loops """
    probs = QOS_MODEL.predict_proba(X_qos)
    classes = np.arange(probs.shape[1])
    expected_classes = probs.dot(classes)
    return expected_classes / (probs.shape[1] - 1)


# =========================================================
# MAIN BATCH PIPELINE (REPLACES predict_candidate_metrics)
# =========================================================

def populate_candidate_predictions(candidates: List[Dict[str, Any]], workload) -> List[Dict[str, Any]]:
    if not candidates:
        return candidates

    # 1. Gather all individual lines into a clean structured bulk table instantly
    base_rows = [build_base_feature_row(c, workload) for c in candidates]
    df_batch = pd.DataFrame(base_rows)

    # 2. Run structural hot encoding exactly once across the whole run
    df_encoded = pd.get_dummies(df_batch, columns=["src_node", "dst_node", "link_id"])

    # 3. Structural alignment initialization
    X_base = align_features_batch(df_encoded, BASE_FEATURE_COLUMNS)

    # 4. Run model estimations as large vector blocks
    predicted_throughputs = THROUGHPUT_MODEL.predict(X_base)
    predicted_mbs = MB_MODEL.predict(X_base)

    # 5. Fast mathematical computation over array blocks 
    predicted_durations = (predicted_mbs * 8) / (predicted_throughputs + 1e-9)

    # 6. Process Energy Model using pre-allocated matrix values
    energy_df = X_base.copy()
    energy_df["predicted_MB"] = predicted_mbs
    energy_df["predicted_throughput_mbps"] = predicted_throughputs
    energy_df["predicted_duration_s"] = predicted_durations

    X_energy = align_features_batch(energy_df, ENERGY_FEATURE_COLUMNS)
    predicted_energies = ENERGY_MODEL.predict(X_energy)

    # 7. Process multi-class QoS matrices
    qos_df = align_features_batch(df_encoded, QOS_FEATURE_COLUMNS)
    qos_scores = predict_qos_probability_batch(qos_df)

    # 8. Unpack matrix blocks back to the dictionary elements instantly
    for idx, candidate in enumerate(candidates):
        candidate["predicted_throughput"] = float(predicted_throughputs[idx])
        candidate["predicted_MB"] = float(predicted_mbs[idx])
        candidate["predicted_duration_s"] = float(predicted_durations[idx])
        candidate["predicted_energy"] = float(predicted_energies[idx])
        candidate["qos_prob"] = float(qos_scores[idx])

    return candidates

# =========================================================
# LEGACY COMPATIBILITY LAYER (FIXES SERVER IMPORT CRASHES)
# =========================================================

def predict_qos_probability(X: pd.DataFrame) -> float:
    """ Legacy single-row fallback wrapper for QoS probability """
    scores = predict_qos_probability_batch(X)
    return float(scores[0])


def one_hot_encode(df: pd.DataFrame) -> pd.DataFrame:
    """ Legacy single-dataframe fallback wrapper for encoding """
    return pd.get_dummies(df, columns=["src_node", "dst_node", "link_id"])


def align_features(df: pd.DataFrame, expected_columns: List[str]) -> pd.DataFrame:
    """
    Legacy fallback wrapper to prevent api_server.py 
    from throwing an align_features import error.
    """
    for col in expected_columns:
        if col not in df.columns:
            df[col] = 0
    return df[expected_columns]