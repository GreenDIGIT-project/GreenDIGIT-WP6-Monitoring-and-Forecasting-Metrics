import numpy as np
from typing import List, Dict, Any


# =========================================================
# 1. CARBON MODEL (STATIC FOR NOW)
# =========================================================

CARBON_INTENSITY = 100  # placeholder


def compute_carbon(energy: float, carbon_intensity: float = 100.0) -> float:
    return carbon_intensity * energy

# =========================================================
# 2. QoS NORMALIZATION (NEW)
# =========================================================

def normalize_qos(c: Dict[str, Any]) -> float:
    """
    Converts QoS into continuous score in [0,1].

    Supports:
    - scalar qos_prob (old format)
    - or future vector probabilities
    """

    qos = c.get("qos_prob", 0.0)

    # ---------------------------------------------
    # CASE 1: already scalar
    # ---------------------------------------------
    if isinstance(qos, (float, int)):
        return float(qos)

    # ---------------------------------------------
    # CASE 2: probability vector (future-proof)
    # ---------------------------------------------
    try:
        probs = np.array(qos, dtype=float)
        classes = np.arange(len(probs))

        expected_class = np.sum(probs * classes)

        return float(expected_class / (len(probs) - 1))

    except Exception:
        return 0.0


# =========================================================
# 3. OBJECTIVE VECTOR (ALL MINIMIZED)
# =========================================================

def objective_vector(c: Dict[str, Any], carbon_intensity: float = 100.0) -> np.ndarray:

    energy = c.get("predicted_energy", 1e9)
    duration = c.get("predicted_duration_s", 1e9)

    qos_score = normalize_qos(c)  

    # Inject real-time intensity
    carbon = compute_carbon(energy, carbon_intensity)

    qos_cost = 1.0 - qos_score  

    return np.array([
        carbon,
        energy,
        duration,
        qos_cost
    ], dtype=float)


# =========================================================
# 4. DOMINANCE
# =========================================================

def dominates(a: Dict[str, Any], b: Dict[str, Any], carbon_intensity: float = 100.0) -> bool:

    a_vec = objective_vector(a, carbon_intensity)
    b_vec = objective_vector(b, carbon_intensity)

    return np.all(a_vec <= b_vec) and np.any(a_vec < b_vec)


# =========================================================
# 5. PARETO FRONT FILTER
# =========================================================

def pareto_front(candidates: List[Dict[str, Any]], carbon_intensity: float = 100.0) -> List[Dict[str, Any]]:

    front = []

    for i, c in enumerate(candidates):
        dominated = False

        for j, other in enumerate(candidates):
            if i != j and dominates(other, c, carbon_intensity):
                dominated = True
                break

        if not dominated:
            front.append(c)

    return front
# =========================================================
# 6. WEIGHTED SCORING
# =========================================================

def weighted_score(c: Dict[str, Any], mode: str = "balanced", carbon_intensity: float = 100.0) -> float:

    energy = c.get("predicted_energy", 1e9)
    duration = c.get("predicted_duration_s", 1e9)

    qos_score = normalize_qos(c)

    # Inject real-time intensity
    carbon = compute_carbon(energy, carbon_intensity)

    qos_cost = 1.0 - qos_score

    if mode == "carbon":
        w = {"carbon": 0.3, "energy": 0.4, "duration": 0.1, "qos": 0.2}

    elif mode == "energy":
        w = {"carbon": 0.1, "energy": 0.5, "duration": 0.2, "qos": 0.2}

    elif mode == "qos":
        w = {"carbon": 0.1, "energy": 0.2, "duration": 0.2, "qos": 0.5}

    else:
        w = {"carbon": 0.1, "energy": 0.2, "duration": 0.2, "qos": 0.5}

    return (
        w["carbon"] * carbon +
        w["energy"] * energy +
        w["duration"] * duration +
        w["qos"] * qos_cost
    )


def format_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
    """
    Final scheduler view (what you actually want to see / return).
    """

    return {
        # identity
        "link_id": c.get("link_id"),
        "direction": c.get("direction"),

        # topology
        "src_node": c.get("src_node"),
        "dst_node": c.get("dst_node"),

        # time
        "start_time": c.get("start_time"),
        "score": c.get("score"),
        
        # objectives
        "predicted_throughput": c.get("predicted_throughput"),
        "predicted_energy": c.get("predicted_energy"),
        "predicted_duration_s": c.get("predicted_duration_s"),
        "qos_prob": c.get("qos_prob"),
    }
    
# =========================================================
# 7. FINAL SCHEDULER
# =========================================================

def schedule_candidates(
    candidates: List[Dict[str, Any]], 
    mode: str = "balanced", 
    carbon_intensity: float = 100.0  # Pass real-time context from the server wrapper
):

    if not candidates:
        return None, []

    # Filter Pareto front using real-world grid conditions
    pareto = pareto_front(candidates, carbon_intensity)

    ranked = sorted(
        pareto,
        key=lambda c: weighted_score(c, mode, carbon_intensity)
    )

    best = ranked[0] if ranked else None

    if best:
        best["score"] = weighted_score(best, mode, carbon_intensity)
        best = format_candidate(best)

    ranked = [format_candidate(c) for c in ranked]

    return best, ranked