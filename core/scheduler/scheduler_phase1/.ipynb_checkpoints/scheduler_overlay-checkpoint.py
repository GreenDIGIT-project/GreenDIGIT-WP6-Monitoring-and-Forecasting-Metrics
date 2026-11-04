from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from candidate_generator import generate_candidates
from feature_builder import (
    populate_candidate_predictions,
    predict_qos_probability,
    one_hot_encode,
    align_features,
    QOS_FEATURE_COLUMNS,
)
from scheduler_policy import schedule_candidates


# -------------------------------------------------
# QoS WRAPPER (ROBUST VERSION)
# -------------------------------------------------

def attach_qos_prob(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Real QoS inference with safe fallback.
    Ensures feature alignment with training schema.
    """

    for c in candidates:
        try:
            df = pd.DataFrame([c])

            # IMPORTANT: match training pipeline
            df = one_hot_encode(df)
            df = align_features(df, QOS_FEATURE_COLUMNS)

            c["qos_prob"] = predict_qos_probability(df)

        except Exception:
            c["qos_prob"] = 0.5  # safe neutral fallback

    return candidates


# -------------------------------------------------
# MAIN SCHEDULER ENTRY POINT
# -------------------------------------------------

def schedule_workload(
    workload,
    active_workloads: List,
    links: List[str],
    current_time=None,
    queue_snapshot=None,
):
    """
    Full scheduling pipeline:
    candidates -> ML enrichment -> QoS -> ranking
    """

    print("\n=========== SCHEDULER DEBUG ===========")
    
    if queue_snapshot is None:
    
        print("❌ Scheduler received NO queue snapshot")
    
    else:
    
        print("✅ Scheduler queue visibility enabled")
    
        for link, info in queue_snapshot.items():
    
            print(
                f"[{link}] queued_jobs={info['queue_length']}"
            )
    
    print("=======================================\n")

    
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # 1. Generate candidates
    candidates = generate_candidates(
        workload=workload,
        links=links,
        current_time=current_time,
        active_workloads=active_workloads,
        queue_snapshot=queue_snapshot,
    )

    if not candidates:
        return {"approved": False, "reason": "no_candidates"}

    # 2. ML feature enrichment (throughput, energy, duration, MB)
    enriched = populate_candidate_predictions(
        candidates=candidates,
        workload=workload,
    )

    # 3. QoS prediction
    enriched = attach_qos_prob(enriched)

    # 4. Scheduling decision
    best, ranked = schedule_candidates(
        enriched,
        mode=getattr(workload, "mode", "balanced"),
    )

    if best is None:
        return {"approved": False, "reason": "no_feasible_solution"}

    # 5. SAFETY: ensure score exists
    score = best.get("score", None)

    return {
        "approved": True,
        "exec_id": getattr(workload, "exec_id", None),
        "link_id": best["link_id"],
        "direction": best["direction"],
        "start_time": best["start_time"],
        "src_node": best["src_node"],
        "dst_node": best["dst_node"],
        "score": score,
        "qos": best.get("qos_prob"),
        "energy": best.get("predicted_energy"),
        "throughput": best.get("predicted_throughput"),
        "predicted_duration_s": best.get("predicted_duration_s"),
    }