from datetime import datetime, timedelta
from typing import List

from concurrency_model import compute_effective_concurrency
from scheduler_state import SimulatedWorkload
from topology_loader import resolve_nodes


# -------------------------------------------------
# TIME GRANULARITY
# -------------------------------------------------

TIME_SLOT_SECONDS = 60


# -------------------------------------------------
# SPLIT INTO INTERVALS
# -------------------------------------------------

def split_into_intervals(start_time: datetime, duration_s: float) -> List[datetime]:
    intervals = []

    n_steps = int((duration_s + TIME_SLOT_SECONDS - 1) // TIME_SLOT_SECONDS)

    for i in range(n_steps):
        intervals.append(start_time + timedelta(seconds=i * TIME_SLOT_SECONDS))

    return intervals


# -------------------------------------------------
# ROLLING HORIZON EVALUATION (FIXED)
# -------------------------------------------------

def evaluate_candidate_rolling(
    candidate_start: datetime,
    candidate_duration_s: float,
    candidate_link_id: str,
    candidate_direction: str,
    candidate_bandwidth: float,
    active_workloads: List[SimulatedWorkload],
) -> float:

    print("\n[ROLLING] evaluating candidate")
    print(f"[ROLLING] candidate_start={candidate_start}")
    print(f"[ROLLING] active_workloads={len(active_workloads)}")
    
    intervals = split_into_intervals(candidate_start, candidate_duration_s)

    weighted_sum = 0.0
    total_weight = 0.0

    for t in intervals:

        elapsed = (t - candidate_start).total_seconds()
        remaining_duration = candidate_duration_s - elapsed

        if remaining_duration <= 0:
            break

        interval_duration = min(TIME_SLOT_SECONDS, remaining_duration)

        candidate_src, candidate_dst = resolve_nodes(candidate_link_id, candidate_direction)

        c = compute_effective_concurrency(
            candidate_start=t,
            candidate_duration_s=interval_duration,
            candidate_link_id=candidate_link_id,
            candidate_src_node=candidate_src,
            candidate_dst_node=candidate_dst,
            candidate_bandwidth=candidate_bandwidth,
            active_workloads=active_workloads,
        )

        weighted_sum += c * interval_duration
        total_weight += interval_duration

    return weighted_sum / max(total_weight, 1e-6)