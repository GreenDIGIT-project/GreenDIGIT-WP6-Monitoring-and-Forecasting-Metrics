from datetime import datetime
from typing import List

from scheduler_state import SimulatedWorkload
from topology_loader import LINK_DIRECTIONS, resolve_nodes
from interference_matrix import INTERFERENCE_MATRIX


# -------------------------------------------------
# LINK CAPACITY MODEL
# -------------------------------------------------

LINK_CAPACITY_MBPS = 2.4


def compute_same_link_factor(total_bandwidth: float) -> float:
    """
    Penalizes overload on the same physical link.
    """

    if total_bandwidth <= LINK_CAPACITY_MBPS:
        return 0.0

    return (total_bandwidth - LINK_CAPACITY_MBPS) / LINK_CAPACITY_MBPS


# -------------------------------------------------
# DIRECTION RESOLUTION
# -------------------------------------------------

def get_direction(link_id: str, src_node: str, dst_node: str) -> str:
    """
    Determines uplink/downlink based on YAML topology.
    """

    if link_id not in LINK_DIRECTIONS:
        raise ValueError(f"Unknown link_id: {link_id}")

    down_src, down_dst = LINK_DIRECTIONS[link_id]["downlink"]

    return (
        "downlink"
        if (src_node == down_src and dst_node == down_dst)
        else "uplink"
    )


# -------------------------------------------------
# SYMMETRIC INTERFERENCE LOOKUP (FIX)
# -------------------------------------------------

def get_interference_weight(
    link_a: str,
    dir_a: str,
    link_b: str,
    dir_b: str
) -> float:
    """
    Symmetric structural lookup that extracts a localized asymmetric weight 
    specifically for link_a (the requested candidate workload).
    """
    key_1 = (
        (link_a, dir_a),
        (link_b, dir_b)
    )

    key_2 = (
        (link_b, dir_b),
        (link_a, dir_a)
    )

    # link_a is always the candidate link ID passed from the concurrency engine
    candidate_link_id = link_a

    if key_1 in INTERFERENCE_MATRIX:
        return INTERFERENCE_MATRIX[key_1].get(candidate_link_id, 0.1)

    if key_2 in INTERFERENCE_MATRIX:
        return INTERFERENCE_MATRIX[key_2].get(candidate_link_id, 0.1)

    # conservative fallback
    return 0.1


# -------------------------------------------------
# OVERLAP MODEL
# -------------------------------------------------

def compute_overlap(
    candidate_start: datetime,
    candidate_duration_s: float,
    workload: SimulatedWorkload
) -> float:

    cand_start = candidate_start.timestamp()
    cand_end = cand_start + candidate_duration_s

    work_start = workload.start_time.timestamp()
    work_end = work_start + workload.predicted_duration_s

    return max(
        0.0,
        min(cand_end, work_end) - max(cand_start, work_start)
    )


# -------------------------------------------------
# MAIN CONCURRENCY ENGINE
# -------------------------------------------------

def compute_effective_concurrency(
    candidate_start: datetime,
    candidate_duration_s: float,

    candidate_link_id: str,
    candidate_src_node: str,
    candidate_dst_node: str,

    candidate_bandwidth: float,

    active_workloads: List[SimulatedWorkload],

    debug: bool = False
) -> float:

    total_concurrency = 0.0
    same_link_bandwidth = 0.0

    # -------------------------------------------------
    # Candidate direction
    # -------------------------------------------------

    candidate_direction = get_direction(
        candidate_link_id,
        candidate_src_node,
        candidate_dst_node
    )

    if debug:
        print("\nWORKLOAD ANALYSIS")
        print("----------------------------------------")

    # -------------------------------------------------
    # ACTIVE WORKLOAD LOOP
    # -------------------------------------------------

    for w in active_workloads:

        # Resolve topology for active workload
        work_src, work_dst = resolve_nodes(w.link_id)

        work_direction = get_direction(
            w.link_id,
            work_src,
            work_dst
        )

        overlap = compute_overlap(
            candidate_start,
            candidate_duration_s,
            w
        )

        overlap_ratio = overlap / max(candidate_duration_s, 1e-6)

        if debug:
            print(f"\nWorkload link: {w.link_id}")
            print(f"Workload direction: {work_direction}")
            print(f"BW: {w.bandwidth_req_mbps}")
            print(f"Overlap(s): {overlap:.2f}")
            print(f"Overlap ratio: {overlap_ratio:.4f}")

        # -------------------------------------------------
        # SAME LINK INTERACTION
        # -------------------------------------------------

        if w.link_id == candidate_link_id:

            same_link_bandwidth += w.bandwidth_req_mbps * overlap_ratio

            if debug:
                print("SAME LINK")

            continue

        # -------------------------------------------------
        # DIRECTIONAL INTERFERENCE EXPANSION
        # -------------------------------------------------

        pair_weight = get_interference_weight(
            candidate_link_id,
            candidate_direction,
            w.link_id,
            work_direction
        )

        # -------------------------------------------------
        # FINAL CONTRIBUTION
        # -------------------------------------------------

        contribution = overlap_ratio * pair_weight
        total_concurrency += contribution

        if debug:
            print("CROSS LINK")
            print(f"Pair weight = {pair_weight:.4f}")
            print(f"Contribution = {contribution:.4f}")

    # -------------------------------------------------
    # SAME LINK CAPACITY PENALTY
    # -------------------------------------------------

    total_same_bw = same_link_bandwidth + candidate_bandwidth

    penalty = compute_same_link_factor(total_same_bw)

    total_concurrency += penalty

    if debug:
        print("\nSAME LINK CAPACITY")
        print(f"Total BW = {total_same_bw:.4f}")
        print(f"Penalty = {penalty:.4f}")
        print("----------------------------------------")
        print(f"FINAL = {total_concurrency:.4f}")

    return total_concurrency