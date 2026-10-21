from typing import Dict, List
from datetime import datetime

from concurrency_model import (
    compute_effective_concurrency
)

from scheduler_state import SimulatedWorkload

from topology_loader import LINK_DIRECTIONS


def compute_all_link_concurrency_states(
    candidate_start: datetime,
    candidate_duration_s: float,
    candidate_bandwidth: float,
    active_workloads: List[SimulatedWorkload],
) -> Dict[str, float]:

    results = {}

    # ---------------------------------------------
    # iterate all links from topology
    # ---------------------------------------------

    for link_id, directions in LINK_DIRECTIONS.items():

        # ---------------------------------------------
        # use default DOWNLINK orientation
        # ---------------------------------------------

        src_node, dst_node = (
            directions["downlink"]
        )

        conc = compute_effective_concurrency(

            candidate_start=candidate_start,

            candidate_duration_s=candidate_duration_s,

            candidate_link_id=link_id,

            candidate_src_node=src_node,
            candidate_dst_node=dst_node,

            candidate_bandwidth=candidate_bandwidth,

            active_workloads=active_workloads
        )

        results[link_id] = conc

    return results