from datetime import datetime, timedelta
from typing import List, Dict, Any

from workload_schema import WorkloadRequest, SCHEDULING_HORIZONS
from rolling_horizon import evaluate_candidate_rolling
from scheduler_state import SimulatedWorkload
from topology_loader import LINK_DIRECTIONS, resolve_nodes
from interference_matrix import INTERFERENCE_MATRIX


# -------------------------------------------------
# TIME SLOT CONFIG
# -------------------------------------------------

TIME_SLOT_MINUTES = 1


# -------------------------------------------------
# HORIZON RESOLUTION
# -------------------------------------------------

def get_horizon(mode: str) -> timedelta:
    if mode not in SCHEDULING_HORIZONS:
        raise ValueError(f"Unknown scheduling mode: {mode}")
    return SCHEDULING_HORIZONS[mode]


# -------------------------------------------------
# TIME SLOT GENERATOR
# -------------------------------------------------

def generate_time_slots(start_time: datetime, end_time: datetime, step_minutes: int = TIME_SLOT_MINUTES) -> List[datetime]:
    slots = []
    current = start_time

    while current <= end_time:
        slots.append(current)
        current += timedelta(minutes=step_minutes)

    return slots


# -------------------------------------------------
# MAIN CANDIDATE GENERATOR (FIXED)
# -------------------------------------------------
from datetime import timezone

def generate_candidates(
    workload: WorkloadRequest,
    links: List[str],
    current_time: datetime,
    active_workloads: List[SimulatedWorkload],
    queue_snapshot=None,
) -> List[Dict[str, Any]]:

    print("\n-------- CANDIDATE GENERATOR --------")
    
    if queue_snapshot is None:
    
        print("❌ Candidate generator has NO queue visibility")
    
    else:
    
        print("✅ Candidate generator sees queue state")
    
        for link, info in queue_snapshot.items():
    
            print(
                f"[{link}] "
                f"queued={info['queue_length']}"
            )
    
    print("-------------------------------------\n")
    
    
    # ---------------------------------------------
    # TIMEZONE NORMALIZATION (CRITICAL FIX)
    # ---------------------------------------------
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    if workload.deadline is not None:
        if workload.deadline.tzinfo is None:
            workload.deadline = workload.deadline.replace(
                tzinfo=timezone.utc
            )

    # ---------------------------------------------
    # 1. HORIZON
    # ---------------------------------------------

    if workload.mode == "carbon":
        # We are evaluating exclusively inside the chosen 15-minute green window
        end_time = current_time + timedelta(minutes=15)
    else:
        horizon = get_horizon(workload.mode)
    
        horizon_end = current_time + horizon
    
        if workload.deadline is not None:
            end_time = min(horizon_end, workload.deadline)
        else:
            end_time = horizon_end

    # ---------------------------------------------
    # 2. TIME SLOTS
    # ---------------------------------------------
    time_slots = generate_time_slots(current_time, end_time)

    # ---------------------------------------------
    # 3. DURATION ESTIMATE
    # ---------------------------------------------
    estimated_duration_s = (workload.data_amount_mb * 8) / workload.bandwidth_req_mbps

    # ---------------------------------------------
    # 4. CANDIDATE BUILDING (NOW DIRECTIONAL)
    # ---------------------------------------------
    candidates = []

    for t in time_slots:
        for link_id in links:
            for direction in ["downlink", "uplink"]:

                # NODE CONTEXT (DIRECTIONAL FIX)
                src_node, dst_node = resolve_nodes(link_id, direction)

                # ROLLING CONCURRENCY
                eff_conc = evaluate_candidate_rolling(
                    candidate_start=t,
                    candidate_duration_s=estimated_duration_s,
                    candidate_link_id=link_id,
                    candidate_direction=direction,
                    candidate_bandwidth=workload.bandwidth_req_mbps,
                    active_workloads=active_workloads,
                )

                print(f"[CANDIDATE] {link_id} {direction} @ {t}")
                print(f"   eff_conc = {eff_conc}")

                # INTERFERENCE FEATURE (safe full structure)
                interference_vector = {
                    k: v for k, v in INTERFERENCE_MATRIX.items()
                    if k[0][0] == link_id or k[1][0] == link_id
                }

                candidates.append({
                    # IDENTIFIERS
                    "link_id": link_id,
                    "direction": direction,

                    # NODE INFO
                    "src_node": src_node,
                    "dst_node": dst_node,

                    # TIME
                    "start_time": t,

                    # CORE METRIC
                    "effective_concurrency": eff_conc,

                    # FEATURES
                    "interference_profile": interference_vector,

                    # PHASE 1
                    "predicted_duration_s": estimated_duration_s,

                    "bandwidth_req_mbps": workload.bandwidth_req_mbps,

                    # PHASE 2 PLACEHOLDERS
                    "predicted_throughput": None,
                    "predicted_energy": None,
                    "predicted_carbon": None,
                    "predicted_qos": None,
                })

    return candidates