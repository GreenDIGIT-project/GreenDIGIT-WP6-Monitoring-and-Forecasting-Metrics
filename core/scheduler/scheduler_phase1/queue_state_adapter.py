from datetime import datetime, timedelta, timezone
from types import SimpleNamespace 

def build_active_workloads(link_queues):
    """
    Convert BOTH queued elements AND thread-active workloads into scheduler-visible objects.
    
    Uses deferred local imports to safely handle circular dependency boundaries 
    with the primary execution engine.
    """
    # --- SAFE LOCAL IMPORTS TO PREVENT CYCLICAL ENTRAPMENT ---
    from api_server import active_running_workloads, registry_lock

    active = []

    # 1. Grab workloads waiting statically in the queues
    for link_id, queue in link_queues.items():
        for item in list(queue):
            try:
                _, _, workload = item

                start_time = datetime.fromisoformat(
                    workload["start_time"].replace("Z", "+00:00")
                )
            except Exception:
                continue

            predicted_duration = workload.get("predicted_duration_s", 0)
            end_time = start_time + timedelta(seconds=float(predicted_duration or 0))

            workload_obj = SimpleNamespace(
                exec_id=workload.get("exec_id"),
                link_id=link_id,
                start_time=start_time,
                end_time=end_time,
                bandwidth_req_mbps=float(workload.get("bandwidth_req_mbps") or 0.0),
                predicted_duration_s=float(workload.get("predicted_duration_s") or 0.0),
                predicted_throughput=float(workload.get("predicted_throughput") or 0.0),
                src_node=workload.get("src_node"),
                dst_node=workload.get("destination_node"), 
                scheduled_direction=workload.get("scheduled_direction"),
            )
            active.append(workload_obj)

    # 2. Grab workloads actively processing right now inside background threads
    with registry_lock:
        for link_id, running_jobs in active_running_workloads.items():
            for exec_id, workload in running_jobs.items():
                try:
                    start_time = datetime.fromisoformat(
                        workload["start_time"].replace("Z", "+00:00")
                    )
                except Exception:
                    start_time = datetime.now(timezone.utc)

                predicted_duration = workload.get("predicted_duration_s", 20)
                end_time = start_time + timedelta(seconds=float(predicted_duration or 0))

                running_obj = SimpleNamespace(
                    exec_id=exec_id,
                    link_id=link_id,
                    start_time=start_time,
                    end_time=end_time,
                    bandwidth_req_mbps=float(workload.get("bandwidth_req_mbps") or 0.0),
                    predicted_duration_s=float(predicted_duration or 0.0),
                    predicted_throughput=float(workload.get("predicted_throughput") or 0.0),
                    src_node=workload.get("src_node"),
                    dst_node=workload.get("destination_node"), 
                    scheduled_direction=workload.get("scheduled_direction"),
                )
                active.append(running_obj)

    return active