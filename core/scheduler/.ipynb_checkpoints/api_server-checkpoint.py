import os
import sys
import json
import time
import uuid
from typing import Optional
import heapq
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import threading
import uvicorn
from types import SimpleNamespace
from influxdb import InfluxDBClient
from carbon_client import get_carbon_intensity

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CIM_DIR = os.path.abspath(os.path.join(BASE_DIR, "../CIM"))
SCHEDULER_DIR = os.path.abspath(os.path.join(BASE_DIR, "./scheduler_phase1"))

for path in [CIM_DIR, SCHEDULER_DIR]:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

try:
    import cim_submitter
    import workload_manager as wm
    import query_predictions as qp

except ImportError as e:
    print(f"[CRITICAL] CIM import failed: {e}")
    sys.exit(1)


# =========================================================
# SCHEDULER IMPORTS
# =========================================================

try:
    from scheduler_overlay import schedule_workload
    from queue_state_adapter import build_active_workloads
except ImportError as e:
    print(f"[CRITICAL] Scheduler import failed: {e}")
    sys.exit(1)
    
# Now you can safely import the CIM modules
import cim_submitter

import workload_manager as wm
import query_predictions as qp


from scheduler_overlay import schedule_workload
from queue_state_adapter import build_active_workloads

# Add import if the module exists, e.g.:
# import site_adapter as sa


INFLUX_HOST = "10.64.44.196"
INFLUX_PORT = 8086
INFLUX_USER = "ilbekas"
INFLUX_PASS = "!I[j~gtN25m{"
INFLUX_DB = "workloads_IoT"

influx_client = InfluxDBClient(
    host=INFLUX_HOST,
    port=INFLUX_PORT,
    username=INFLUX_USER,
    password=INFLUX_PASS,
    database=INFLUX_DB
)

try:
    influx_client.ping()
    print(f"[Influx] Connected to {INFLUX_DB}")
except Exception as e:
    print(f"[WARN] Could not connect to InfluxDB: {e}")


def log(msg, link_name=None):
    """Timestamped log to stdout."""
    timestamp = datetime.now(timezone.utc).isoformat()
    if link_name:
        line = f"[{timestamp}] [{link_name}] {msg}"
    else:
        line = f"[{timestamp}] {msg}"

    print(line)


def push_prediction_to_influx(prediction: dict):
    """
    Store workload prediction into InfluxDB (Workloads_IoT).
    """
    try:
        point = {
            "measurement": "workloads",
            "tags": {
                "exec_unit_id": prediction.get("exec_unit_id"),
                "src_node": prediction.get("src_node"),
                "dst_node": prediction.get("dst_node"),
            },
            "time": prediction.get("start_time"),
            "fields": {
                "duration_s": float(prediction.get("duration_s") or 0.0),
                "data_amount_mb": float(prediction.get("data_amount_mb") or 0.0),
                "bandwidth_req_mbps": float(prediction.get("bandwidth_req_mbps") or 0.0),
                "throughput_mbps": float(prediction.get("throughput_mbps") or 0.0),
                "jitter_ms": float(prediction.get("jitter_ms") or 0.0),
                "packet_loss_percent": float(prediction.get("packet_loss_percent") or 0.0),
                "total_tx_Wh": float(prediction.get("energy_results", {}).get("total_tx_Wh") or 0.0),
                "total_rx_Wh": float(prediction.get("energy_results", {}).get("total_rx_Wh") or 0.0),
                "total_energy_Wh": float(prediction.get("energy_results", {}).get("total_energy_Wh") or 0.0),
                "MB": float(prediction.get("energy_results", {}).get("MB") or 0.0),
            }
        }

        print(json.dumps(point, indent=4))
        
        influx_client.write_points([point])
        print(f"Stored workload {prediction['exec_unit_id']} in InfluxDB.")

    except Exception as e:
        print(f"InfluxDB write failed: {e}")

app = FastAPI(title="Workload Energy API")

@app.get("/metadata")
async def metadata():
    try:
        import site_adapter as sa  # or whichever module defines get_combined_metadata
        return sa.get_combined_metadata()
    except Exception as e:
        return {"error": f"Failed to fetch site metadata: {str(e)}"}
# -------------------------------
# API Request Model
# -------------------------------
'''
class WorkloadRequest(BaseModel):
    destination_node: str
    bandwidth: str
    data_amount: str
    start_time: str = None  # optional ISO8601 string
'''

class WorkloadRequest(BaseModel):

    data_amount: str
    bandwidth: str

    deadline: Optional[datetime] = None
    mode: str = "balanced"
    tracking_id: Optional[str] = None  # <--- ADD THIS FIELD
# -------------------------------
# LINK QUEUE MANAGEMENT
# -------------------------------
link_queues = {link_name: [] for link_name in wm.links.keys()}

active_running_workloads = {link_name: {} for link_name in wm.links.keys()}
registry_lock = threading.Lock()

# ---------------------------------------------------------
# MINIMAL LINK MONITORING STATE
# ---------------------------------------------------------
link_runtime_state = {
    link_name: {
        "worker_alive": False,
        "current_exec_id": None,
        "current_phase": "idle",
        "phase_start_time": None,
        "total_enqueued": 0,
        "completed": 0,
        "failed": 0
    }
    for link_name in wm.links.keys()
}


def get_start_dt(workload):
    return datetime.fromisoformat(
        workload["start_time"].replace("Z", "+00:00")
    )



def execute_workload_async(workload: dict, link_name: str):
    """
    Independent workload execution lifecycle. 
    Runs inside a transient background thread to enable true link concurrency
    without blocking the primary queue consumer loop.
    """
    state = link_runtime_state[link_name]
    exec_id = workload.get("exec_id")

    # --- REGISTER TO CURRENTLY RUNNING REGISTRY ---
    with registry_lock:
        active_running_workloads[link_name][exec_id] = workload

    try:
        # ---------------------------
        # PHASE: running_iperf
        # ---------------------------
        state["current_phase"] = "running_iperf"
        state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

        result = wm.run_workload(workload, link_name)
        
        # Guard against None values if an unexpected execution edge-case hits
        execution_failed = result.get("execution_failed", False) if result else True

        # ---------------------------
        # PHASE: tail_wait
        # ---------------------------
        state["current_phase"] = "tail_wait"
        state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

        time.sleep(20)

        # ---------------------------
        # PHASE: analyzing
        # ---------------------------
        state["current_phase"] = "analyzing"
        state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

        analysis_start = time.time()
        prediction = qp.analyze_workload(result)
        analysis_duration = time.time() - analysis_start

        print(
            f"[{datetime.now()}] [{link_name}] "
            f"ANALYZE DONE in {analysis_duration:.2f}s"
        )

        # ---------------------------
        # PHASE: submitting
        # ---------------------------
        state["current_phase"] = "submitting"
        state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

        submit_start = time.time()
        status = "failed" if execution_failed else "done"

        try:
            cim_submitter.submit_to_cim(prediction, status=status)
        except Exception as cim_err:
            print(f"[{datetime.now()}] [{link_name}] CIM Error: {cim_err}")

        try:
            push_prediction_to_influx(prediction)
        except Exception as influx_err:
            print(f"[{datetime.now()}] [{link_name}] Influx Error: {influx_err}")

        submit_duration = time.time() - submit_start

        print(
            f"[{datetime.now()}] [{link_name}] "
            f"SUBMIT DONE in {submit_duration:.2f}s"
        )

        if execution_failed:
            state["failed"] += 1
        else:
            state["completed"] += 1

    except Exception as e:
        print(f"[{datetime.now()}] [{link_name}] HARD ERROR inside thread loop for {exec_id}: {e}")
        state["failed"] += 1

    finally:
        # --- REMOVE FROM REGISTRY ON COMPLETION ---
        with registry_lock:
            if exec_id in active_running_workloads[link_name]:
                del active_running_workloads[link_name][exec_id]

        remaining = len(link_queues[link_name])
        print(
            f"[{datetime.now()}] [{link_name}] "
            f"Finished concurrent execution of {exec_id}. Remaining in queue: {remaining}"
        )


def run_link_queue(link_name: str):
    """
    Non-blocking, infinite concurrency dispatcher thread.
    Pops scheduled items immediately when their start time matures and hands 
    them off to an un-capped background thread pool.
    """
    state = link_runtime_state[link_name]
    state["worker_alive"] = True
    state["current_phase"] = "idle"
    state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

    print(f"[{datetime.now()}] [WORKER START] Parallel thread dispatcher started for {link_name}")

    while True:
        # Break cleanly if the heap queue becomes empty
        if not link_queues[link_name]:
            break

        now = datetime.now(timezone.utc)
        
        # Peek at the top item without popping it from the heap
        next_start_dt, _, _ = link_queues[link_name][0]
        
        if next_start_dt > now:
            # Earliest item is in the future. Let the loop spin.
            state["current_phase"] = "waiting_start_time"
            state["phase_start_time"] = now.isoformat()
            time.sleep(0.5)  # Snappy poll interval for precision start timings
            continue

        # The item at the top is ready to run now! Pop it out
        start_dt, exec_id, workload = heapq.heappop(link_queues[link_name])
        state["current_exec_id"] = exec_id
        state["current_phase"] = "dispatching_concurrent_job"
        state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

        # ---------------------------------------------------------------------
        # DISPATCH EVENT: Fire-and-forget background processing thread
        # ---------------------------------------------------------------------
        threading.Thread(
            target=execute_workload_async,
            args=(workload, link_name),
            daemon=True
        ).start()

    # Reset metadata state maps when the consumer cleanly wraps up
    state["worker_alive"] = False
    state["current_exec_id"] = None
    state["current_phase"] = "idle"
    state["phase_start_time"] = datetime.now(timezone.utc).isoformat()

    print(f"[{datetime.now()}] [WORKER STOP] Thread dispatcher stopping for {link_name}")



def enqueue_workload(workload: dict):
    """
    Place workload into the correct link queue and start worker if idle.
    """
    link_name = wm.get_link_name_from_dest(workload["destination_node"])
    if link_name not in link_queues:
        raise RuntimeError(f"Unknown link for destination {workload['destination_node']}")

    #link_queues[link_name].append(workload)
    start_dt = get_start_dt(workload)

    # SAFE: we store (time, id, workload)
    heapq.heappush(
        link_queues[link_name],
        (start_dt, workload["exec_id"], workload)
    )
    
    state = link_runtime_state[link_name]
    state["total_enqueued"] += 1

    q_size = len(link_queues[link_name])
    log(f"ENQUEUED {workload['exec_id']}: Queue depth now {q_size} | Workload details: {json.dumps(workload)}", link_name)

    # Start worker ONLY if not already running
    if not state["worker_alive"]:
        log("Starting worker thread for link", link_name)
        threading.Thread(
            target=run_link_queue,
            args=(link_name,),
            daemon=True
        ).start()



def fetch_greenest_window(current_time: datetime, user_deadline: Optional[datetime]) -> datetime:
    """
    Calculates the exact experimental horizon constraint: min(user_deadline, now + 12h)
    Queries the isolated carbon client and returns the greenest interval timestamp.
    """
    # 1. Enforce the strict 12-hour maximum experiment bounding window
    max_horizon_limit = current_time + timedelta(hours=12)
    
    if user_deadline:
        if user_deadline.tzinfo is None:
            user_deadline = user_deadline.replace(tzinfo=timezone.utc)
        # Apply thesis requirement: min(deadline, 12h)
        end_window = min(user_deadline, max_horizon_limit)
    else:
        end_window = max_horizon_limit

    if end_window <= current_time:
        return current_time

    # 2. Call your isolated client function
    intervals = get_carbon_intensity(current_time, end_window)
    
    if intervals:
        # Pull the absolute lowest intensity data point
        greenest_interval = min(intervals, key=lambda x: x["carbon_intensity"])
        return datetime.fromisoformat(greenest_interval["start_time"].replace("Z", "+00:00"))
        
    print("[EXPERIMENT] Carbon API unavailable or timed out. Defaulting to immediate window.")
    return current_time

    
    

def parse_datetime_safe(dt_str: str):
    if not dt_str:
        return None
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


# -------------------------------
# API Endpoint
# -------------------------------
'''
@app.post("/execute")
async def execute(request: WorkloadRequest):
    workload = {
        "destination_node": request.destination_node,
        "bandwidth": request.bandwidth,
        "data_amount": request.data_amount,
        "start_time": request.start_time or datetime.utcnow().isoformat() + "Z",
        "exec_id": f"exec_{uuid.uuid4().hex}"
    }

    # Validate destination node
    if not wm.get_link_name_from_dest(workload["destination_node"]):
        raise HTTPException(status_code=404, detail="Destination node not found")

    enqueue_workload(workload)
    return {"status": "Queued", "exec_id": workload["exec_id"]}
'''
# -------------------------------
# API Endpoint for checking health every 30sec
# -------------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}



def build_queue_snapshot(link_queues):
    snapshot = {}

    for link_id, queue in link_queues.items():
        # Get count of items statically waiting in the heap
        heap_length = len(queue)
        
        # Get count of items currently running in parallel threads
        with registry_lock:
            running_jobs_map = active_running_workloads.get(link_id, {})
            running_length = len(running_jobs_map)

        snapshot[link_id] = {
            # Express total depth as: Waiting + Running
            "queue_length": heap_length + running_length,
            "queued_jobs": [],
        }

        # 1. Append the actively running concurrent background jobs first
        with registry_lock:
            for exec_id, workload in running_jobs_map.items():
                snapshot[link_id]["queued_jobs"].append({
                    "exec_id": f"{exec_id} [RUNNING]",
                    "start_time": workload.get("start_time"),
                    "bandwidth_mbps": workload.get("bandwidth_req_mbps"), 
                    "predicted_duration_s": workload.get("predicted_duration_s"),
                    "scheduled_link_id": workload.get("scheduled_link_id"),
                })

        # 2. Append the items statically waiting in the priority queue next
        for item in list(queue):
            _, _, workload = item

            snapshot[link_id]["queued_jobs"].append({
                "exec_id": f"{workload.get('exec_id')} [PENDING]",
                "start_time": workload.get("start_time"),
                "bandwidth_mbps": workload.get("bandwidth_req_mbps"), 
                "predicted_duration_s": workload.get("predicted_duration_s"),
                "scheduled_link_id": workload.get("scheduled_link_id"),
            })

    return snapshot


@app.post("/execute")
async def execute(request: WorkloadRequest):

    exec_id = f"exec_{uuid.uuid4().hex}"
    current_time_utc = datetime.now(timezone.utc)

    # -------------------------------------------------
    # TIME NORMALIZATION (DEADLINE ONLY)
    # -------------------------------------------------
    def parse_datetime_safe(dt_str: str):
        if dt_str is None:
            return None
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    deadline_dt = request.deadline

    # enforce UTC-aware consistency
    if deadline_dt is not None and deadline_dt.tzinfo is None:
        deadline_dt = deadline_dt.replace(tzinfo=timezone.utc)

    # -------------------------------------------------
    # FEATURE ENGINEERING
    # -------------------------------------------------
    data_amount_mb = (
        wm.parse_size_to_bytes(request.data_amount) / (1024 * 1024)
    )

    bandwidth_mbps = wm.parse_bandwidth_to_mbps(request.bandwidth)
    
    chosen_mode = request.mode or "balanced"

    # =================================================================
    # PHASE 1: MACRO TIME-SHIFT INTERVAL ALLOCATION (CARBON CONSTRAINTS)
    # =================================================================
    evaluated_scheduling_time = current_time_utc

    if chosen_mode == "carbon":
        # Hardcoded experiment safety cap threshold
        max_horizon_limit = current_time_utc + timedelta(hours=12)
        
        # Implement thesis explicit horizon logic: min(deadline, now + 12h)
        if deadline_dt:
            end_window = min(deadline_dt, max_horizon_limit)
        else:
            end_window = max_horizon_limit

        if end_window > current_time_utc:
            # Query your isolated raw de-aggregated carbon module
            intervals = get_carbon_intensity(current_time_utc, end_window)
            if intervals:
                greenest_interval = min(intervals, key=lambda x: x["carbon_intensity"])
                evaluated_scheduling_time = datetime.fromisoformat(
                    greenest_interval["start_time"].replace("Z", "+00:00")
                )
                print(f"[CARBON MODE] Shifted evaluation target window to: {evaluated_scheduling_time.isoformat()}")
            else:
                print("[EXPERIMENT WARN] Carbon API returned no intervals. Defaulting to immediate window.")

    # -------------------------------------------------
    # BUILD SCHEDULER WORKLOAD (UNIFIED FORMAT)
    # -------------------------------------------------
    scheduler_workload = SimpleNamespace(
        exec_id=exec_id,

        data_amount_mb=data_amount_mb,
        bandwidth_req_mbps=bandwidth_mbps,

        mode=chosen_mode,

        # scheduling constraints
        deadline=deadline_dt,
    )

    # -------------------------------------------------
    # ACTIVE SYSTEM STATE
    # -------------------------------------------------
    active_workloads = build_active_workloads(link_queues)

    queue_snapshot = build_queue_snapshot(link_queues)

    print("\n========== QUEUE SNAPSHOT ==========")
    
    for link, info in queue_snapshot.items():
    
        print(f"[{link}] queue_len={info['queue_length']}")
    
        for job in info["queued_jobs"]:
    
            print(
                f"   -> {job['exec_id']} | "
                f"bw={job['bandwidth_mbps']} | "
                f"start={job['start_time']}"
            )
    
    print("====================================\n")
    
    # -------------------------------------------------
    # RUN SCHEDULER (EVALUATED AT THE SPECIFIC CHOSEN TIMELINE)
    # -------------------------------------------------
    scheduling_result = schedule_workload(
        workload=scheduler_workload,
        current_time=evaluated_scheduling_time,  # <--- Evaluates future overlaps if carbon mode is active
        active_workloads=active_workloads,
        links=list(wm.links.keys()),
        queue_snapshot=queue_snapshot,
    )

    # -------------------------------------------------
    # REJECTION CASE
    # -------------------------------------------------
    if not scheduling_result.get("approved"):
        return {
            "status": "rejected",
            "exec_id": exec_id,
            "reason": scheduling_result.get("reason"),
        }

    # -------------------------------------------------
    # QUEUE PAYLOAD (EXECUTION LAYER)
    # -------------------------------------------------
    workload = {
        "exec_id": exec_id,
        "tracking_id": request.tracking_id or exec_id,  # <--- UPDATE THIS FIELD

        # NOTE: scheduler chooses placement now
        "scheduled_link_id": scheduling_result["link_id"],
        "src_node": scheduling_result["src_node"],
        "destination_node": scheduling_result["dst_node"],

        # Captures the future shifted interval starting time
        "start_time": scheduling_result["start_time"].isoformat(),

        "bandwidth": request.bandwidth,   
        "data_amount": request.data_amount,

        "bandwidth_req_mbps": bandwidth_mbps,
        "data_amount_mb": data_amount_mb,
        "scheduled_direction": scheduling_result.get("direction"),
        
        "mode": scheduler_workload.mode,

        # predictions
        "predicted_duration_s": scheduling_result.get("predicted_duration_s"),
        "predicted_throughput": scheduling_result.get("throughput"),
        "predicted_energy": scheduling_result.get("energy"),
        "qos_prob": scheduling_result.get("qos"),
        "score": scheduling_result.get("score"),
    }

    # -------------------------------------------------
    # ENQUEUE
    # -------------------------------------------------
    enqueue_workload(workload)

    # -------------------------------------------------
    # RESPONSE
    # -------------------------------------------------
    return {
        "status": "scheduled",
        "exec_id": exec_id,

        "scheduled_link": scheduling_result["link_id"],
        "destination_node": scheduling_result["dst_node"],
        "scheduled_start_time": workload["start_time"],

        "predicted_duration_s": scheduling_result.get("predicted_duration_s"),
        "predicted_throughput": scheduling_result.get("throughput"),
        "predicted_energy": scheduling_result.get("energy"),
        "qos_prob": scheduling_result.get("qos"),
        "score": scheduling_result.get("score"),
    }
# -------------------------------
# MONITORING ENDPOINT
# -------------------------------
from fastapi.responses import JSONResponse

@app.get("/monitor/links")
async def monitor_links():
    # Returns the full runtime state for all links
    return JSONResponse(content=link_runtime_state)

@app.get("/metadata")
async def metadata():
    try:
        return sa.get_combined_metadata()
    except Exception as e:
        return {
            "error": f"Failed to fetch site metadata: {str(e)}"
        }


# -------------------------------------------------------------
# ADD: Pydantic Model for Pre-Calculated Baseline Allocations
# -------------------------------------------------------------
class ExplicitWorkloadRequest(BaseModel):
    source_node: str
    destination_node: str
    bandwidth: str
    data_amount: str
    start_time: str
    tracking_id: str  # Captures rich scenario ID string: mode_bw_flex_JOB_XXXX

# -------------------------------------------------------------
# ADD: Direct Execution Endpoint
# -------------------------------------------------------------
@app.post("/execute-explicit")
async def execute_explicit(request: ExplicitWorkloadRequest):
    """
    Directly queues workloads that have been pre-scheduled by baselines.
    Bypasses schedule_workload() calculation loops.
    """
    exec_id = f"exec_{uuid.uuid4().hex}"
    
    # Standardize types using your framework's native parsing utilities
    data_amount_mb = wm.parse_size_to_bytes(request.data_amount) / (1024 * 1024)
    bandwidth_mbps = wm.parse_bandwidth_to_mbps(request.bandwidth)
    
    # Resolve the correct tracking link channel ID dynamically using destination mapping
    try:
        link_id = wm.get_link_name_from_dest(request.destination_node)
    except Exception:
        link_id = None
        
    if not link_id or link_id not in link_queues:
        raise HTTPException(
            status_code=404, 
            detail=f"No valid communication link found tracking destination {request.destination_node}"
        )

    # Reconstruct the exact queue payload shape your engine thread pool expects
    workload = {
        "exec_id": exec_id,
        "tracking_id": request.tracking_id, # Injected downstream for metric harvesting
        "scheduled_link_id": link_id,
        "src_node": request.source_node,
        "destination_node": request.destination_node,
        "start_time": request.start_time, # Preserves baseline timestamps
        "bandwidth": request.bandwidth,
        "data_amount": request.data_amount,
        "bandwidth_req_mbps": bandwidth_mbps,
        "data_amount_mb": data_amount_mb,
        "mode": "explicit_baseline",
        
        # Default predictions to zero since these aren't generated by the smart overlay
        "predicted_duration_s": 0.0,
        "predicted_throughput": 0.0,
        "predicted_energy": 0.0,
        "qos_prob": 1.0,
        "score": 0.0
    }

    # Safely commit to the operational thread pool queue
    enqueue_workload(workload)
    
    return {
        "status": "explicitly_queued",
        "exec_id": exec_id,
        "tracking_id": request.tracking_id,
        "assigned_link": link_id
    }


# This endpoint and class support the baseline framework
# Explicit class was used for evaluation and may be removed
class LegacyWorkloadRequest(BaseModel):
    destination_node: str
    bandwidth: str
    data_amount: str
    start_time: Optional[str] = None


@app.post("/execute-legacy")
async def execute_legacy(request: LegacyWorkloadRequest):
    workload = {
        "destination_node": request.destination_node,
        "bandwidth": request.bandwidth,
        "data_amount": request.data_amount,
        "start_time": (
            request.start_time
            or datetime.now(timezone.utc).isoformat()
        ),
        "exec_id": f"exec_{uuid.uuid4().hex}",
    }

    if not wm.get_link_name_from_dest(workload["destination_node"]):
        raise HTTPException(
            status_code=404,
            detail="Destination node not found",
        )

    enqueue_workload(workload)

    return {
        "status": "Queued",
        "exec_id": workload["exec_id"],
    }
    
# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)