import os
import sys
import json
import time
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import threading
import uvicorn
from influxdb import InfluxDBClient

# Use absolute paths to be safe within Docker
CIM_DIR = "/home/ilbekas/Workload Scheduler/CIM" 

if os.path.exists(CIM_DIR):
    if CIM_DIR not in sys.path:
        sys.path.insert(0, CIM_DIR)

try:
    import cim_submitter
    import workload_manager as wm
    import query_predictions as qp
except ImportError as e:
    print(f"CRITICAL: Resource import failed: {e}")
    sys.exit(1) # Exit so Docker knows the service failed

# Now you can safely import the CIM modules
import cim_submitter

import workload_manager as wm
import query_predictions as qp

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
                "duration_s": float(prediction.get("duration_s", 0)),
                "data_amount_mb": float(prediction.get("data_amount_mb", 0)),
                "bandwidth_req_mbps": float(prediction.get("bandwidth_req_mbps", 0)),
                "throughput_mbps": float(prediction.get("throughput_mbps", 0)),
                "jitter_ms": float(prediction.get("jitter_ms", 0)),
                "packet_loss_percent": float(prediction.get("packet_loss_percent", 0)),
                "total_tx_Wh": float(prediction.get("energy_results", {}).get("total_tx_Wh", 0)),
                "total_rx_Wh": float(prediction.get("energy_results", {}).get("total_rx_Wh", 0)),
                "total_energy_Wh": float(prediction.get("energy_results", {}).get("total_energy_Wh", 0)),
                "MB": float(prediction.get("energy_results", {}).get("MB", 0)),
            }
        }

        influx_client.write_points([point])
        print(f"Stored workload {prediction['exec_unit_id']} in InfluxDB.")

    except Exception as e:
        print(f"InfluxDB write failed: {e}")

app = FastAPI(title="Workload Energy API")

# -------------------------------
# API Request Model
# -------------------------------
class WorkloadRequest(BaseModel):
    destination_node: str
    bandwidth: str
    data_amount: str
    start_time: str = None  # optional ISO8601 string


# -------------------------------
# LINK QUEUE MANAGEMENT
# -------------------------------
link_queues = {link_name: [] for link_name in wm.links.keys()}


def run_link_queue(link_name: str):
    """
    Worker function to process queued workloads sequentially for a link.
    Ensures start times and tail wait are respected.
    """
    while link_queues[link_name]:
        workload = link_queues[link_name][0]  # peek first workload

        # Respect future start time
        start_time_str = workload.get("start_time")
        if start_time_str:
            start_dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            wait_s = (start_dt - now).total_seconds()
            if wait_s > 0:
                print(f"[Link {link_name}] Waiting {int(wait_s)}s for future start of {workload['destination_node']}...")
                time.sleep(wait_s)

        # Now it’s safe to run the workload
        try:
            print(f"[Link {link_name}] Running workload for {workload['destination_node']}")
            result = wm.run_workload(workload, link_name)
            if result is None:
                raise RuntimeError("run_workload returned None")

            # Tail wait for energy measurement
            tail_wait_s = 20
            print(f"[Link {link_name}] Waiting {tail_wait_s}s for tail power data...")
            time.sleep(tail_wait_s)

            # Analyze energy
            prediction = qp.analyze_workload(result)
            print("\n===== FINAL RESULT =====")
            print(json.dumps(prediction, indent=2))
            print("========================\n")

            # --- INJECT TO CIM SYSTEM ---
            print(f"[Link {link_name}] Submitting to CIM...")
            try:
                success = cim_submitter.submit_to_cim(prediction)
                if success:
                    print(f"✅ Success: Workload {prediction['exec_unit_id']} injected to CIM.")
                else:
                    print(f"❌ Failure: CIM injection failed for {prediction['exec_unit_id']}.")
            except Exception as cim_err:
                print(f"❌ CIM Submitter Error: {cim_err}")
                
            # --- STORE IN INFLUXDB ---
            push_prediction_to_influx(prediction)

        except Exception as e:
            print(f"[Link {link_name}] [ERROR] Workload failed: {e}")

        # Remove finished workload
        link_queues[link_name].pop(0)


def enqueue_workload(workload: dict):
    """
    Place workload into the correct link queue and start worker if idle.
    """
    link_name = wm.get_link_name_from_dest(workload["destination_node"])
    if link_name not in link_queues:
        raise RuntimeError(f"Unknown link for destination {workload['destination_node']}")

    link_queues[link_name].append(workload)

    # Start queue worker if first workload
    if len(link_queues[link_name]) == 1:
        threading.Thread(target=run_link_queue, args=(link_name,), daemon=True).start()


# -------------------------------
# API Endpoint
# -------------------------------
@app.post("/execute")
async def execute(request: WorkloadRequest):
    workload = {
        "destination_node": request.destination_node,
        "bandwidth": request.bandwidth,
        "data_amount": request.data_amount,
        "start_time": request.start_time or datetime.utcnow().isoformat() + "Z",
        "exec_id": f"exec_{int(datetime.utcnow().timestamp())}"
    }

    # Validate destination node
    if not wm.get_link_name_from_dest(workload["destination_node"]):
        raise HTTPException(status_code=404, detail="Destination node not found")

    enqueue_workload(workload)
    return {"status": "Queued", "exec_id": workload["exec_id"]}

# -------------------------------
# API Endpoint for checking health every 30sec
# -------------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
