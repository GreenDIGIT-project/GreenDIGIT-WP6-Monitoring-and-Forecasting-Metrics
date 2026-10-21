import argparse
import asyncio
import os
import csv
from datetime import datetime, timezone
import httpx
from influxdb import InfluxDBClient
from experiment_factory import WorkloadFactory
from experiment_wrapper import ExperimentWrapper

# Core Datastore Configurations
INFLUX_HOST = "10.64.44.196"
INFLUX_PORT = 8086
INFLUX_USER = "ilbekas"
INFLUX_PASS = "!I[j~gtN25m{"
INFLUX_DB = "workloads_IoT"

async def wait_for_queue_to_clear(api_url: str):
    """Polls the execution manager until all link worker queues are clean and empty."""
    print("\n⏳ Workloads dispatched. Monitoring pipeline queue completion status...")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.get(f"{api_url}/monitor/links")
                if resp.status_code == 200:
                    state = resp.json()
                    
                    total_depth = sum(info.get("queue_depth", 0) for info in state.values())
                    active_workers = any(info.get("worker_alive", False) for info in state.values())
                    
                    if total_depth == 0 and not active_workers:
                        print("🏁 Link engine queues are reporting idle.")
                        break
                    else:
                        print(f"📊 Processing elements in system background... Remaining Queue Depth: {total_depth}")
            except Exception as e:
                print(f"⚠️ Monitoring check skipped: {e}")
            await asyncio.sleep(5)

async def wait_for_all_metrics_to_persist(client, query: str, expected_count: int):
    """
    Repeatedly queries InfluxDB until the database records match our exact 
    dispatched batch size. This loops infinitely to give long-running workloads 
    ample time to settle.
    """
    print(f"🕵️ Target verification initialized. Expecting {expected_count} workloads in InfluxDB...")
    attempt = 1
    
    while True:
        try:
            result_set = client.query(query)
            points = list(result_set.get_points())
            current_count = len(points)
            
            if current_count >= expected_count:
                print(f"🎉 Success! Database matches telemetry demands ({current_count}/{expected_count} records recovered).")
                return points
                
            print(f"⏳ [Check #{attempt}] Telemetry indexing in progress... Found {current_count}/{expected_count} items. Retrying in 5 seconds...")
        except Exception as e:
            print(f"⚠️ Query trial warning: {e}")
            
        attempt += 1
        await asyncio.sleep(5)

def harvest_and_save_run(mode: str, profile: str, flexibility: str, dispatched_jobs: list, points: list):
    """
    Processes the collected data points and writes them out into a flat file within results_final/.
    """
    os.makedirs("results_final", exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results_final/run__{mode}__{profile.replace(' ', '_')}__{flexibility}__{timestamp_str}.csv"
    
    job_map = {job["exec_id"]: job for job in dispatched_jobs if job.get("exec_id")}
    
    headers = [
        "tracking_id", "assigned_link", "src_node", "dst_node", "start_time",
        "requested_bandwidth", "requested_data_amount", "pred_duration_s", 
        "pred_energy_Wh", "qos_probability", "achieved_duration_s", 
        "achieved_data_mb", "achieved_throughput", "jitter_ms", 
        "packet_loss_percent", "throughput_efficiency", "real_energy_consumed", "carbon_emissions"
    ]
    
    rows_written = 0
    with open(filename, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for pt in points:
            exec_id = pt.get("exec_unit_id")
            if exec_id in job_map:
                local_context = job_map[exec_id]
                
                requested_bw = float(local_context.get("bandwidth_req_mbps") or 1.0)
                achieved_tp = float(pt.get("throughput_mbps") or pt.get("achieved_throughput_mbps") or 0.0)
                real_energy = float(pt.get("total_energy_Wh") or pt.get("total_energy") or 0.0)
                carbon_intensity = float(pt.get("carbon_intensity") or pt.get("carbon_intensity_mg_per_wh") or 50.0)
                
                # Fetching the execution start time string from Influx entry ('time' column)
                execution_start_time = pt.get("time") or "unknown"
                
                efficiency = achieved_tp / requested_bw if requested_bw > 0 else 0.0
                emissions = (real_energy / 1000.0) * carbon_intensity
                
                writer.writerow([
                    local_context.get("tracking_id"),
                    pt.get("assigned_link") or pt.get("scheduled_link_id") or "unknown",
                    pt.get("src_node"),
                    pt.get("dst_node"),
                    execution_start_time,  
                    requested_bw,
                    float(local_context.get("data_amount_mb") or 0.0),
                    float(local_context.get("predicted_duration_s") or 0.0),
                    float(local_context.get("predicted_energy") or 0.0),
                    float(local_context.get("qos_prob") or 1.0),
                    float(pt.get("duration_s") or pt.get("achieved_duration_s") or 0.0),
                    float(pt.get("data_amount_mb") or pt.get("achieved_data_amount_mb") or 0.0),
                    achieved_tp,
                    float(pt.get("jitter_ms") or 0.0),
                    float(pt.get("packet_loss_percent") or 0.0),
                    efficiency,       
                    real_energy,      
                    emissions
                ])
                rows_written += 1
                
    print(f"💾 [DATA EXPEDITION SUCCESS] File populated: {filename} ({rows_written}/{len(dispatched_jobs)} lines aggregated).")

async def main():
    parser = argparse.ArgumentParser(description="Run a single isolated workflow simulation scenario.")
    parser.add_argument("--mode", type=str, required=True, 
                        choices=["baseline_random", "baseline_balancer", "carbon", "balanced"])
    parser.add_argument("--profile", type=str, default="Random BW")
    parser.add_argument("--flexibility", type=str, default="Loose")
    
    args = parser.parse_args()
    anchor_time = datetime.now(timezone.utc)
    
    factory = WorkloadFactory(seed=42)
    wrapper = ExperimentWrapper(api_url="http://localhost:8000")
    
    print("\n" + "="*70)
    print(f"🧪 TESTING ISOLATED SCENARIO CONTEXT")
    print(f"📅 Baseline Time Set To: {anchor_time.isoformat()}")
    print("="*70)
    
    try:
        # 1. Pipeline execution and track dispatch returns
        dispatched_jobs = await wrapper.run_scenario(
            factory_instance=factory,
            profile=args.profile,
            flexibility=args.flexibility,
            mode=args.mode,
            anchor_time=anchor_time
        )
        
        # 2. Block execution until the server finishes writing real operational results
        await wait_for_queue_to_clear(wrapper.api_url)
        
        # Filter down and capture valid mappings
        valid_jobs = [job for job in dispatched_jobs if job.get("exec_id")]
        if not valid_jobs:
            print("❌ No valid tasks found to collect metadata from.")
            return

        # Initialize Influx client parameters early to handle live counting checks
        client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASS, database=INFLUX_DB)
        or_conditions = " OR ".join([f"exec_unit_id = '{job['exec_id']}'" for job in valid_jobs])
        query = f"SELECT * FROM workloads WHERE {or_conditions}"
        
        # 3. Indefinite dynamic verification loop block
        confirmed_points = await wait_for_all_metrics_to_persist(
            client=client, 
            query=query, 
            expected_count=len(valid_jobs)
        )
        
        # 4. Pull metrics from Influx and build the data file container
        harvest_and_save_run(args.mode, args.profile, args.flexibility, dispatched_jobs, confirmed_points)
        
        print("="*70 + "\n✅ Scenario submission and harvesting execution loops successfully completed.")
    except Exception as e:
        print(f"\n❌ Execution pipeline failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())