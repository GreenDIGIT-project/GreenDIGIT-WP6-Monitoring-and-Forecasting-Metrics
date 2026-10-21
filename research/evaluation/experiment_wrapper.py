import httpx
import asyncio
from datetime import datetime

class ExperimentWrapper:
    def __init__(self, api_url: str = "http://localhost:8000"):
        """
        Coordinates execution scenarios between the Workload Factory and your API.
        """
        self.api_url = api_url

    def _generate_scenario_tracking_id(self, mode: str, profile: str, flexibility: str, job_id: str) -> str:
        """
        Encodes scenario metadata directly into the tracking ID string for analytics.
        """
        clean_bw = profile.replace(" ", "")
        return f"{mode}_{clean_bw}_{flexibility}_{job_id}"

    async def run_scenario(self, factory_instance, profile: str, flexibility: str, mode: str, anchor_time: datetime):
        """
        Executes an experimental scenario suite using identical dataset loads.
        Returns a structured list of metadata for all dispatched jobs.
        """
        raw_workloads = factory_instance.generate_2gb_batch(
            profile=profile, 
            deadline_flexibility=flexibility, 
            current_simulation_time=anchor_time
        )
        
        print(f"\n🚀 [SCENARIO RUN] Mode: {mode} | Profile: {profile} | Flexibility: {flexibility}")
        print(f"📦 Handling batch size: {len(raw_workloads)} distinct workloads.")

        # ---------------------------------------------------------------------
        # PATHWAY A: BASELINE EXECUTION
        # ---------------------------------------------------------------------
        if mode in ["baseline_random", "baseline_balancer"]:
            if mode == "baseline_random":
                from baseline_random_strategy import BaselineRandomStrategy
                strategy = BaselineRandomStrategy()
            else:
                from baseline_balancer_strategy import BaselineBalancerStrategy
                strategy = BaselineBalancerStrategy()
                strategy.reset_accumulator()

            allocated_jobs = strategy.allocate(raw_workloads, anchor_time)
            
            dispatched_jobs = []
            async with httpx.AsyncClient(timeout=httpx.Timeout(None)) as client:
                for idx, job in enumerate(allocated_jobs, start=1):
                    job_label = f"JOB_{idx:04d}"
                    rich_id = self._generate_scenario_tracking_id(mode, profile, flexibility, job_label)
                    
                    raw_time = job["start_time"]
                    time_str = raw_time.isoformat() if hasattr(raw_time, "isoformat") else str(raw_time)
                    if "+00:00" in time_str:
                        time_str = time_str.split("+00:00")[0] + "+00:00"
                    if "Z" in time_str:
                        time_str = time_str.replace("Z", "") + "+00:00"
                    
                    kbps_value = int(job['bandwidth'] * 1000)
                    kb_value = int(job['data_amount'] * 1000)

                    payload = {
                        "source_node": job["source_node"],
                        "destination_node": job["destination_node"],
                        "bandwidth": f"{kbps_value}K",       
                        "data_amount": f"{kb_value}K",
                        "start_time": time_str,  
                        "tracking_id": rich_id
                    }
                    
                    try:
                        resp = await client.post(f"{self.api_url}/execute-explicit", json=payload)
                        if resp.status_code == 200:
                            data = resp.json()
                            dispatched_jobs.append({
                                "exec_id": data.get("exec_id"),
                                "tracking_id": rich_id,
                                "bandwidth_req_mbps": job['bandwidth'],
                                "data_amount_mb": job['data_amount'],
                                "predicted_duration_s": 0.0,
                                "predicted_energy": 0.0,
                                "qos_prob": 1.0
                            })
                            print(f"✅ [{job_label}] Baseline Enqueued successfully.")
                        else:
                            print(f"⚠️ [{job_label}] Baseline rejected ({resp.status_code}): {resp.text}")
                    except Exception as e:
                        print(f"💥 [{job_label}] Baseline transmission crash: {e}")
                    
                    await asyncio.sleep(0.05)
                    
            print(f"📥 Done. Queued {len(dispatched_jobs)}/{len(allocated_jobs)} jobs via /execute-explicit.")
            return dispatched_jobs

        # ---------------------------------------------------------------------
        # PATHWAY B: SMART SCHEDULER EXECUTION
        # ---------------------------------------------------------------------
        elif mode in ["carbon", "balanced"]:
            return await self._execute_smart_sequentially(raw_workloads, profile, flexibility, mode)

        else:
            raise ValueError(f"Unknown system runtime scenario mode target: {mode}")

    async def _execute_smart_sequentially(self, raw_workloads: list, profile: str, flexibility: str, mode: str):
        """
        Feeds jobs to the live server endpoint sequentially with an open timeout window.
        """
        dispatched_jobs = []
        timeout_config = httpx.Timeout(None)
        
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            for idx, raw_job in enumerate(raw_workloads, start=1):
                job_label = f"JOB_{idx:04d}"
                rich_id = self._generate_scenario_tracking_id(mode, profile, flexibility, job_label)
                
                kbps_value = int(raw_job.bandwidth_req_mbps * 1000)
                kb_value = int(raw_job.data_amount_mb * 1000)

                if hasattr(raw_job.deadline, "isoformat"):
                    deadline_str = raw_job.deadline.isoformat()
                else:
                    deadline_str = str(raw_job.deadline)
                
                if "+00:00" in deadline_str:
                    deadline_str = deadline_str.replace("+00:00", "") + "Z"
                elif not deadline_str.endswith("Z"):
                    deadline_str += "Z"

                optimized_payload = {
                    "data_amount": f"{kb_value}K",
                    "bandwidth": f"{kbps_value}K",
                    "deadline": deadline_str,
                    "mode": mode,
                    "tracking_id": rich_id
                }
                
                try:
                    resp = await client.post(f"{self.api_url}/execute", json=optimized_payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        dispatched_jobs.append({
                            "exec_id": data.get("exec_id"),
                            "tracking_id": rich_id,
                            "bandwidth_req_mbps": raw_job.bandwidth_req_mbps,
                            "data_amount_mb": raw_job.data_amount_mb,
                            "predicted_duration_s": data.get("predicted_duration_s", 0.0),
                            "predicted_energy": data.get("predicted_energy", 0.0),
                            "qos_prob": data.get("qos_prob", 1.0)
                        })
                        print(f"✅ [{job_label}] Enqueued successfully.")
                    else:
                        print(f"⚠️ [{job_label}] Server rejected with {resp.status_code}: {resp.text}")
                        
                except httpx.HTTPError as net_err:
                    print(f"💥 [{job_label}] Network connection error: {net_err}")
                
                await asyncio.sleep(0.1)
                
        print(f"\n🧠 Smart Framework Suite complete. Successfully queued {len(dispatched_jobs)}/{len(raw_workloads)} jobs via /execute.")
        return dispatched_jobs