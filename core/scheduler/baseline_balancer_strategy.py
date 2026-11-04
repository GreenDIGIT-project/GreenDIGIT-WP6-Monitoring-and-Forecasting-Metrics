import uuid
import random
from datetime import datetime, timedelta

class BaselineRandomStrategy:
    def __init__(self, valid_links=None):
        self.valid_links = valid_links or [
            ("node01", "node05"), ("node03", "node06"), ("node02", "node04"),
            ("node07", "node08"), ("node09", "node12"), ("node10", "node11")
        ]
        self.link_byte_accumulator = {f"{src}-{dst}": 0.0 for src, dst in self.valid_links}

    def reset_accumulator(self):
        for link_key in self.link_byte_accumulator:
            self.link_byte_accumulator[link_key] = 0.0

    def allocate(self, raw_workloads: list, current_sim_time: datetime) -> list:
        allocated_workloads = []

        for raw_job in raw_workloads:
            src, dst = random.choice(self.valid_links)
            link_key = f"{src}-{dst}"
            self.link_byte_accumulator[link_key] += raw_job.data_amount_mb

            max_delay_seconds = int((raw_job.deadline - current_sim_time).total_seconds())
            if max_delay_seconds > 0:
                random_offset = random.randint(0, max_delay_seconds)
                allocated_start_time = current_sim_time + timedelta(seconds=random_offset)
            else:
                allocated_start_time = current_sim_time

            workload_dict = {
                "source_node": src,
                "destination_node": dst,
                "bandwidth": raw_job.bandwidth_req_mbps,
                "data_amount": raw_job.data_amount_mb,
                "start_time": allocated_start_time.isoformat() + "Z",
                "exec_id": f"exec_{uuid.uuid4().hex}",
                "tracking_id": raw_job.exec_id  
            }
            allocated_workloads.append(workload_dict)

        return allocated_workloads


class BaselineBalancerStrategy:
    def __init__(self, valid_links=None):
        self.valid_links = valid_links or [
            ("node01", "node05"), ("node03", "node06"), ("node02", "node04"),
            ("node07", "node08"), ("node09", "node12"), ("node10", "node11")
        ]
        self.link_byte_accumulator = {f"{src}-{dst}": 0.0 for src, dst in self.valid_links}

    def reset_accumulator(self):
        for link_key in self.link_byte_accumulator:
            self.link_byte_accumulator[link_key] = 0.0

    def allocate(self, raw_workloads: list, current_sim_time: datetime) -> list:
        allocated_workloads = []

        # Step 1: Naive Temporal Pre-sorting (Earliest Deadline First)
        sorted_workloads = sorted(raw_workloads, key=lambda x: x.deadline)

        # Step 2: Compute uniform staggering step dimensions using a safe horizon fraction
        if sorted_workloads:
            min_deadline = min(job.deadline for job in sorted_workloads)
            total_horizon_s = (min_deadline - current_sim_time).total_seconds()
            safe_spread_window_s = max(0.0, total_horizon_s * 0.75)
        else:
            safe_spread_window_s = 0.0

        total_jobs = len(sorted_workloads)

        # Step 3: Run Cumulative Data-Balancing across the staggered time array
        for i, raw_job in enumerate(sorted_workloads):
            # Spatial Balance: Match with the single link currently handling the lowest total data load
            best_link_key = min(self.link_byte_accumulator.keys(), key=lambda k: self.link_byte_accumulator[k])
            src, dst = best_link_key.split("-")
            
            self.link_byte_accumulator[best_link_key] += raw_job.data_amount_mb

            # Naive Temporal Balance: Linearly stretch execution starts over your timeline window
            stagger_fraction = i / max(1, total_jobs - 1)
            assigned_delay_s = safe_spread_window_s * stagger_fraction
            allocated_start_time = current_sim_time + timedelta(seconds=assigned_delay_s)

            workload_dict = {
                "source_node": src,
                "destination_node": dst,
                "bandwidth": raw_job.bandwidth_req_mbps,
                "data_amount": raw_job.data_amount_mb,
                "start_time": allocated_start_time.isoformat() + "Z",
                "exec_id": f"exec_{uuid.uuid4().hex}",
                "tracking_id": raw_job.exec_id  
            }
            allocated_workloads.append(workload_dict)

        return allocated_workloads


# =====================================================================
# SYSTEM TEST AND DIAGNOSTIC EXECUTION RUNNER
# =====================================================================
if __name__ == "__main__":
    from experiment_factory import WorkloadFactory
    import json
    
    factory = WorkloadFactory(seed=42)
    anchor_time = datetime(2026, 6, 26, 12, 0, 0)
    full_2gb_batch = factory.generate_2gb_batch("Max BW", "Loose", anchor_time)
    
    # Run the strategies
    random_runner = BaselineRandomStrategy()
    _ = random_runner.allocate(full_2gb_batch, anchor_time)
    
    balancer_runner = BaselineBalancerStrategy()
    balancer_allocated_jobs = balancer_runner.allocate(full_2gb_batch, anchor_time)
    
    # Printing explicit workload samples from the Balancer Output Array
    print("\n=====================================================================")
    print("📋 BALANCER OUTPUT DICTIONARY SAMPLES (FIRST 3 ALLOCATIONS)")
    print("=====================================================================")
    for index, job_dict in enumerate(balancer_allocated_jobs[:3]):
        print(f"\n[SAMPLE JOB {index + 1}]")
        print(json.dumps(job_dict, indent=4))
    print("=====================================================================\n")

    # Keep the comparative diagnostics table to verify total accumulation limits
    print("=====================================================================")
    print("📊 LINK ACCUMULATION METRIC BREAKDOWN (TARGET: ~2048 MB BATCH)")
    print("=====================================================================")
    print(f"{'Active Link Channel':<20} | {'Random Allocator':<22} | {'Byte Load Balancer':<22}")
    print("-" * 71)
    
    for link in random_runner.link_byte_accumulator.keys():
        rnd_val = random_runner.link_byte_accumulator[link]
        bal_val = balancer_runner.link_byte_accumulator[link]
        print(f"{link:<20} | {f'{rnd_val:.2f} MB':<22} | {f'{bal_val:.2f} MB':<22}")
        
    print("-" * 71)
    print(f"{'SUM TOTAL':<20} | {f'{sum(random_runner.link_byte_accumulator.values()):.2f} MB':<22} | {f'{sum(balancer_runner.link_byte_accumulator.values()):.2f} MB':<22}")
    print("=====================================================================\n")