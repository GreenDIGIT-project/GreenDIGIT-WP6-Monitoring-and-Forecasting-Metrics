import uuid
import random
from datetime import datetime, timedelta

class BaselineRandomStrategy:
    def __init__(self, valid_links=None):
        # Explicitly defines the 6 active channel pairs in your topology
        self.valid_links = valid_links or [
            ("node01", "node05"), ("node03", "node06"), ("node02", "node04"),
            ("node07", "node08"), ("node09", "node12"), ("node10", "node11")
        ]
        # Track accumulator and workload counters across explicit routing paths for analytics
        self.link_byte_accumulator = {f"{src}-{dst}": 0.0 for src, dst in self.valid_links}
        self.link_workload_counter = {f"{src}-{dst}": 0 for src, dst in self.valid_links}

    def reset_accumulator(self):
        for link_key in self.link_byte_accumulator:
            self.link_byte_accumulator[link_key] = 0.0
            self.link_workload_counter[link_key] = 0

    def allocate(self, raw_workloads: list, current_sim_time: datetime) -> list:
        """
        Transforms raw factory workloads into random link and time allocations.
        First picks a link pair, then randomizes direction (source vs destination node).
        """
        allocated_workloads = []

        for raw_job in raw_workloads:
            # 1. Randomly select an available link pair channel asset
            chosen_link = random.choice(self.valid_links)
            
            # 2. Flip a coin to determine which node is the source vs destination
            if random.choice([True, False]):
                src, dst = chosen_link[0], chosen_link[1]
            else:
                src, dst = chosen_link[1], chosen_link[0]
            
            # Record data volume and increment workload counts under the topology channel tracker
            link_key = f"{chosen_link[0]}-{chosen_link[1]}"
            self.link_byte_accumulator[link_key] += raw_job.data_amount_mb
            self.link_workload_counter[link_key] += 1

            # 3. Calculate the maximum delay allowed before breaching the deadline
            max_delay_seconds = int((raw_job.deadline - current_sim_time).total_seconds())
            
            if max_delay_seconds > 0:
                random_offset = random.randint(0, max_delay_seconds)
                allocated_start_time = current_sim_time + timedelta(seconds=random_offset)
            else:
                allocated_start_time = current_sim_time

            # 4. Construct the explicit payload dictionary signature
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
# LOCAL VERIFICATION RUNNER
# =====================================================================
if __name__ == "__main__":
    from experiment_factory import WorkloadFactory
    
    factory = WorkloadFactory(seed=42)
    anchor_time = datetime(2026, 6, 26, 12, 0, 0)
    raw_jobs = factory.generate_2gb_batch("Random BW", "Loose", anchor_time)
    
    strategy = BaselineRandomStrategy()
    results = strategy.allocate(raw_jobs, anchor_time)
    
    print("\n--- NEW LINK-FIRST RANDOM STRATEGY OUTPUT SAMPLE (FIRST 3) ---")
    for res in results[:3]:
        print(res)
        
    print("\n=====================================================================")
    print("📊 RANDOM ALLOCATOR LINK METRIC SUMMARY (FULL BATCH)")
    print("=====================================================================")
    print(f"{'Active Link Channel':<20} | {'Accumulated Volume':<20} | {'Assigned Workloads':<15}")
    print("-" * 64)
    for link in strategy.link_byte_accumulator.keys():
        print(f"{link:<20} | {strategy.link_byte_accumulator[link]:.2f} MB{'' :<13} | {strategy.link_workload_counter[link]} jobs")
    print("-" * 64)
    print(f"{'SUM TOTAL':<20} | {sum(strategy.link_byte_accumulator.values()):.2f} MB{'' :<13} | {sum(strategy.link_workload_counter.values())} jobs")
    print("=====================================================================\n")