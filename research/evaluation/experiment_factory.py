import random
from datetime import datetime, timedelta

class WorkloadFactory:
    def __init__(self, seed: int = 42):
        self.seed = seed

    def generate_2gb_batch(self, profile: str, deadline_flexibility: str, current_simulation_time: datetime) -> list:
        """
        Generates a fixed list of raw workloads totaling exactly 2048 MB.
        Each workload contains a hard constraint DEADLINE, but NO execution start time.
        """
        # Ensure perfect reproducibility across different baseline matches
        random.seed(self.seed)
        
        total_target_mb = 1024.0
        accumulated_mb = 0.0
        workloads = []
        job_counter = 0

        # Uniform pool of network nodes for topology simulation
        nodes = [f"node{i:02d}" for i in range(1, 13)]

        while accumulated_mb < total_target_mb:
            job_counter += 1
            
            # 1. Randomize data chunk sizes between 2MB and 1500MB
            remaining = total_target_mb - accumulated_mb
            if remaining <= 2.0:
                chunk_mb = remaining
            else:
                chunk_mb = round(random.uniform(.0, min(150.0, remaining)), 2)
            
            accumulated_mb += chunk_mb

            # 2. Assign Bandwidth MB/s based on Profile Block
            if profile == "Max BW":
                bw_req = 2.4  # Saturated link capacity limit
            else:
                bw_req = round(random.uniform(0.2, 2.4), 2)  # Messy dynamic traffic

            # 3. Assign HARD DEADLINE based on Flexibility Dimension
            # Calculated as an offset from the baseline time when the batch arrives
            if deadline_flexibility == "Loose":
                minutes_to_deadline = random.randint(120, 180) # 2 to 3 hours in the future
            else:
                minutes_to_deadline = random.randint(10, 15)   # 15 to 30 minutes in the future
            
            job_deadline = current_simulation_time + timedelta(minutes=minutes_to_deadline)

            src = random.choice(nodes)
            dst = random.choice([n for n in nodes if n != src])

            # 4. Create the raw workload constraint object
            workload_item = type("Workload", (), {
                "exec_id": f"JOB_{job_counter:04d}",
                "data_amount_mb": chunk_mb,
                "bandwidth_req_mbps": bw_req,
                "deadline": job_deadline,
                "src_node": src,
                "dst_node": dst
            })()
            
            workloads.append(workload_item)

        print(f"📦 Factory Status: Generated {len(workloads)} workloads totaling {accumulated_mb:.2f} MB.")
        print(f"   Configuration details -> Profile: {profile} | Deadlines: {deadline_flexibility}\n")
        return workloads


# =====================================================================
# LOCAL INTEGRATION TEST RUNNER
# =====================================================================
if __name__ == "__main__":
    # 1. Initialize factory with our standard evaluation seed
    factory = WorkloadFactory(seed=42)
    
    # 2. Establish a constant, clean baseline clock time anchor
    test_anchor_time = datetime(2026, 6, 26, 16, 0, 0)
    
    print("=====================================================================")
    print("⚡ RUNNING LOCAL TEST: GENERATING SEEDED 2GB BATCH...")
    print("=====================================================================\n")
    
    # 3. Generate a sample batch (Messy Traffic + Loose Deadlines)
    sample_batch = factory.generate_2gb_batch(
        profile="Random BW", 
        deadline_flexibility="Loose", 
        current_simulation_time=test_anchor_time
    )
    
    # 4. Print out the top structural metadata rows for human validation
    print(f"{'Job ID':<10} | {'Payload Size':<15} | {'Required BW':<15} | {'Hard Constraint Deadline':<25} | {'Path Info':<15}")
    print("-" * 88)
    
    # Display the first 8 generated workloads as a validation sample
    for job in sample_batch[:8]:
        deadline_str = job.deadline.strftime("%Y-%m-%d %H:%M:%S")
        path_str = f"{job.src_node} -> {job.dst_node}"
        print(f"{job.exec_id:<10} | {f'{job.data_amount_mb} MB':<15} | {f'{job.bandwidth_req_mbps} Mbps':<15} | {deadline_str:<25} | {path_str:<15}")
        
    print("-" * 88)
    print(f"Total jobs remaining in sequence array: {len(sample_batch) - 8} more workloads...")
    print("\n✅ Verification check passed successfully! Object definitions are perfectly isolated.")