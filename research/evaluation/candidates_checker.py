from datetime import datetime, timezone, timedelta

from workload_schema import WorkloadRequest
from candidate_generator import generate_candidates


def test_candidate_generation():
    print("\n========== CANDIDATE GENERATION TEST ==========")

    # -------------------------------------------------
    # FIXED TIME (IMPORTANT for reproducibility)
    # -------------------------------------------------
    current_time = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)

    # -------------------------------------------------
    # CREATE WORKLOAD
    # -------------------------------------------------
    workload = WorkloadRequest(
        data_amount_mb=100,
        bandwidth_requirement_mbps=10,
        min_qos=0.8,
        mode="balanced"
    )

    # Optional: uncomment to test deadline behavior
    workload.deadline = current_time + timedelta(minutes=40)

    # -------------------------------------------------
    # LINKS
    # -------------------------------------------------
    links = ["link1", "link2", "link3", "link4", "link5", "link6"]

    # -------------------------------------------------
    # RUN GENERATION
    # -------------------------------------------------
    candidates = generate_candidates(workload, links, current_time)

    # -------------------------------------------------
    # BASIC OUTPUT INFO
    # -------------------------------------------------
    print(f"Mode: {workload.mode}")
    print(f"Current time: {current_time}")
    print(f"Deadline: {workload.deadline}")
    print(f"Total candidates generated: {len(candidates)}\n")

    print("First 5 candidates:")
    for c in candidates[:10]:
        print(c)


    # -------------------------------------------------
    # ASSERTIONS (CRITICAL FOR DEBUGGING)
    # -------------------------------------------------

    # 1. candidates exist
    assert len(candidates) > 0, "No candidates generated!"

    # 2. structure correctness
    for c in candidates:
        assert "link_id" in c
        assert "start_time" in c
        assert c["predicted_throughput"] is None

    # 3. time ordering sanity check
    times = [datetime.fromisoformat(c["start_time"]) for c in candidates]
    assert times == sorted(times), "Time slots are not sorted!"

    # 4. link coverage check (each time slot should have all links)
    unique_times = len(set(times))
    expected_min = unique_times * len(links)

    assert len(candidates) == expected_min, (
        f"Candidate count mismatch: got {len(candidates)}, expected {expected_min}"
    )

    print("\nALL TESTS PASSED ✅")


if __name__ == "__main__":
    test_candidate_generation()