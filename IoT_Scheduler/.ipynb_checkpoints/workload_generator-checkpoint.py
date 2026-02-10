import random
import time
import requests
from datetime import datetime, timedelta, timezone

API_URL = "http://localhost:8000/execute"

# Same link mapping as nodes.yaml
LINKS = {
    "link1": ["node01", "node05"],
    "link2": ["node03", "node06"],
    "link3": ["node02", "node04"],
    "link4": ["node07", "node08"],
    "link5": ["node09", "node12"],
    "link6": ["node10", "node11"],
}

BANDWIDTH = "2.4M"
MIN_DATA = 30   # MB
MAX_DATA = 250  # MB

DAYS_AHEAD = random.randint(6, 7)
MIN_PER_HOUR = 3
MAX_PER_HOUR = 5


def random_data():
    return f"{random.randint(MIN_DATA, MAX_DATA)}M"


def estimate_duration_s(data_mb: float, bw_mbps: float = 2.4):
    return int((data_mb * 8) / bw_mbps)


def submit(payload):
    print("Submitting:", payload)
    r = requests.post(API_URL, json=payload, timeout=10)
    try:
        print("Response:", r.json())
    except Exception:
        print("Response text:", r.text)


def main():
    now = datetime.now(timezone.utc)
    start_base = now + timedelta(minutes=1)

    total_sent = 0

    for day in range(DAYS_AHEAD):
        day_start = start_base + timedelta(days=day)

        for hour in range(24):
            hour_start = day_start.replace(
                hour=hour, minute=0, second=0, microsecond=0
            )

            for link, nodes in LINKS.items():
                num = random.randint(MIN_PER_HOUR, MAX_PER_HOUR)

                # spread workloads across the hour window
                cursor = hour_start

                for _ in range(num):
                    dest = random.choice(nodes)
                    data = random_data()
                    data_mb = float(data.replace("M", ""))

                    duration = estimate_duration_s(data_mb)
                    gap = random.randint(20, 60)

                    start_time = cursor

                    payload = {
                        "destination_node": dest,
                        "bandwidth": BANDWIDTH,
                        "data_amount": data,
                        "start_time": start_time.isoformat().replace("+00:00", "Z"),
                    }

                    submit(payload)
                    total_sent += 1

                    # move cursor so no overlap per link
                    cursor += timedelta(seconds=duration + gap)

                    # don’t overflow the hour
                    if cursor >= hour_start + timedelta(hours=1):
                        break

                    time.sleep(1.5)

    print(f"\n✅ Done. Total workloads submitted: {total_sent}")


if __name__ == "__main__":
    main()
