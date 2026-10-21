import random
import requests
import time
from datetime import datetime, timezone

# =====================================================
# CONFIGURATION
# =====================================================

API_URL = "http://localhost:8000/execute"

# Physical links (bidirectional)
LINKS = [
    ("node01", "node05"),
    ("node03", "node06"),
    ("node02", "node04"),
    ("node07", "node08"),
    ("node09", "node12"),
    ("node10", "node11"),
]

# Workload ranges
MIN_DATA = 40
MAX_DATA = 120

MIN_BW = 0.7
MAX_BW = 2.4

# =====================================================
# RANDOM HELPERS
# =====================================================

def random_bandwidth():
    bw = round(random.uniform(MIN_BW, MAX_BW), 2)
    return f"{bw}M"


def random_data():
    size = random.randint(MIN_DATA, MAX_DATA)
    return f"{size}M"


def current_time():
    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

# =====================================================
# RANDOM PHYSICAL FLOW
# =====================================================

def random_link():

    node_a, node_b = random.choice(LINKS)

    # randomize direction
    if random.random() < 0.5:
        return node_a, node_b
    else:
        return node_b, node_a

# =====================================================
# ARRIVAL PROCESS
# =====================================================

def random_sleep_time():

    r = random.random()

    # occasional idle gaps
    if r < 0.25:
        return random.uniform(70, 300)

    # most common regime
    elif r < 0.85:
        return random.uniform(30, 260)

    # occasional small burst
    else:
        return random.uniform(10, 50)

# =====================================================
# SUBMIT WORKLOAD
# =====================================================

def submit_workload():

    src, dst = random_link()

    payload = {
        "source_node": src,
        "destination_node": dst,
        "bandwidth": random_bandwidth(),
        "data_amount": random_data(),
        "start_time": current_time(),
    }

    try:

        response = requests.post(
            API_URL,
            json=payload,
            timeout=10
        )

        print(
            f"[OK] "
            f"{src} → {dst} | "
            f"{payload['data_amount']} @ {payload['bandwidth']}"
        )

    except Exception as e:

        print(
            f"[ERROR] "
            f"{src} → {dst} | {e}"
        )

# =====================================================
# MAIN LOOP
# =====================================================

def main():

    print("🔥 Simple stochastic workload generator started")

    try:

        while True:

            submit_workload()

            sleep_time = random_sleep_time()

            print(f"⏳ Sleeping for {sleep_time:.1f} seconds\n")

            time.sleep(sleep_time)

    except KeyboardInterrupt:

        print("\n🛑 Generator stopped cleanly")

# =====================================================

if __name__ == "__main__":
    main()