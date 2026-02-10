import random
import time
import requests
from datetime import datetime, timedelta, timezone

API_URL = "http://localhost:8000/execute"

# List of all available nodes
NODES = [f"node{i:02d}" for i in range(1, 13)]

# Configuration
TOTAL_WORKLOADS = 50
MIN_DATA = 7
MAX_DATA = 85
MIN_BW = 0.7
MAX_BW = 2.4

def random_bandwidth():
    bw = round(random.uniform(MIN_BW, MAX_BW), 1)
    return f"{bw}M"

def random_data():
    return f"{random.randint(MIN_DATA, MAX_DATA)}M"

def submit(payload):
    print(f"[SUBMIT NOW] Dest: {payload['destination_node']} | {payload['data_amount']} @ {payload['bandwidth']}")
    try:
        r = requests.post(API_URL, json=payload, timeout=10)
        try:
            print("Response:", r.json())
        except:
            print("Response Text:", r.text)
    except Exception as e:
        print(f"❌ Connection Error: {e}")

def main():
    # Capture the "Now" timestamp once
    start_now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    print(f"--- Generating {TOTAL_WORKLOADS} workloads starting IMMEDIATELY ---\n")

    for i in range(TOTAL_WORKLOADS):
        dest = random.choice(NODES)
        bw = random_bandwidth()
        data = random_data()

        payload = {
            "destination_node": dest,
            "bandwidth": bw,
            "data_amount": data,
            "start_time": start_now, # All tasks use the same current timestamp
        }

        submit(payload)
        
        # Keep a tiny delay so the OS/Network can handle the 150 POST requests
        time.sleep(1.5)

    print(f"\n✅ Finished. {TOTAL_WORKLOADS} workloads submitted with start_time: {start_now}")

if __name__ == "__main__":
    main()