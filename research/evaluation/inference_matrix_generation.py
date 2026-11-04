import itertools
import requests
import time
from datetime import datetime, timedelta

# Configuration
URL = "http://localhost:8000/execute"
DATA_AMOUNT = "30M"
BANDWIDTH = "2.4M"
INTERVAL_MINUTES = 20

LINK_MAP = {
    "link1": {"downlink": "node05", "uplink": "node01"},
    "link2": {"downlink": "node06", "uplink": "node03"},
    "link3": {"downlink": "node04", "uplink": "node02"},
    "link4": {"downlink": "node08", "uplink": "node07"},
    "link5": {"downlink": "node12", "uplink": "node09"},
    "link6": {"downlink": "node11", "uplink": "node10"},
}

def generate_unique_pairs():
    links = [f"link{i}" for i in range(1, 7)]
    directions = ["downlink", "uplink"]
    entities = [(link, direct) for link in links for direct in directions]
    
    unique_pairs = []
    for entity_a, entity_b in itertools.combinations(entities, 2):
        if entity_a[0] != entity_b[0]:
            unique_pairs.append((entity_a, entity_b))
    return unique_pairs

def send_request(link_id, direction, start_time):
    dst = LINK_MAP[link_id][direction]
    payload = {
        "destination_node": dst,
        "bandwidth": BANDWIDTH,
        "data_amount": DATA_AMOUNT,
        "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    # Print exactly what we are sending before we fire it
    print(f"   [SENDING] Target: {dst} | BW: {BANDWIDTH} | Data: {DATA_AMOUNT} | Time: {payload['start_time']}")
    
    try:
        response = requests.post(URL, json=payload, timeout=10)
        if response.status_code in [200, 201]:
            print(f"   [SUCCESS] API accepted {link_id} ({direction}). Response: {response.status_code}")
            return True
        else:
            print(f"   [API ERROR] Status {response.status_code} received! Details: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"   [CONNECTION FAILED] Could not reach API endpoint: {e}")
        return False

# Setup and Verification Prints
test_pairs = generate_unique_pairs()
total_pairs = len(test_pairs)
total_estimated_hours = (total_pairs * INTERVAL_MINUTES) / 60

print("=" * 60)
print("             INTERFERENCE MATRIX DATA COLLECTOR             ")
print("=" * 60)
print(f" Total Unique Combinations Generated: {total_pairs} (Expected: 60)")
print(f" Time interval between steps        : {INTERVAL_MINUTES} minutes")
print(f" Total experiment runtime           : {total_estimated_hours:.1f} hours")
print(f" Target API URL                     : {URL}")
print("=" * 60)
print("Starting execution loop now...\n")

# Main Real-Time Loop
for index, (entity_a, entity_b) in enumerate(test_pairs):
    link_a, dir_a = entity_a
    link_b, dir_b = entity_b
    
    # Capture the exact moment this couple is deployed
    now = datetime.utcnow()
    
    print(f"\n[SLOT {index+1:02d}/{total_pairs:02d}] —— Deploying Couple at {now.strftime('%H:%M:%S')} UTC")
    print(f" Pairs to map: (('{link_a}', '{dir_a}'), ('{link_b}', '{dir_b}'))")
    print("-" * 50)
    
    # Fire requests back-to-back
    success_a = send_request(link_a, dir_a, now)
    success_b = send_request(link_b, dir_b, now)
    
    print("-" * 50)
    if success_a and success_b:
        print(f" [STATUS] Slot {index+1} successfully deployed. Monitor your network logs now.")
    else:
        print(f" [WARNING] One or both requests encountered errors in Slot {index+1}!")
        
    # If it's not the last element, run the countdown delay
    if index < total_pairs - 1:
        next_run = now + timedelta(minutes=INTERVAL_MINUTES)
        print(f"\n [WAITING] Sleeping for {INTERVAL_MINUTES} minutes.")
        print(f"            Next pair will submit at: {next_run.strftime('%H:%M:%S')} UTC")
        
        # Simple countdown wrapper so you don't think the terminal crashed
        for remaining in range(INTERVAL_MINUTES, 0, -1):
            print(f"\rTime remaining in this collection cycle: {remaining:02d} minute(s)...", end="", flush=True)
            time.sleep(60)
        print("\rCollection cycle complete! Moving to next pair...                         \n")

print("\n" + "=" * 60)
print(" EXPERIMENT COMPLETE: All 60 combinations have been simulated! ")
print("=" * 60)