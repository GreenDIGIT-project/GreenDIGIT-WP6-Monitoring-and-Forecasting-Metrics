import requests
import time
import random
import json
import os
import sys
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

from carbon_client import get_carbon_intensity

# -------------------------------
# API CONFIG
# -------------------------------
API_URL = "http://localhost:8000/execute"

# -------------------------------
# LINK MAPPING
# -------------------------------
LINKS = {
    "link1": ["node01", "node05"],
    "link2": ["node03", "node06"],
    "link3": ["node02", "node04"],
    "link4": ["node07", "node08"],
    #"link5": ["node09", "node12"],
    "link6": ["node10", "node11"],
}

BANDWIDTH = "2.4M"

# -------------------------------
# INFLUX CONFIG
# -------------------------------
INFLUX_HOST = "10.64.44.196"
INFLUX_PORT = 8086
INFLUX_USER = "ilbekas"
INFLUX_PASSWORD = "!I[j~gtN25m{"
INFLUX_DB = "workloads_IoT"

# -------------------------------
# OUTPUT
# -------------------------------
BASE_DIR = "results"
os.makedirs(BASE_DIR, exist_ok=True)

random.seed(42)

# -------------------------------
# INFLUX
# -------------------------------
def get_influx():
    return InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD,
        database=INFLUX_DB
    )

# -------------------------------
# HELPERS
# -------------------------------
def pick_links(n):
    return random.sample(list(LINKS.keys()), n)

def get_active_links():
    r = requests.get("http://localhost:8000/monitor/links")
    data = r.json()

    active = []

    for link, state in data.items():
        if state["current_phase"] != "idle":
            active.append(link)

    return active


def wait_for_capacity(max_active):
    while True:
        active = get_active_links()

        if len(active) < max_active:
            return

        time.sleep(5)

def generate_batch(now, links, size_range):
    workloads = []
    for l in links:
        node = random.choice(LINKS[l])
        size = random.randint(*size_range)

        workloads.append(generate_workload(node, size, now))
    return workloads


def generate_workloads(total):
    workloads = []

    for _ in range(total):
        link = random.choice(list(LINKS.keys()))
        node = random.choice(LINKS[link])
        size = random.randint(40, 85)

        workloads.append({
            "destination_node": node,
            "bandwidth": BANDWIDTH,
            "data_amount": f"{size}M",
            "start_time": datetime.utcnow().isoformat() + "Z"
        })

    return workloads
# -------------------------------
# EXPERIMENTS
# -------------------------------

def EXP1():

    print("\n===== EXP1: LOW CONCURRENCY =====")

    MAX_ACTIVE = 2
    workloads = generate_workloads(20)

    exec_ids = []

    for w in workloads:
        wait_for_capacity(MAX_ACTIVE)

        time.sleep(random.uniform(1, 3))
        
        eid = submit_workload(w)
        exec_ids.append(eid)

    return exec_ids

def EXP2():

    print("\n===== EXP2: MEDIUM CONCURRENCY =====")

    MAX_ACTIVE = 4
    workloads = generate_workloads(20)

    exec_ids = []

    for w in workloads:
        wait_for_capacity(MAX_ACTIVE)

        time.sleep(random.uniform(1, 3))
        
        eid = submit_workload(w)
        exec_ids.append(eid)

    return exec_ids


def EXP3():

    print("\n===== EXP3: HIGH CONCURRENCY =====")

    MAX_ACTIVE = 6
    workloads = generate_workloads(20)

    exec_ids = []

    for w in workloads:
        wait_for_capacity(MAX_ACTIVE)

        time.sleep(random.uniform(1, 3))
        
        eid = submit_workload(w)
        exec_ids.append(eid)

    return exec_ids


# -------------------------------
# SUBMIT
# -------------------------------
def submit_workload(w):
    try:
        r = requests.post(API_URL, json=w)
        
        if r.status_code != 200:
            print("\n[SUBMIT FAILED]")
            print("Status:", r.status_code)
            print("Response:", r.text)
            print("Payload:", w)
            return None

        eid = r.json().get("exec_id")

        if not eid:
            print("[ERROR] Missing exec_id in response:", r.json())
            return None

        print(f"[SUBMIT OK] {eid}")
        return eid

    except Exception as e:
        print("[SUBMIT ERROR]", e)
        print("Payload:", w)
        return None

# -------------------------------
# FETCH
# -------------------------------
def fetch_results(exec_ids):
    client = get_influx()
    results = {}

    print("\n[FETCH] Querying InfluxDB...")

    for eid in exec_ids:
        query = f'''
        SELECT * FROM workloads
        WHERE "exec_unit_id" = '{eid}'
        ORDER BY time DESC
        LIMIT 1
        '''

        points = list(client.query(query).get_points())

        if points:
            results[eid] = points[0]
            print(f"[OK] {eid}")
        else:
            print(f"[MISS] {eid}")

    return results


# -------------------------------
# CARBON
# -------------------------------
def compute_carbon(entry):
    try:
        start = datetime.fromisoformat(entry["time"].replace("Z", "+00:00"))
        duration = float(entry["duration_s"])
        end = start + timedelta(seconds=duration)

        ci = get_carbon_intensity(start, end)

        energy_kWh = float(entry["total_energy_Wh"]) / 1000.0
        carbon = energy_kWh * ci if ci else None

        return ci, carbon

    except Exception:
        return None, None


# -------------------------------
# RUNNER
# -------------------------------
def run_experiment(name):

    EXP_MAP = {
        "EXP1": EXP1,
        "EXP2": EXP2,
        "EXP3": EXP3,
    }

    if name not in EXP_MAP:
        print("Invalid experiment. Use EXP1–EXP3")
        return

    print(f"\n===== RUNNING {name} =====")

    exec_ids = EXP_MAP[name]()

    input("\nPress ENTER after execution completes...")

    results = fetch_results(exec_ids)

    final = []

    for eid, r in results.items():

        ci, carbon = compute_carbon(r)

        record = {
            "exec_id": eid,
            "destination_node": r.get("dst_node"),
            "data_amount_mb": r.get("data_amount_mb"),
            "throughput_mbps": r.get("throughput_mbps"),
            "packet_loss_percent": r.get("packet_loss_percent"),
            "jitter_ms": r.get("jitter_ms"),
            "duration_s": r.get("duration_s"),
            "total_energy_Wh": r.get("total_energy_Wh"),
            "carbon_intensity": ci,
            "carbon_g": carbon
        }

        final.append(record)

        print(json.dumps(record, indent=2))

    out_file = f"{BASE_DIR}/{name}_{datetime.utcnow().isoformat()}.json"

    with open(out_file, "w") as f:
        json.dump(final, f, indent=2)

    print(f"\n[SAVED] {out_file}")


# -------------------------------
# CLI
# -------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python evaluation_runner.py EXP1")
        sys.exit(1)

    run_experiment(sys.argv[1])