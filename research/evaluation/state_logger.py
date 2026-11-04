import requests
import time
import csv
from datetime import datetime
import os
import sys

MONITOR_URL = "http://localhost:8000/monitor/links"

INTERVAL = 10  # seconds


# -------------------------------
# ARGUMENT-BASED OUTPUT FILE
# -------------------------------
def get_output_file(exp_name):
    os.makedirs("logs", exist_ok=True)
    return f"logs/link_activity_{exp_name}.csv"


# -------------------------------
# ACTIVE LINKS QUERY
# -------------------------------
def get_active_links():
    try:
        r = requests.get(MONITOR_URL)
        data = r.json()

        active = []

        for link, state in data.items():
            if state.get("current_phase") == "running_iperf":
                active.append(link)

        return active

    except Exception as e:
        print(f"[ERROR] {e}")
        return []


# -------------------------------
# CSV INITIALISATION
# -------------------------------
def init_csv(file_path):
    if not os.path.exists(file_path):
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "num_active_links",
                "active_links"
            ])


# -------------------------------
# LOGGER
# -------------------------------
def run_logger(exp_name):
    output_file = get_output_file(exp_name)

    print(f"[LOGGER STARTED]")
    print(f"[OUTPUT] {output_file}")

    init_csv(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f)

        while True:
            now = datetime.utcnow().isoformat() + "Z"
            active_links = get_active_links()

            writer.writerow([
                now,
                len(active_links),
                "|".join(active_links)
            ])

            f.flush()

            print(f"[{now}] active_links={active_links}")

            time.sleep(INTERVAL)


# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python logger.py EXP_NAME")
        sys.exit(1)

    run_logger(sys.argv[1])