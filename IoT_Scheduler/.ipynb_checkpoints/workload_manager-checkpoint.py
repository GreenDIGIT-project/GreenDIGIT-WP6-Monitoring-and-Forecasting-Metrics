import yaml
import threading
import time
from datetime import datetime, timedelta
import pytz
import json
import os
import random
from node_manager import (
    load_config,
    check_sta_association,
    kill_iperf,
    start_iperf_server,
    stop_iperf_server,
    start_iperf_client,
    parse_iperf_output,
    get_wlan_ip,
    parse_size_to_bytes,
    parse_bandwidth_to_mbps,
)

# ---------------------------------------------------------
# CREATE RESULTS DIRECTORY
# ---------------------------------------------------------
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# ---------------------------------------------------------
# LOAD CONFIGS
# ---------------------------------------------------------
nodes_config = load_config("../config/nodes.yml")
with open("../config/workloads.yml") as f:
    workloads_data = yaml.safe_load(f)

ssh_user = nodes_config.get("ssh_user")
key_path = nodes_config.get("ssh_key_path")
links = nodes_config.get("links")

# ---------------------------------------------------------
# TIMEZONE SETUP
# ---------------------------------------------------------
tz = pytz.timezone("Europe/Athens")

def get_link_name_from_dest(dest_node: str) -> str:
    """
    Return the link_name corresponding to the given destination node
    based on the loaded nodes_config['links'].
    Raises RuntimeError if the node is not found in any link.
    """
    for link_name, nodes in nodes_config['links'].items():
        if dest_node in nodes:
            return link_name
    raise RuntimeError(f"Destination node '{dest_node}' not found in any link.")
    
# ---------------------------------------------------------
# WORKLOAD HANDLER
# ---------------------------------------------------------
def run_workload(workload, link_name):
    dest_node = workload["destination_node"]
    bandwidth = workload["bandwidth"]
    data_amount = workload["data_amount"]

    exec_start_time = datetime.utcnow()  # Start timestamp

    # Get AP and STA for this link
    ap_node, sta_node = links[link_name]

    # Determine client/server nodes
    server_node = dest_node
    client_node = sta_node if server_node == ap_node else ap_node

    server_ip = get_wlan_ip(nodes_config["nodes"][server_node], ssh_user, key_path)
    client_ip = get_wlan_ip(nodes_config["nodes"][client_node], ssh_user, key_path)
    if not server_ip or not client_ip:
        # print("Could not detect wlan0 IPs. Skipping workload.")
        raise RuntimeError("Could not detect wlan0 IPs.")
        return

    # --- Always check the STA's connectivity ---
    ok, info = check_sta_association(nodes_config["nodes"][sta_node], ssh_user, key_path)
    if not ok:
        #print(f"Link not established for STA {sta_node} on {link_name}. Skipping workload.\nDetails: {info}")
        raise RuntimeError(f"STA not assiciated: {info}")
        return

    # Kill any existing iperf processes on both nodes
    kill_iperf(nodes_config["nodes"][server_node], ssh_user, key_path)
    kill_iperf(nodes_config["nodes"][client_node], ssh_user, key_path)

    # Start iperf server and capture PID
    server_pid = start_iperf_server(nodes_config["nodes"][server_node], ssh_user, key_path)

    # Start iperf client
    output = start_iperf_client(
        nodes_config["nodes"][client_node],
        ssh_user,
        key_path,
        server_ip,
        bandwidth,
        data_amount
    )

    # Parse results
    parsed = parse_iperf_output(output)
    exec_end_time = datetime.utcnow()

    # Stop iperf server after experiment
    stop_iperf_server(nodes_config["nodes"][server_node], ssh_user, key_path, server_pid)

    if parsed is None:
        # print(" Failed to parse iperf output.")
        raise RuntimeError("❌ Failed to parse iperf output.")
        return

    mbps, jitter, lost, total, loss_percent = parsed
    duration_s = (exec_end_time - exec_start_time).total_seconds()

    # Converting data_amount and bandwidth
    data_bytes = parse_size_to_bytes(data_amount)
    data_amount_mb = data_bytes / (1024 * 1024)

    # ---------------------------------------------------------
    # BUILD JSON RESULT
    # ---------------------------------------------------------
    #exec_unit_id = f"exec_{int(exec_start_time.timestamp())}"
    exec_unit_id = f"exec_{int(exec_start_time.timestamp())}_{random.randint(0, 1000):04d}"
    json_result = {
        "exec_unit_id": exec_unit_id,  # simple timestamp-based ID
        "src_node": client_node,
        "dst_node": server_node,
        "start_time": exec_start_time.isoformat() + "Z",
        "end_time": exec_end_time.isoformat() + "Z",
        "duration_s": duration_s,
        "data_amount_mb": data_amount_mb,
        "bandwidth_req_mbps": parse_bandwidth_to_mbps(bandwidth),
        "throughput_mbps": mbps,
        "jitter_ms": jitter,
        "packet_loss_percent": loss_percent
    }

    # ---------------------------------------------------------
    # SAVE JSON TO FILE
    # ---------------------------------------------------------
    #filename = os.path.join(RESULTS_DIR, f"workload_exec_{int(exec_start_time.timestamp())}.json")
    #with open(filename, "w") as f:
        #json.dump(json_result, f, indent=2)
    #print(f"Saved JSON to {filename}")

    # ---------------------------------------------------------
    # PRINT JSON FOR DEBUGGING
    # ---------------------------------------------------------
    #print("===== EXECUTION RESULT JSON =====")
    #print(json.dumps(json_result, indent=2))
    #print("=================================")

    #print(f"Workload {client_node} -> {server_node} completed.")

    return json_result 


# ---------------------------------------------------------
# QUEUE HANDLER FOR LINKS
# ---------------------------------------------------------
def handle_link_queue(link_name, workloads):
    for wl in workloads:
        start_time_str = wl["start_time"]
        try:
            start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # handle incorrect "24:xx" time
            if "24:" in start_time_str:
                start_time_str = start_time_str.replace("24:", "00:")
                start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                start_time += timedelta(days=1)
            else:
                raise

        start_time = tz.localize(start_time)
        now = datetime.now(tz)
        wait_seconds = (start_time - now).total_seconds()

        if wait_seconds > 0:
            print(f" Waiting {int(wait_seconds)}s to start workload for {wl['destination_node']}")
            time.sleep(wait_seconds)
        else:
            print(f"Start time already passed, running workload immediately for {wl['destination_node']}")

        run_workload(wl, link_name)


# ---------------------------------------------------------
# GROUP WORKLOADS PER LINK
# ---------------------------------------------------------
# Keep this here because the API needs to know which links exist
link_queues = {ln: [] for ln in links.keys()}

for wl in workloads_data["workloads"]:
    dest_node = wl["destination_node"]
    for link_name, nodes_pair in links.items():
        if dest_node in nodes_pair:
            link_queues[link_name].append(wl)
            break

# ---------------------------------------------------------
# START THREADS - ONLY RUN IF EXECUTED DIRECTLY
# ---------------------------------------------------------
if __name__ == "__main__":
    threads = []
    for link_name, wl_list in link_queues.items():
        print(f"Starting queue handler for {link_name} with {len(wl_list)} workloads.")
        if not wl_list:
            continue
        t = threading.Thread(target=handle_link_queue, args=(link_name, wl_list))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("All workloads across all links completed.")