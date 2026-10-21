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
    ssh_exec,
)

# ---------------------------------------------------------
# CREATE RESULTS DIRECTORY
# ---------------------------------------------------------
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# ---------------------------------------------------------
# LOAD CONFIGS
# ---------------------------------------------------------
nodes_config = load_config("../../config/nodes.yml")
with open("../../config/workloads.yml") as f:
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

    # Convert early so it's available for BOTH success and failure
    data_bytes = parse_size_to_bytes(data_amount)
    data_amount_mb = data_bytes / (1024 * 1024)

    exec_start_time = datetime.utcnow()

    ap_node, sta_node = links[link_name]
    server_node = dest_node
    client_node = sta_node if server_node == ap_node else ap_node

    server_ip = get_wlan_ip(nodes_config["nodes"][server_node], ssh_user, key_path)
    client_ip = get_wlan_ip(nodes_config["nodes"][client_node], ssh_user, key_path)

    if not server_ip or not client_ip:
        raise RuntimeError("Could not detect wlan0 IPs.")

    ok, info = check_sta_association(nodes_config["nodes"][sta_node], ssh_user, key_path)
    if not ok:
        raise RuntimeError(f"STA not associated: {info}")

    # ---------------------------------------------------------------------
    # CONCURRENCY CHANGE: Dynamic port selection to prevent port reuse crashes
    # ---------------------------------------------------------------------
    unique_port = random.randint(5000, 6000)

    # ---------------------------------------------------------------------
    # CONCURRENCY CHANGE: Removed global kill_iperf(...) calls here.
    # Individual threads must never run broad pkill steps while other jobs run.
    # ---------------------------------------------------------------------

    # Start the iperf server on the specific host utilizing the custom allocated port
    server_host = nodes_config["nodes"][server_node]
    server_cmd = f"nohup iperf -s -u -p {unique_port} -i 1 > /dev/null 2>&1 & echo $!"
    code, out_srv, err = ssh_exec(server_host, ssh_user, key_path, server_cmd)
    
    try:
        server_pid = int(out_srv.strip())
        print(f"[{datetime.now()}] [{link_name}] Started concurrent iperf server on {server_node} (Port: {unique_port}, PID: {server_pid})")
    except ValueError:
        print(f"[{datetime.now()}] [{link_name}] Failed to parse PID for iperf server on {server_node}. Output: {out_srv}")
        server_pid = None

    # Run the iperf client targeting the corresponding dedicated port
    client_host = nodes_config["nodes"][client_node]
    client_cmd = f"iperf -c {server_ip} -u -p {unique_port} -b {bandwidth} -n {data_amount}"
    output = start_iperf_client(
        nodes_config["nodes"][client_node],
        ssh_user,
        key_path,
        server_ip,
        bandwidth,
        f"{data_amount} -p {unique_port}"  # Append port parameter dynamically safely
    )

    parsed = parse_iperf_output(output)
    exec_end_time = datetime.utcnow()

    # ---------------------------------------------------------------------
    # CONCURRENCY CHANGE: Explicitly kill only OUR server process via its PID
    # ---------------------------------------------------------------------
    if server_pid:
        stop_iperf_server(nodes_config["nodes"][server_node], ssh_user, key_path, server_pid)

    duration_s = (exec_end_time - exec_start_time).total_seconds()

    # ---------------------------------------------------------
    # FAILURE CASE (Parsing failed)
    # ---------------------------------------------------------
    if parsed is None:
        return {
            "exec_unit_id": workload["exec_id"],
            "src_node": client_node,
            "dst_node": server_node,
            "start_time": exec_start_time.isoformat() + "Z",
            "end_time": exec_end_time.isoformat() + "Z",
            "duration_s": duration_s,
            "data_amount_mb": data_amount_mb,
            "bandwidth_req_mbps": parse_bandwidth_to_mbps(bandwidth),
            "throughput_mbps": None,
            "jitter_ms": None,
            "packet_loss_percent": None,
            "execution_failed": True
        }

    # ---------------------------------------------------------
    # SUCCESS CASE
    # ---------------------------------------------------------
    mbps, jitter, lost, total, loss_percent = parsed

    return {
        "exec_unit_id": workload["exec_id"],
        "src_node": client_node,
        "dst_node": server_node,
        "start_time": exec_start_time.isoformat() + "Z",
        "end_time": exec_end_time.isoformat() + "Z",
        "duration_s": duration_s,
        "data_amount_mb": data_amount_mb,
        "bandwidth_req_mbps": parse_bandwidth_to_mbps(bandwidth),
        "throughput_mbps": mbps,
        "jitter_ms": jitter,
        "packet_loss_percent": loss_percent,
        "execution_failed": False
    }


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