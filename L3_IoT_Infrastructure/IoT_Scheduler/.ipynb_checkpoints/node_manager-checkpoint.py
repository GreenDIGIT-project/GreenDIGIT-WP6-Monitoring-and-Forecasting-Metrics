import paramiko
from datetime import datetime
import re
import os
import yaml
import uuid

# ---------------------------------------------------------
# LOAD CONFIG FROM YAML
# ---------------------------------------------------------
def load_config(path="../config/nodes.yml"):
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config


# ---------------------------------------------------------
# SSH EXECUTION HELPER
# ---------------------------------------------------------
def ssh_exec(host, username, key_path, cmd="hostname", timeout=15):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=username, key_filename=key_path, timeout=timeout)
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    code = stdout.channel.recv_exit_status()
    client.close()
    return code, out, err


# ---------------------------------------------------------
# TIME CHECK FUNCTION
# ---------------------------------------------------------
def is_time_ready(start_time):
    now = datetime.now()
    return now >= start_time


# ---------------------------------------------------------
# CHECK IF A PROCESS IS RUNNING (E.G., IPERF)
# ---------------------------------------------------------
def is_experiment_running(host, user, key_path, process_name="iperf"):
    cmd = f"pgrep -fl {process_name}"
    code, out, err = ssh_exec(host, user, key_path, cmd)
    return code == 0 and bool(out.strip())


# ---------------------------------------------------------
# KILL IPERF PROCESSES
# ---------------------------------------------------------
def kill_iperf(host, user, key_path):
    cmd_check = "pgrep -fl iperf"
    code, out, err = ssh_exec(host, user, key_path, cmd_check)
    processes = [line for line in out.strip().split("\n") if line.strip()]
    if processes:
        ssh_exec(host, user, key_path, "pkill -f iperf")
        print(f"Found and killed existing iperf processes on {host}")
        return True
    return False


# ---------------------------------------------------------
# CHECK STA CONNECTIVITY
# ---------------------------------------------------------
def check_sta_association(sta_host, user, key_path):
    cmd = "/usr/sbin/iw dev wlan0 link"
    code, out, err = ssh_exec(sta_host, user, key_path, cmd)
    if err.strip() and "command not found" in err:
        return False, "ERROR: iw not found on STA"
    if "Not connected" in out:
        return False, "STA not connected"
    if "Connected to" in out:
        return True, out
    return False, out


# ---------------------------------------------------------
# RESOURCE AVAILABILITY CHECK
# ---------------------------------------------------------
def can_start_workload(host, user, key_path, start_time, process_name="iperf"):
    if not is_time_ready(start_time):
        return False, "Start time not reached"
    if is_experiment_running(host, user, key_path, process_name):
        return False, f"{process_name} is already running"
    return True, "Node available"


# ---------------------------------------------------------
# IPERF SERVER 
# ---------------------------------------------------------
def start_iperf_server(host, user, key_path):
    """
    Starts iperf server in background and returns its PID.
    """
    cmd = "nohup iperf -s -u > /dev/null 2>&1 & echo $!"
    code, out, err = ssh_exec(host, user, key_path, cmd)
    try:
        pid = int(out.strip())
        print(f"Started iperf server on {host} with PID {pid}")
        return pid
    except ValueError:
        print(f"Failed to start iperf server on {host}")
        return None


# ---------------------------------------------------------
# STOP IPERF SERVER
# ---------------------------------------------------------
def stop_iperf_server(host, user, key_path, pid):
    """
    Stops iperf server given a PID
    """
    if pid is None:
        return
    cmd = f"kill -9 {pid}"
    ssh_exec(host, user, key_path, cmd)
    print(f"Stopped iperf server on {host} (PID {pid})")


# ---------------------------------------------------------
# IPERF CLIENT 
# ---------------------------------------------------------
def start_iperf_client(host, user, key_path, server_ip, bandwidth, data_amount):
    cmd = f"iperf -c {server_ip} -u -b {bandwidth} -n {data_amount}"
    code, out, err = ssh_exec(host, user, key_path, cmd)
    return out


# ---------------------------------------------------------
# SIZE PARSER (e.g. "20M", "50k", "1G") -> bytes
# ---------------------------------------------------------
def parse_size_to_bytes(size_str):
    """
    Convert size strings like:
      "200K", "5M", "1G", "1024", "200 k", "200KB"
    into integer bytes.


    - Case-insensitive
    - Whitespace tolerant
    - Supports K, M, G (binary: 1024-based)
    - If no unit → assume bytes
    """

    if size_str is None:
        return 0

    if isinstance(size_str, (int, float)):
        return int(size_str)

    s = str(size_str).strip().lower()
    if not s:
        return 0

    # Regex: number + optional unit
    match = re.match(r"([\d\.]+)\s*([kmg]?)b?$", s)
    if not match:
        raise ValueError(f"Invalid size string: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    multipliers = {
        "": 1,
        "k": 1024,
        "m": 1024 ** 2,
        "g": 1024 ** 3,
    }

    return int(value * multipliers[unit])

def parse_iperf_output(output):
    """
    Parse iperf UDP output and return:
    (throughput_mbps, jitter_ms, lost_packets, total_packets, loss_percent)

    Supports K/M/G bits/sec and arbitrary whitespace.
    """
    try:
        lines = output.splitlines()

        # Keep only actual UDP data rows (not headers)
        candidate_lines = [
            l for l in lines
            if "bits/sec" in l and re.search(r"\d+\s*/\s*\d+\s*\(", l)
        ]

        if not candidate_lines:
            print("DEBUG: No UDP summary lines found")
            return None

        # Use LAST line → final aggregate
        line = candidate_lines[-1]

        pattern = re.compile(
            r"""
            ([\d\.]+)          # throughput value
            \s*
            ([KMG])bits/sec    # unit
            \s+
            ([\d\.]+)          # jitter
            \s*ms
            \s+
            (\d+)              # lost
            \s*/\s*
            (\d+)              # total
            \s*
            \(
            ([\d\.]+)          # loss %
            %
            \)
            """,
            re.IGNORECASE | re.VERBOSE,
        )

        m = pattern.search(line)
        if not m:
            print(f"DEBUG: Regex failed on line: {line}")
            return None

        value, unit, jitter, lost, total, loss = m.groups()

        value = float(value)
        unit = unit.upper()

        # Normalize to Mbps
        if unit == "K":
            throughput_mbps = value / 1000.0
        elif unit == "M":
            throughput_mbps = value
        elif unit == "G":
            throughput_mbps = value * 1000.0
        else:
            throughput_mbps = 0.0

        return (
            throughput_mbps,
            float(jitter),
            int(lost),
            int(total),
            float(loss),
        )

    except Exception as e:
        print("Error parsing iperf output:", e)
        return None



# ---------------------------------------------------------
# GET LINK NODES BASED ON DESTINATION
# ---------------------------------------------------------
def get_link_nodes(config, dest_node):
    for link_name, nodes in config["links"].items():
        if dest_node in nodes:
            return nodes[0], nodes[1]  # AP, STA
    return None, None


# ---------------------------------------------------------
# GET WLAN IP
# ---------------------------------------------------------
def get_wlan_ip(host, user, key_path):
    cmd = "ip addr show wlan0 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1"
    try:
        code, out, err = ssh_exec(host, user, key_path, cmd)
        if code == 0 and out.strip():
            return out.strip()
        else:
            print(f"Warning: could not determine wlan0 IP for {host}. Error: {err}")
            return None
    except Exception as e:
        print(f"Warning: could not determine wlan0 IP for {host}. Exception: {e}")
        return None




def parse_bandwidth_to_mbps(bandwidth):
    """
    Convert iperf bandwidth strings to Mbps.

    Examples:
      "2.4M" -> 2.4
      "200K" -> 0.2
      "1G"   -> 1000.0
      "2.4"  -> 2.4 (assume Mbps)
    """
    if isinstance(bandwidth, (int, float)):
        return float(bandwidth)

    s = str(bandwidth).strip().lower()

    match = re.match(r"([\d\.]+)\s*([kmg]?)$", s)
    if not match:
        raise ValueError(f"Invalid bandwidth format: {bandwidth}")

    value = float(match.group(1))
    unit = match.group(2)

    if unit == "k":
        return value / 1000.0
    elif unit == "m" or unit == "":
        return value
    elif unit == "g":
        return value * 1000.0

