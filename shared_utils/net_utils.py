import fcntl
import json
import socket
import struct
import time
import requests # type: ignore
import threading
from .log_utils import info, success, warning, error 
from config import RASPBERRY_MAPPING

# === Global CPU model coefficients ===
CPU_COEF_A = 0.0
CPU_COEF_B = 0.0
P_OTHER = 0.0
SAMPLE_DURATION = 4

# === Load CPU power model coefficients ===
def load_cpu_model(json_path="../cpu-model/consumption-model/cpu_info.json"):
    global CPU_COEF_A, CPU_COEF_B, P_OTHER
    try:
        with open(json_path, "r") as f:
            model = json.load(f)
            CPU_COEF_A = model.get("a", 0.0)
            CPU_COEF_B = model.get("b", 0.0)
            P_OTHER = model.get("p_other", 0.0)
            info(f"CPU Model: ({CPU_COEF_A}, {CPU_COEF_B}) | P_other: {P_OTHER}")
    except Exception as e:
       error(f"Failed to load CPU model: {e}")

# === Estimate CPU power consumption ===
def estimate_cpu_power(cpu_usage):
    if cpu_usage < 0 or cpu_usage > 100:
        raise ValueError(f"Invalid CPU usage: {cpu_usage}")

    return (CPU_COEF_A * cpu_usage + CPU_COEF_B)
    
# === Estimate network power ===
def estimate_network_power(total_power, cpu_usage):
    cpu_power = estimate_cpu_power(cpu_usage)
    network_power = total_power - cpu_power - P_OTHER
    return max(0.0, round(network_power, 4))

# === Detect Shelly device generation ===
def detect_shelly_generation(ip):
    try:
        response = requests.get(f"http://{ip}/rpc/Switch.GetStatus?id=0", timeout=5)
        if response.status_code == 200 and 'apower' in response.json():
            success("✓ Detected Shelly Gen2")
            return "gen2"
    except:
        pass

    try:
        response = requests.get(f"http://{ip}/status", timeout=5)
        if response.status_code == 200 and 'meters' in response.json():
            success("✓ Detected Shelly Gen1")
            return "gen1"
    except:
        pass

    warning("✘ Could not detect Shelly generation.")
    return None

# === Get IP address of local interface ===
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ip_addr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )[20:24])
        return ip_addr
    except OSError:
        return None

# === Function to read CPU usage from /proc/stat ===
def get_cpu_utilization(sample_duration=3):
    def read_cpu_times():
        with open('/proc/stat', 'r') as f:
            fields = f.readline().strip().split()[1:]
            values = list(map(int, fields))
            idle = values[3] + values[4]  # idle + iowait
            total = sum(values)
            return idle, total

    idle1, total1 = read_cpu_times()
    time.sleep(sample_duration)
    idle2, total2 = read_cpu_times()

    idle_delta = idle2 - idle1
    total_delta = total2 - total1

    usage = 100.0 * (total_delta - idle_delta) / total_delta if total_delta > 0 else 0
    return round(usage, 2)

# === Get total power from Shelly ===
def get_power(ip, gen):
    try:
        if gen == "gen2":
            response = requests.get(f"http://{ip}/rpc/Switch.GetStatus?id=0", timeout=5)
            return response.json()['apower']
        elif gen == "gen1":
            response = requests.get(f"http://{ip}/status", timeout=5)
            return response.json()['meters'][0]['power']
    except:
        return None

# === Check network configuration and Shelly binding ===
def net_conf_check():
    my_ip = get_ip_address('eth0')
    info("\n--- Network Configuration Check ---")

    if not my_ip:
        error("✘ Could not determine IP address for interface 'eth0'.")
        return None, None, None

    success(f"✓ Interface: eth0")
    success(f"✓ Detected IP: {my_ip}")

    if my_ip in RASPBERRY_MAPPING:
        topic, shelly_ip = RASPBERRY_MAPPING[my_ip]
        gen = detect_shelly_generation(shelly_ip)
        if not gen:
            error("✘ Failed to detect Shelly generation.")
            exit(1)

        success("✓ Node Mapping Found:")
        info(f"   MQTT Topic     : {topic}")
        info(f"   Shelly IP      : {shelly_ip}")
        return topic, shelly_ip, gen
    else:
        warning("⚠ IP not found in known device mapping.")
        return None, None, None