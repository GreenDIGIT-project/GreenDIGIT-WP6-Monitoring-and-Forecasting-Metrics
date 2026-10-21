import time
import json
import requests
import paho.mqtt.client as mqtt
import psutil
import socket
import fcntl
import struct
import random
import os
import subprocess
import re
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from shared_utils.cpu_utils import get_avg_cpu_freq_khz as get_current_freq
from shared_utils.extract import (
    get_interface_bytes as get_network_bytes,
    get_rssi_from_iw,
    get_tx_power_conf_from_nrc,
    get_last_mcs,
    get_wifi_role)
from shared_utils import tx_rx_consumption as estimate_energy

HERMIS_CONFIG_PATH = '/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/cpu/consumption-model/cpu_info.json'
hermis_a = None
hermis_b = None
hermis_p_other = None

NRC_CLI_DIR = "/home/pi/nrc_pkg/script"
NRC_CLI_BIN = "./cli_app"


# Mapping based on your image
raspberry_mapping = {
    "10.64.44.70":  ("power/node01", "10.64.44.221"),
    "10.64.44.71":  ("power/node02", "10.64.44.222"),
    "10.64.44.72":  ("power/node03", "10.64.44.223"),
    "10.64.44.73":  ("power/node04", "10.64.44.224"),
    "10.64.44.74":  ("power/node05", "10.64.44.225"),
    "10.64.44.75":  ("power/node06", "10.64.44.226"),
    "10.64.44.190": ("power/node07", "10.64.44.227"),
    "10.64.44.191": ("power/node08", "10.64.44.228"),
    "10.64.44.192": ("power/node09", "10.64.44.229"),
    "10.64.44.193": ("power/node10", "10.64.44.230"),
    "10.64.44.194": ("power/node11", "10.64.44.231"),
    "10.64.44.195": ("power/node12", "10.64.44.232"),
    "10.64.44.196": ("power/jetson_orin", "10.64.44.220"),  # Special Jetson device
}

JETSON_IP = '10.64.44.196'     # 🛠️ Put your Jetson Orin IP here
SHELLY_IP = '-'      # 🛠️ Put your Shelly Plug IP here
MQTT_TOPIC = '-'              # 🛠️ Topic for this node

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected successfully.")
    else:
        print(f"MQTT connection failed with code {rc}.")

def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected with code {rc}. Trying to reconnect...")
    while True:
        try:
            client.reconnect()
            print("Reconnected to MQTT broker.")
            break
        except Exception as e:
            print(f"Reconnect failed: {e}")
            time.sleep(5)


mqtt_client = mqtt.Client()
mqtt_client.connect_async(JETSON_IP, 1883, 60)
mqtt_client.loop_start()

def push_heartbeat():
    #hostname = socket.gethostname()
    metrics = f'heartbeat{{node="{MQTT_TOPIC}"}} 1\n'

    try:
        res = requests.post(f"http://{JETSON_IP}:9091/metrics/job/power_node", data=metrics)
        print(f"Heartbeat pushed: {res.status_code}")
    except Exception as e:
        print(f"Failed to push heartbeat: {e}")

def detect_shelly_generation():
    """Detect if Shelly device is Gen1 or Gen2 based on API response."""
    try:
        # Try Gen2 API first
        response = requests.get(f'http://{SHELLY_IP}/rpc/Switch.GetStatus?id=0', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if 'apower' in data:
                print("Detected Shelly Gen2 (Plus/Pro)")
                return "gen2"
    except Exception as e:
        print(f"Gen2 detection failed: {e}")

    try:
        # Try Gen1 API
        response = requests.get(f'http://{SHELLY_IP}/status', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if 'meters' in data:
                print("Detected Shelly Gen1")
                return "gen1"
    except Exception as e:
        print(f"Gen1 detection failed: {e}")

    print("Could not detect Shelly generation.")
    return None

def get_power_gen1(timestamp):
    """Get power measurement for Gen1 Shelly."""
    try:
        response = requests.get(f'http://{SHELLY_IP}/status', timeout=5)
        data = response.json()
        power = data['meters'][0]['power']  # in Watts
        voltage = data.get('emeters', [{}])[0].get('voltage', None)  # voltage may or may not exist
        return {
            'power': power,
            'voltage': voltage,
            'timestamp': timestamp
        }
    except Exception as e:
        print(f"Error fetching Gen1 Shelly data: {e}")
        return None

def get_power_gen2(timestamp):
    """Get power measurement for Gen2 Shelly (Plus/Pro)."""
    try:
        response = requests.get(f'http://{SHELLY_IP}/rpc/Switch.GetStatus?id=0', timeout=5)
        data = response.json()
        return {
            'power': data['apower'],      # active power in Watts
            'voltage': data['voltage'],    # voltage in Volts
            'timestamp': timestamp
        }
    except Exception as e:
        print(f"Error fetching Gen2 Shelly data: {e}")
        return None

def get_cpu_usage():
    """Get current CPU usage percentage."""
    # Αλλαγή σε interval=None (Non-blocking) για να μην καθυστερεί το script
    return psutil.cpu_percent(interval=None)

def get_ip_address(ifname):
    """Get IP address assigned to a specific network interface."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ip_addr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )[20:24])
        return ip_addr
    except OSError:
        return None

def load_hermis_params(config_path=HERMIS_CONFIG_PATH, topic=None, ip=None):
    """
    Load linear model params (a, b, p_other) from cpu_info.json.
    """
    try:
        with open(config_path, 'r') as f:
            cfg = json.load(f)

        # Flat form
        if isinstance(cfg, dict) and 'a' in cfg and 'b' in cfg:
            return float(cfg['a']), float(cfg['b']), float(cfg.get('p_other', 0.0))

        # Prefer topic, then IP, then default
        for key in (topic, ip, 'default', 'defaults'):
            if key and key in cfg and isinstance(cfg[key], dict) and 'a' in cfg[key] and 'b' in cfg[key]:
                return (
                    float(cfg[key]['a']),
                    float(cfg[key]['b']),
                    float(cfg[key].get('p_other', 0.0))
                )

        print(f"HERMIS params not found in {config_path}.")
    except Exception as e:
        print(f"Failed to load HERMIS params from {config_path}: {e}")
    return None, None, None

def get_cumulative_mcs_stats(cli_bin, cli_dir):
    """
    Parses 'show mac tx stats' to get cumulative bytes sent per MCS index.
    Optimized to bypass bash execution overhead and avoid shell=True.
    """
    try:
        # Direct execution of binary with custom Working Directory (cwd)
        # and injected input to avoid spawning '/bin/sh' and 'echo'
        # Προσθήκη του 'nice -n 19' για ελάχιστη δυνατή χρήση της CPU
        result = subprocess.run(
            ['nice', '-n', '19', cli_bin],
            input="show mac tx stats\n",
            capture_output=True,
            text=True,
            timeout=5,
            cwd=cli_dir
        )
        
        stats = {}
        pattern = re.compile(r'-\s*MCS\[\s*(\d+)\]\s*:\s*OK\(\s*\d+/\s*(\d+)\)')
        
        for line in result.stdout.splitlines():
            match = pattern.search(line)
            if match:
                mcs_idx = int(match.group(1))
                bytes_ok = int(match.group(2))
                stats[mcs_idx] = bytes_ok
        return stats
    except Exception as e:
        print(f"Error getting cumulative MCS stats: {e}")
        return {}


# ---- Main Loop ----
my_ip = get_ip_address('eth0')
    
if not my_ip:
    print("Could not find IP for eth0 interface.")

print(f"My eth0 IP address: {my_ip}")

if my_ip in raspberry_mapping:
    MQTT_TOPIC, SHELLY_IP = raspberry_mapping[my_ip]
    print(f"Node name: {MQTT_TOPIC}")
    print(f"Smart Meter IP: {SHELLY_IP}")
else:
    print("IP address not recognized in mapping.")

hermis_a, hermis_b, hermis_p_other = load_hermis_params(topic=MQTT_TOPIC, ip=my_ip)
if hermis_a is not None and hermis_b is not None:
    print(f"HERMIS params loaded: a={hermis_a}, b={hermis_b}, p_other={hermis_p_other}")
else:
    print("HERMIS params not available; estimation will be None.")

shelly_gen = detect_shelly_generation()
print(shelly_gen)

if not shelly_gen:
    print("Exiting: Shelly device not detected or unsupported.")
    exit(1)

prev_rx_bytes, prev_tx_bytes = get_network_bytes('wlan0')
prev_time = time.time()

# 🛠️ Αρχικοποίηση (Priming) του CPU monitor πριν το κεντρικό loop
psutil.cpu_percent(interval=None)

# 🛠️ Initialize previous MCS stats before the loop begins
prev_mcs_stats = get_cumulative_mcs_stats(NRC_CLI_BIN, NRC_CLI_DIR)

# 🛠️ OPTIMIZATION: Get Wi-Fi role ONCE outside the loop since it is static
wifi_role = get_wifi_role('wlan0')

# 🛠️ Counters for throttling heavy configuration lookups
loop_counter = 0
tx_power_cached = None

while True:
    common_timestamp = time.time()  # Capture ONE timestamp per loop iteration

    if shelly_gen == "gen1":
        measurement = get_power_gen1(common_timestamp)
    elif shelly_gen == "gen2":
        measurement = get_power_gen2(common_timestamp)
    else:
        print("Unknown Shelly generation.")
        break

    if measurement:
        # Calculate bitrate
        curr_rx_bytes, curr_tx_bytes = get_network_bytes('wlan0')
        curr_time = time.time()

        if prev_rx_bytes is not None and curr_rx_bytes is not None:
            time_diff = curr_time - prev_time
            rx_bitrate = (curr_rx_bytes - prev_rx_bytes) * 8 / time_diff / 1_000_000  # Mbps
            tx_bitrate = (curr_tx_bytes - prev_tx_bytes) * 8 / time_diff / 1_000_000  # Mbps
            J_per_bit, netmW, scenario = estimate_energy.monitor_once(tx_bitrate, rx_bitrate)
        else:
            rx_bitrate = None
            tx_bitrate = None

        # Update previous values
        prev_rx_bytes, prev_tx_bytes = curr_rx_bytes, curr_tx_bytes
        prev_time = curr_time

        # Get avg freq of all CPUs
        mean_freq_vals = get_current_freq(8, 0.05)
        if mean_freq_vals:
            print(f"Average frequencies of CPUs is {mean_freq_vals}")
        else:
            print(f"Didn't read frequency values")
            
        # Add CPU usage and P_cpu estimation
        measurement['cpu_freq'] = mean_freq_vals / 1000

        # Add CPU usage and P_cpu estimation
        cpu = get_cpu_usage()
        measurement['cpu_usage'] = cpu  # percent (0–100)

        # HERMIS power estimate: pcpu = a * cpu(%) + b
        if hermis_a is not None and hermis_b is not None:
            pcpu = hermis_a * cpu + hermis_b           # CPU-only power (W)
            measurement['HERMIS_power_cpu_estimation'] = round(pcpu, 3)
        else:
            measurement['HERMIS_power_cpu_estimation'] = None

        measurement['timestamp'] = common_timestamp  # Re-assign timestamp just to be sure

        # Add bitrate measurements
        measurement['rx_bitrate_mbps'] = round(rx_bitrate, 3) if rx_bitrate is not None else None
        measurement['tx_bitrate_mbps'] = round(tx_bitrate, 3) if tx_bitrate is not None else None
        
        if scenario == "idle": 
            measurement['idle_mw'] = round(netmW, 3) if rx_bitrate is not None else None
        elif scenario == "tx":
            measurement['tx_mW'] = round(netmW, 3) if rx_bitrate is not None else None
        else: 
            measurement['rx_mW'] = round(netmW, 3) if rx_bitrate is not None else None

        measurement['rssi'] = get_rssi_from_iw('wlan0')

        # 🛠️ OPTIMIZATION: Read tx_power_conf only once every 15 loops (~2.5 minutes) to save CPU
        if loop_counter % 15 == 0 or tx_power_cached is None:
            tx_power_cached = get_tx_power_conf_from_nrc(NRC_CLI_BIN, NRC_CLI_DIR)
        measurement["tx_power_conf"] = tx_power_cached

        # ---------------------------------------------------------
        # 🛠️ DOMINANT MCS LOGIC
        # ---------------------------------------------------------
        curr_mcs_stats = get_cumulative_mcs_stats(NRC_CLI_BIN, NRC_CLI_DIR)
        dominant_mcs = None
        max_bytes_sent = -1

        if prev_mcs_stats and curr_mcs_stats:
            for mcs_idx, curr_bytes in curr_mcs_stats.items():
                prev_bytes = prev_mcs_stats.get(mcs_idx, 0)
                delta_bytes = curr_bytes - prev_bytes
                
                # Handle edge-case where counters reset (e.g. driver reload)
                if delta_bytes < 0:
                    delta_bytes = curr_bytes 
                
                # Find the MCS index that transmitted the highest amount of bytes
                if delta_bytes > 0 and delta_bytes > max_bytes_sent:
                    max_bytes_sent = delta_bytes
                    dominant_mcs = mcs_idx
        
        # If we successfully sent bytes, log the dominant MCS. 
        # If we sent 0 bytes (Idle/RX), fallback to your original get_last_mcs method
        if dominant_mcs is not None:
            measurement['mcs'] = dominant_mcs
        else:
            # Using the cached wifi_role here instead of querying it dynamically
            measurement['mcs'] = get_last_mcs(wifi_role, scenario)

        # Save the current state for the next loop
        prev_mcs_stats = curr_mcs_stats
        # ---------------------------------------------------------

        mqtt_client.publish(MQTT_TOPIC, json.dumps(measurement))
        print(f"Published: {measurement}")

    # 🛠️ OPTIMIZATION: Changed baseline loop time to 10 seconds to drastically reduce CPU overhead
    jitter = random.uniform(-1.0, 1.0)
    time.sleep(10 + jitter)
    loop_counter += 1