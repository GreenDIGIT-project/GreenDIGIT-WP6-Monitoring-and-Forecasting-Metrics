import time
import json
import requests
import paho.mqtt.client as mqtt
import psutil
import socket
import fcntl
import struct
import random
import subprocess
import re
from jtop import jtop
import threading

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


#def get_power_gen1():
#    """Get power measurement for Gen1 Shelly."""
#    try:
#        response = requests.get(f'http://{SHELLY_IP}/status', timeout=5)
#        data = response.json()
#        power = data['meters'][0]['power']  # in Watts
#        voltage = data.get('emeters', [{}])[0].get('voltage', None)  # voltage may or may not exist
#        return {
#            'power': power,
#            'voltage': voltage,
#            'timestamp': time.time()
#        }
#    except Exception as e:
#        print(f"Error fetching Gen1 Shelly data: {e}")
#        return None

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

#def get_power_gen2():
#    """Get power measurement for Gen2 Shelly (Plus/Pro)."""
#    try:
#        response = requests.get(f'http://{SHELLY_IP}/rpc/Switch.GetStatus?id=0', timeout=5)
#        data = response.json()
#        return {
#            'power': data['apower'],      # active power in Watts
#            'voltage': data['voltage'],    # voltage in Volts
#            'timestamp': time.time()
#        }
#    except Exception as e:
#        print(f"Error fetching Gen2 Shelly data: {e}")
#        return None

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
    return psutil.cpu_percent(interval=1)

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


# --- Tegrastats Streaming Worker ---
tegrastats_proc = subprocess.Popen(
    ["sudo", "tegrastats", "--interval", "1000"],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    text=True,
    bufsize=1
)

latest_jetson_power = None

def _tegrastats_reader_thread():
    global latest_jetson_power
    try:
        for line in tegrastats_proc.stdout:
            match = re.search(r"(?:VDD_CPU_GPU_CV|CPU_GPU_CV)\s+(\d+)mW", line)
            if match:
                # Ενημερώνουμε τη μεταβλητή με τη νέα τιμή σε Watts
                latest_jetson_power = int(match.group(1)) / 1000.0
    except Exception as e:
        print(f"Tegrastats thread error: {e}")

threading.Thread(target=_tegrastats_reader_thread, daemon=True).start()

def get_jetson_internal_power():
    """On main loop. Returns the newest measurment"""
    return latest_jetson_power


#def get_jetson_internal_power():
#    """Reads instantaneous power from tegrastats and returns a dict of rails in Watts."""
#    try:
#        proc = subprocess.Popen(
#            ["sudo", "tegrastats", "--interval", "10"],
#            stdout=subprocess.PIPE,
#            stderr=subprocess.DEVNULL,
#            text=True
#        )
#        output = proc.stdout.readline()
#        
#        # Clean up process immediately to avoid zombies
#        proc.terminate()
#        try:
#            proc.wait(timeout=1)
#        except subprocess.TimeoutExpired:
#            proc.kill()
#
#        power_data = {}
#        # Parse all matching power rails (e.g., VDD_IN, VDD_CPU_GPU_CV)
#        for match in re.finditer(r"([A-Za-z0-9_]+)\s+(\d+)mW", output):
#            rail, mw = match.groups()
#            if rail.startswith("VDD_") or rail.startswith("POM_"):
#                power_data[f"jetson_{rail.lower()}_w"] = int(mw) / 1000.0
#        
#        return power_data
#    except Exception as e:
#        print(f"Error fetching tegrastats: {e}")
#        return {}

# ---- Main Loop ----
my_ip = get_ip_address('eth0')
    
if not my_ip:
    print("Could not find IP for eth0 interface.")
    #return

print(f"My eth0 IP address: {my_ip}")

if my_ip in raspberry_mapping:
    MQTT_TOPIC, SHELLY_IP = raspberry_mapping[my_ip]
    print(f"Node name: {MQTT_TOPIC}")
    print(f"Smart Meter IP: {SHELLY_IP}")
else:
    print("IP address not recognized in mapping.")

shelly_gen = detect_shelly_generation()
print(shelly_gen)

if not shelly_gen:
    print("Exiting: Shelly device not detected or unsupported.")
    exit(1)

#while True:
#    common_timestamp = time.time()  # 👈 Capture ONE timestamp per loop iteration
#
#    if shelly_gen == "gen1":
#        measurement = get_power_gen1(common_timestamp)
#    elif shelly_gen == "gen2":
#        measurement = get_power_gen2(common_timestamp)
#    else:
#        print("Unknown Shelly generation.")
#        break
#
#    if measurement:
#        cpu = get_cpu_usage()  # This still takes ~1 second because of psutil's 1-second sample window
#        measurement['cpu_usage'] = cpu
#        measurement['timestamp'] = common_timestamp  # Re-assign just to be sure
#
#        mqtt_client.publish(MQTT_TOPIC, json.dumps(measurement))
#        print(f"Published: {measurement}")
#
#    time.sleep(10)

while True:
    common_timestamp = time.time()  # 👈 Capture ONE timestamp per loop iteration

    if shelly_gen == "gen1":
        measurement = get_power_gen1(common_timestamp)
    elif shelly_gen == "gen2":
        measurement = get_power_gen2(common_timestamp)
    else:
        print("Unknown Shelly generation.")
        break

    if measurement:
        cpu = get_cpu_usage()  # Still takes ~1 second
        measurement['cpu_usage'] = cpu
        measurement['timestamp'] = common_timestamp  # Re-assign just to be sure

        jetson_power = get_jetson_internal_power()
        if jetson_power:
            #Not an estimation, in shake of appearing in the proper grafana window.
            measurement['HERMIS_power_cpu_estimation'] = jetson_power

        mqtt_client.publish(MQTT_TOPIC, json.dumps(measurement))
        print(f"Published: {measurement}")

    # 👉 Introduce random jitter between -1 and +1 second
    jitter = random.uniform(-1.0, 1.0)
    time.sleep(10 + jitter)
