import requests # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from shared_utils.log_utils import success, warning

# Detect Shelly plug generation based on API response
def detect_shelly_generation(ip):
    try:
        r = requests.get(f"http://{ip}/rpc/Switch.GetStatus?id=0", timeout=5)
        if r.status_code == 200 and "apower" in r.json():
            success("Detected Shelly Gen2 (Plus/Pro)")
            return "gen2"
    except: pass

    try:
        r = requests.get(f"http://{ip}/status", timeout=5)
        if r.status_code == 200 and "meters" in r.json():
            success("Detected Shelly Gen1")
            return "gen1"
    except: pass

    warning("Could not detect Shelly generation.")
    return None

# Get instantaneous power reading from Shelly plug
def get_power(ip, generation):
    try:
        if generation == "gen2":
            return requests.get(f"http://{ip}/rpc/Switch.GetStatus?id=0", timeout=5).json()["apower"]
        elif generation == "gen1":
            return requests.get(f"http://{ip}/status", timeout=5).json()["meters"][0]["power"]
    except:
        return None
    
def get_voltage(ip: str, gen: str):
    try:
        if gen == "gen1":
            r = requests.get(f"http://{ip}/status", timeout=2).json()
            return float(r["meters"][0]["voltage"])
        else:  # gen2
            r = requests.get(f"http://{ip}/rpc/Shelly.GetStatus", timeout=2).json()
            # προσαρμόσου στο μοντέλο σου (em:0 ή switch:0)
            if "em:0" in r and "voltage" in r["em:0"]:
                return float(r["em:0"]["voltage"])
            if "switch:0" in r and "voltage" in r["switch:0"]:
                return float(r["switch:0"]["voltage"])
            return None
    except Exception:
        return None
