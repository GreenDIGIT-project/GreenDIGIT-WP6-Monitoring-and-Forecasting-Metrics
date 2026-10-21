# config.py

RESULTS_DIR = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/net/results"   # κοινό runner + iperf_utils
DATA_DIR    = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/net/data"
MODELS_DIR  = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/net/models"

INTERFACE = "wlan0"
IP_AP  = "192.168.200.1"
IP_STA = "192.168.200.25"

TX_POWER_LIST = [12, 17, 23]
BANDWIDTH_PERCENTAGES = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

SAMPLE_INTERVAL = 4
WARMUP   = 5
COOLDOWN = 1
DURATION = 29       # 29 - 5 = 24s → 6 δείγματα
IDLE_DURATION = 120
SYNC_SLEEP = 2

CPU_MODEL_JSON = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/cpu/consumption-model/cpu_info.json"

# === Device-to-meter mapping ===
RASPBERRY_MAPPING = {
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
    "10.64.44.196": ("jetson_orin", "10.64.44.220"),
}
