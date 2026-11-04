# ======================== config.py ========================
# Configuration constants
METER_URL = '-'
SAMPLE_DURATION = 4
SAMPLES_PER_LEVEL = 25
CPU_LEVELS = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
OUTPUT_DIR = "output"
MODEL_DIR = "/home/pi/greendigit-l3-iot-uth-deployment/core/hermis/cpu/consumption-model"

# Mapping IPs to MQTT topic and Shelly IP
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
    "10.64.44.116": ("power/upboard", "10.64.44.117")
}
