import time
import json
import requests
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from zoneinfo import ZoneInfo

# Κάνουμε import τα links από το αρχείο link_config.py
from link_config import LINKS 

env_path ='/home/greendigit/app/greendigit-l3-iot-uth-deployment/.env'
load_dotenv(env_path)

# ==========================================
# ⚙️ ΡΥΘΜΙΣΕΙΣ (CONFIGURATION)
# ==========================================
API_URL = "https://gd3.lab.uvalight.net/l2/sites/SLICES-GR-UTH/snapshots"
POLL_INTERVAL_SECONDS = 600
TOTAL_SITE_NODES = 10 

# --- InfluxDB Ρυθμίσεις ---
INFLUX_HOST = os.getenv("INFLUX_HOST")
API_TOKEN = os.getenv("API_TOKEN")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", 8086))
INFLUX_DB_POWER   = os.getenv("INFLUX_DB_POWER")
INFLUX_DB_WORKLOADS = os.getenv("INFLUX_DB_WORKLOADS")
INFLUX_USER = os.getenv("INFLUX_USER")
INFLUX_PASS = os.getenv("INFLUX_PASS")

MEASUREMENT  = os.getenv("INFLUX_MEASUREMENT", "mqtt_consumer")
TOPIC_TAG    = os.getenv("INFLUX_TOPIC_TAG", "topic")

# Σταθερές για μετρικές που δεν υποστηρίζονται ακόμα (Θα ανανεωθούν στο μέλλον)
DEFAULT_CARBON_INTENSITY = 250.0  # gCO2eq/kWh
DEFAULT_PUE_ESTIMATE = 1.2        # Power Usage Effectiveness (1.0 = ιδανικό)
DEFAULT_PACKET_LOSS = 0.0         # %
DEFAULT_NETWORK_JITTER = 2.5      # ms

# ==========================================
# 📊 ΣΥΝΑΡΤΗΣΕΙΣ ΒΑΣΗΣ & ΕΠΕΞΕΡΓΑΣΙΑΣ
# ==========================================

def get_local_time():
    return datetime.now(ZoneInfo('Europe/Athens')).strftime('%H:%M:%S')

def fetch_data_from_db():
    """
    Αυτή η συνάρτηση συνδέεται στη βάση δεδομένων InfluxDB
    και τραβάει τις τελευταίες μετρήσεις από όλους τους κόμβους.
    """
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASS, database=INFLUX_DB_POWER, timeout=5)
    
    # Λίστα με τα topics των ενεργών nodes (1-9 και 12, χωρίς 10, 11 και jetson)
    topics = [f"power/node{i:02d}" for i in range(1, 10)] + ["power/node12"]
    
    db_results = []
    try:
        for topic in topics:
            # Παίρνουμε την πιο πρόσφατη μέτρηση (LIMIT 1) για τον κόμβο, στα τελευταία 2 λεπτά (για να μην θεωρηθεί stale)
            query = f"SELECT * FROM \"{MEASUREMENT}\" WHERE \"{TOPIC_TAG}\" = '{topic}' AND time >= now() - 2m ORDER BY time DESC LIMIT 1"
            result = client.query(query)
            points = list(result.get_points(measurement=MEASUREMENT))
            
            if points:
                p = points[0]
                # Εξάγουμε τα πεδία, χρησιμοποιώντας default 0.0 αν λείπουν
                db_results.append({
                    "node": topic,
                    "power": float(p.get("power") or 0.0),
                    "cpu_usage": float(p.get("cpu_usage") or 0.0),
                    "rx_bitrate_mbps": float(p.get("rx_bitrate_mbps") or 0.0),
                    "tx_bitrate_mbps": float(p.get("tx_bitrate_mbps") or 0.0),
                    "timestamp": p.get("time")
                })
    except Exception as e:
        print(f"[{get_local_time()}] Σφάλμα κατά την ανάγνωση από το InfluxDB: {e}")
    finally:
        client.close()
        
    return db_results

def get_packet_loss(window_start: datetime, window_end: datetime) -> float:
    if not window_start or not window_end:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(minutes=10)
        
        
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASS, database=INFLUX_DB_WORKLOADS, timeout=5)
    try:
        # Μετατροπή των datetimes σε ISO 8601 string (π.χ. 2026-08-03T10:00:00Z) για την InfluxDB
        start_iso = window_start.isoformat(timespec='seconds').replace("+00:00", "Z")
        end_iso = window_end.isoformat(timespec='seconds').replace("+00:00", "Z")
        
        # Ζητάμε από την InfluxDB να μας βρει τον μέσο όρο
        query = f'SELECT mean("packet_loss_percent") as avg_loss FROM "workloads" WHERE time >= \'{start_iso}\' AND time <= \'{end_iso}\''
        result = client.query(query)
        points = list(result.get_points())
        
        if points and points[0].get("avg_loss") is not None:
            return round(float(points[0]["avg_loss"]), 2)
    except Exception as e:
        print(f"[{get_local_time()}] Σφάλμα κατά τον υπολογισμό Packet Loss: {e}")
    finally:
        client.close()

def process_site_metrics(raw_data):
    """
    Επεξεργάζεται τα raw δεδομένα των κόμβων και βγάζει συγκεντρωτικές μετρικές (Site Level).
    """
    if not raw_data:
        return None

    active_nodes_count = len(raw_data)
    node_availability = active_nodes_count / TOTAL_SITE_NODES
    
    # Λίστα με τα topics των ενεργών κόμβων που επέστρεψε η InfluxDB
    active_node_topics = set(node.get("node") for node in raw_data)

    # Υπολογισμός Link Availability (Αγνόηση του link6)
    valid_links = {name: data for name, data in LINKS.items() if name != "link6"}
    active_links_count = 0
    
    for link_name, link_data in valid_links.items():
        # Ένα link θεωρείται available ΜΟΝΟ αν ΚΑΙ το AP ΚΑΙ το STA topic υπάρχουν στα ενεργά nodes
        if link_data["ap_topic"] in active_node_topics and link_data["sta_topic"] in active_node_topics:
            active_links_count += 1
            
    total_valid_links = len(valid_links)
    link_availability = active_links_count / total_valid_links if total_valid_links > 0 else 0.0

    # Υπολογισμός Operational Status
    if node_availability == 1.0:
        op_status = "UP"
    elif node_availability > 0.0:
        op_status = "DEGRADED"
    else:
        op_status = "DOWN"

    # Αθροίσματα και Μέσοι Όροι
    total_power_w = sum(node.get("power", 0) for node in raw_data)
    avg_cpu_util = sum(node.get("cpu_usage", 0) for node in raw_data) / active_nodes_count
    free_cpu_capacity = 100 - avg_cpu_util
    total_rx_mbps = sum(node.get("rx_bitrate_mbps", 0) for node in raw_data)
    total_tx_mbps = sum(node.get("tx_bitrate_mbps", 0) for node in raw_data)

    start_time = None
    end_time = None 

    packet_loss = get_packet_loss(start_time, end_time)

    TOTAL_MBPS_CAPACITY = 12
    
    # Network utilization (εμπειρικός υπολογισμός - % της συνολικής χωρητικότητας)
    # Ας υποθέσουμε 12Mbps συνολικό capacity 
    network_util = ((total_rx_mbps + total_tx_mbps) / TOTAL_MBPS_CAPACITY) * 100

    # Energy Consumed (Wh). Αν μετράμε κάθε λεπτό: Wh = Watts * (1 λεπτό / 60 λεπτά)
    energy_consumed_wh = total_power_w * (POLL_INTERVAL_SECONDS / 3600.0)

    carbon_intensity = fetch_carbon_intensity()

    # Συνθέτουμε το λεξικό με τα δεδομένα σύμφωνα με το UML
    metrics = {
        "status": {
            "operational_status": op_status,
            "maintenance_flag": False,
            "scheduled_maintenance": {"start": None, "end": None}
        },
        "availability": {
            "node_availability": round(node_availability, 4),
            "link_availability": round(link_availability, 4),
            "stability_score": round(node_availability, 2), # Απλοποίηση προς το παρόν
            "packet_loss": packet_loss,
            "network_jitter": DEFAULT_NETWORK_JITTER,
            "network_utilization": round(network_util, 2),
            "available_bandwidth": round(1000.0 - (total_rx_mbps + total_tx_mbps), 2),
            "free_cpu_capacity": free_cpu_capacity
        },
        "usage": {
            "cpu_util_avg": round(avg_cpu_util, 2),
            "queue_length": 0,       # Θα προστεθεί μελλοντικά
            "remaining_jobs": 0,     # Θα προστεθεί μελλοντικά
            "load_index": round((avg_cpu_util / 100.0) + (network_util / 100.0), 3) # Composite
        },
        "efficiency": {
            "energy_consumed": round(energy_consumed_wh, 4), # Wh aggregate σε αυτό το παράθυρο
            "pue_estimate": DEFAULT_PUE_ESTIMATE,
            "carbon_intensity": carbon_intensity,
            "energy_per_task_proxy": 0.0
        },
        "quality": {
            "update_frequency": POLL_INTERVAL_SECONDS,
            "data_confidence": 1.0 if node_availability > 0.8 else 0.5, # Εμπειρικό confidence
            "coverage_ratio": round(node_availability, 4),
            "stale_flag": False
        }
    }
    return metrics

def build_payload(site_metrics):
    """
    Δημιουργεί το τελικό JSON που απαιτεί το curl command (SLICES API).
    """
    # Δημιουργία timestamp στη μορφή: 2026-07-13T07:46:58.541Z
    now_iso = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    
    site_metrics['quality']['timestamp'] = now_iso # Προσθήκη timestamp στο quality κομμάτι

    payload = {
        "ts": now_iso,
        "capabilities": {}, # Κενό προς το παρόν βάσει του cURL
        "status": site_metrics["status"],
        "availability": site_metrics["availability"],
        "usage": site_metrics["usage"],
        "efficiency": site_metrics["efficiency"],
        "quality": site_metrics["quality"]
    }
    return payload

def fetch_carbon_intensity() -> float:
    """
    Αντλεί το δυναμικό carbon intensity από το Wattnet API χρησιμοποιώντας το token από το .env.
    """
    # Φορτώνουμε ξανά το .env για να πάρουμε το πιο φρέσκο WATTNET_TOKEN
    load_dotenv(env_path, override=True)
    wattnet_token = os.getenv("WATTNET_TOKEN")
    
    if not wattnet_token:
        print(f"[{get_local_time()}] ⚠️ Λείπει το WATTNET_TOKEN. Χρήση default ({DEFAULT_CARBON_INTENSITY}).")
        return DEFAULT_CARBON_INTENSITY
        
    url = "https://api.wattnet.eu/v1/footprints?zone=GR&footprint_type=carbon&aggregate=false&use_global=true"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {wattnet_token.strip()}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Εξαγωγή δεδομένων με βάση τη δομή του Wattnet JSON (series -> values)
            if isinstance(data, list) and len(data) > 0:
                series = data[0].get("series", [])
                if isinstance(series, list) and len(series) > 0:
                    values = series[0].get("values", [])
                    if values:
                        # Υπολογίζουμε τον μέσο όρο από όλες τις τιμές που επέστρεψε
                        intensity_values = []
                        for entry in values:
                            if len(entry) == 2:
                                try:
                                    intensity_values.append(float(entry[1]))
                                except (TypeError, ValueError):
                                    continue
                                    
                        if intensity_values:
                            avg_intensity = sum(intensity_values) / len(intensity_values)
                            return round(avg_intensity, 2)
                            
        print(f"[{get_local_time()}] ⚠️ Σφάλμα ή κενά δεδομένα από Wattnet (Status: {response.status_code}).")
        return DEFAULT_CARBON_INTENSITY
        
    except Exception as e:
        print(f"[{get_local_time()}] ❌ Σφάλμα επικοινωνίας με Wattnet API: {e}")
        return DEFAULT_CARBON_INTENSITY

def send_to_api(payload):
    """Στέλνει το POST request στο εξωτερικό API."""

    load_dotenv(env_path, override=True)
    fresh_token = os.getenv("API_TOKEN")
    
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {API_TOKEN}',
        'Content-Type': 'application/json'
    }

    # --- DEBUG LOG: Εκτύπωση της ισοδύναμης εντολής curl ---
    payload_str = json.dumps(payload)
    
    curl_cmd = f"""curl -X 'POST' \\
  '{API_URL}' \\
  -H 'accept: application/json' \\
  -H 'Authorization: Bearer {fresh_token}' \\
  -H 'Content-Type: application/json' \\
  -d '{payload_str}'"""
  
    print(f"[{get_local_time()}] [DEBUG] Αποστολή στο API. Αντίστοιχο curl:\n{curl_cmd}\n")
    # --------------------------------------------------------

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
        if response.status_code in [200, 201, 202]:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Επιτυχής αποστολή (Status: {response.status_code})")
        else:
            print(f"[{get_local_time()}] Σφάλμα API: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[{get_local_time()}] Αποτυχία σύνδεσης στο API: {e}")

# ==========================================
# 🚀 ΚΥΡΙΩΣ ΒΡΟΧΟΣ (MAIN LOOP)
# ==========================================

if __name__ == "__main__":
    print("Ξεκινάει ο Site Data Forwarder για το SLICES-GR-UTH...")
    
    while True:
        try:
            API_TOKEN = os.getenv("API_TOKEN")

            # 1. Άντληση δεδομένων από τη βάση
            raw_data = fetch_data_from_db()
            
            # 2. Επεξεργασία (Aggregation σε επίπεδο Site)
            site_metrics = process_site_metrics(raw_data)
            
            if site_metrics:
                # 3. Δημιουργία Payload
                json_payload = build_payload(site_metrics)
                
                # 4. Αποστολή
                send_to_api(json_payload)
            else:
                print(f"[{get_local_time()}] Δεν βρέθηκαν δεδομένα στη βάση για αυτό το διάστημα.")
                
        except Exception as e:
            print(f"Προέκυψε σφάλμα στο κύριο loop: {e}")
            
        # Αναμονή για τον επόμενο κύκλο
        time.sleep(POLL_INTERVAL_SECONDS)