from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
import os
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from link_config import LINKS

env_path ='/home/greendigit/app/greendigit-l3-iot-uth-deployment/.env'
load_dotenv(env_path)

# ---------------- InfluxDB v1 configuration ---------------- #

INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", 8086))
INFLUX_DB   = os.getenv("INFLUX_DB_POWER")
INFLUX_USER = os.getenv("INFLUX_USER")
INFLUX_PASS = os.getenv("INFLUX_PASS")

MEASUREMENT  = os.getenv("INFLUX_MEASUREMENT", "mqtt_consumer")
TOPIC_TAG    = os.getenv("INFLUX_TOPIC_TAG", "topic")

FIELD_TX_BITRATE = os.getenv("FIELD_TX_BITRATE", "tx_bitrate_mbps")
FIELD_RX_BITRATE = os.getenv("FIELD_RX_BITRATE", "rx_bitrate_mbps")
FIELD_TX_MW      = os.getenv("FIELD_TX_MW", "tx_mW")
FIELD_RX_MW      = os.getenv("FIELD_RX_MW", "rx_mW")
FIELD_RSSI       = os.getenv("FIELD_RSSI", "rssi")  
FIELD_LINK_QUALITY = os.getenv("FIELD_LINK_QUALITY", "link_quality")

# ---------------- Node-level metrics ---------------- #

@dataclass
class NodeMetrics:
    topic: str
    is_busy: bool = False
    avg_rx_mbps: float = 0.0
    latest_rssi: Optional[float] = None
    latest_link_quality: Optional[float] = None

@dataclass
class LinkState:
    name: str
    channel: int
    busy: int
    avg_thr_mbps: float
    rssi: Optional[float]
    neighbor_interference: float
    link_quality: Optional[float]
    
    def __repr__(self):
        return (f"LinkState(name='{self.name}', channel={self.channel}, busy={self.busy}, "
                f"rssi={self.rssi}, lq={self.link_quality}, neighbor_interference={self.neighbor_interference:.4f})")

class StateBuilder:
    def __init__(self):
        self.host = INFLUX_HOST
        self.port = INFLUX_PORT
        self.username = INFLUX_USER
        self.password = INFLUX_PASS
        self.database = INFLUX_DB
        self.client = InfluxDBClient(
            host=self.host, 
            port=self.port, 
            username=self.username, 
            password=self.password, 
            database=self.database, 
            timeout=5
        )

    def get_node_metrics(self, topic: str, alpha: float = 0.2) -> NodeMetrics:
        # 1. Φέρνουμε δεδομένα ΜΟΝΟ από τα τελευταία 30s, ταξινομημένα από το πιο πρόσφατο στο πιο παλιό (DESC)
        query = f"SELECT * FROM \"{MEASUREMENT}\" WHERE \"{TOPIC_TAG}\" = '{topic}' AND time >= now() - 30s ORDER BY time DESC"
        
        try:
            res = self.client.query(query)
            points = list(res.get_points(measurement=MEASUREMENT))
        except Exception as e:
            print(f"Error querying state for {topic}: {e}")
            return NodeMetrics(topic=topic)

        # 2. Αν δεν υπάρχει τίποτα στα 30s, επιστρέφουμε αμέσως κενό
        if not points:
            return NodeMetrics(topic=topic)

        latest_rssi = None
        latest_link_quality = None

        # 3. Βρίσκουμε το πιο πρόσφατο RSSI και LQ 
        for p in points:
            if latest_rssi is None and p.get(FIELD_RSSI) is not None:
                latest_rssi = p.get(FIELD_RSSI)
            if latest_link_quality is None and p.get(FIELD_LINK_QUALITY) is not None:
                latest_link_quality = p.get(FIELD_LINK_QUALITY)
                
            if latest_rssi is not None and latest_link_quality is not None:
                break

        # 4. Υπολογισμός Is_Busy και συγκέντρωση RX τιμών για EMA
        is_busy = False
        rx_values = []

        for p in points:
            tx = p.get(FIELD_TX_MW)
            rx = p.get(FIELD_RX_MW)
            
            val_tx = float(tx) if tx is not None else 0.0
            val_rx = float(rx) if rx is not None else 0.0
            
            if val_tx > 5.0 or val_rx > 5.0:
                is_busy = True
                
            rx_mbps = p.get(FIELD_RX_BITRATE)
            if rx_mbps is not None:
                rx_values.append(float(rx_mbps))

        # 5. Υπολογισμός Exponential Moving Average (EMA)
        ema_rx = 0.0
        if rx_values:
            # Αντιστρέφουμε τη λίστα για να την διατρέξουμε χρονολογικά (από το παλαιότερο στο νεότερο)
            rx_values.reverse()
            
            # Αρχικοποιούμε το EMA με την παλαιότερη τιμή του παραθύρου
            ema_rx = rx_values[0]
            
            # Εφαρμόζουμε τον τύπο EMA για τις υπόλοιπες τιμές
            for val in rx_values[1:]:
                ema_rx = (val * alpha) + (ema_rx * (1.0 - alpha))

        return NodeMetrics(
            topic=topic,
            is_busy=is_busy,
            avg_rx_mbps=ema_rx,  # Τώρα επιστρέφει το EMA αντί για το SMA
            latest_rssi=latest_rssi,
            latest_link_quality=latest_link_quality
        )

    # ---------------- Link-level helpers ---------------- #

    def _combine_ap_sta_to_link(self, link_name: str, ap_metrics: NodeMetrics, sta_metrics: NodeMetrics, channel: int) -> LinkState:
        # Ένα link είναι busy αν είτε το AP είτε το STA είναι busy
        busy = 1 if (ap_metrics.is_busy or sta_metrics.is_busy) else 0
        
        # Το Throughput του Link καθορίζεται κυρίως από το average RX του STA τα τελευταία 30s
        avg_thr_mbps = sta_metrics.avg_rx_mbps
        
        rssi = sta_metrics.latest_rssi if sta_metrics.latest_rssi is not None else ap_metrics.latest_rssi
        link_quality = sta_metrics.latest_link_quality if sta_metrics.latest_link_quality is not None else ap_metrics.latest_link_quality

        return LinkState(
            name=link_name,
            channel=channel,
            busy=busy,
            avg_thr_mbps=avg_thr_mbps,
            rssi=rssi,
            neighbor_interference=0.0, # Θα υπολογιστεί στο αμέσως επόμενο βήμα
            link_quality=link_quality
        )

    def _compute_interferences(self, links: Dict[str, LinkState]) -> None:
        # Βάρη παρεμβολής ανάλογα με την απόσταση καναλιών (channel distance)
        weights = {
            0: 1.0,   # Το ίδιο κανάλι δίνει 100% της τιμής του
            1: 0.8,  # Απόσταση +1 ή -1 δίνει 85%
            2: 0.3,   # Απόσταση +2 ή -2 δίνει 60%
            3: 0.2,   # Απόσταση +3 ή -3 δίνει 40%
            4: 0.15    # Απόσταση +4 ή -4 δίνει 20%
        }
        
        for name_i, li in links.items():
            interference = 0.0
            for name_j, lj in links.items():
                if name_i == name_j: 
                    continue # Αποφεύγουμε τον εαυτό του
                
                dist = abs(li.channel - lj.channel)
                w = weights.get(dist, 0.0) # Απόσταση >= 5 έχει βάρος 0.0
                
                if w > 0:
                    interference += (lj.avg_thr_mbps * w)
                    
            li.neighbor_interference = interference

    # ---------------- Public: build_state ---------------- #

    def build_state(self, workload_size_bits: Optional[float] = None) -> Tuple[List[float], Dict[str, LinkState]]:
        link_states: Dict[str, LinkState] = {}
        
        node_cache: Dict[str, NodeMetrics] = {}
        def get_nm(topic: str) -> NodeMetrics:
            if topic not in node_cache:
                node_cache[topic] = self.get_node_metrics(topic)
            return node_cache[topic]

        for link_name, cfg in LINKS.items():
            link_states[link_name] = self._combine_ap_sta_to_link(
                link_name, 
                get_nm(cfg["ap_topic"]), 
                get_nm(cfg["sta_topic"]), 
                cfg["channel"]
            )

        self._compute_interferences(link_states)
        
        # --- State Vector (1 Global + 4 per Link) ---
        
        # 1. Global: Πλήθος busy links
        busy_count = sum(1 for ls in link_states.values() if ls.busy)
        s: List[float] = []
        s.append(float(busy_count)) 

        # 2. Per Link: Busy, RSSI, Link Quality, Neighbor Interference
        for link_name in sorted(LINKS.keys()):
            ls = link_states[link_name]
            
            safe_rssi = float(ls.rssi) if ls.rssi is not None else -90.0
            safe_lq = float(ls.link_quality) if ls.link_quality is not None else 0.0
            
            s.extend([
                float(ls.busy),
                safe_rssi,
                safe_lq,
                float(ls.neighbor_interference)
            ])

        return s, link_states

if __name__ == "__main__":
    sb = StateBuilder()
    state_vec, link_states = sb.build_state(workload_size_bits=1000)
    print("State Vector (30s Window + Weighted Interference):", state_vec)
    for name, ls in link_states.items():
        print(f"{ls}")