import os
import json
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from statistics import mean



 
# -------------------------------
# CONFIGURATION
# -------------------------------
INFLUX_HOST = "10.64.44.196"
INFLUX_PORT = 8086
INFLUX_USER = "ilbekas"
INFLUX_PASSWORD = "!I[j~gtN25m{"
INFLUX_DB = "shelly_power"
WORKLOAD_DIR = "results"

SHORT_WORKLOAD_THRESHOLD_S = 60     # seconds
# -------------------------------
# Helper: Integrate over time (trapezoid)
# -------------------------------
def integrate_to_total(points, field_name):
    """
    Integrate field_name over time using trapezoidal rule.
    Returns total = sum(avg(value_i, value_{i+1}) * dt) with units (field_unit * seconds).
    """
    if not points or len(points) < 2:
        return 0.0

    sorted_points = sorted(points, key=lambda x: x["time"])
    total = 0.0

    for i in range(len(sorted_points) - 1):
        try:
            t1 = datetime.fromisoformat(sorted_points[i]["time"].replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(sorted_points[i + 1]["time"].replace("Z", "+00:00"))
            dt = (t2 - t1).total_seconds()
            if dt <= 0:
                continue

            v1 = float(sorted_points[i].get(field_name, 0) or 0)
            v2 = float(sorted_points[i + 1].get(field_name, 0) or 0)

            total += (v1 + v2) / 2.0 * dt
        except Exception:
            continue

    return total


def calculate_window_hours(start_iso, end_iso):
    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    return (end - start).total_seconds() / 3600.0


def influx_query(client, query: str):
    query = query.strip()
    try:
        result = client.query(query)
        return list(result.get_points()) if result else []
    except Exception as e:
        print(f"[ERROR] Influx query failed: {e}\nQuery: {query}")
        return []


def load_latest_json():
    files = [
        f for f in os.listdir(WORKLOAD_DIR)
        if f.startswith("workload_exec_") and f.endswith(".json")
    ]

    if not files:
        raise FileNotFoundError("No workload_exec_*.json found in results/")

    def extract_id(fname):
        return int(fname.split("_")[2].split(".")[0])

    latest = max(files, key=extract_id)
    full_path = os.path.join(WORKLOAD_DIR, latest)

    with open(full_path, "r") as f:
        data = json.load(f)

    print(f"Using workload JSON: {full_path}")
    return data


# -------------------------------
# FIXED Influx fetch (now() handling)
# -------------------------------
def fetch_influx(client, node, field, start_iso, end_iso):
    topic = f"power/{node}"

    if end_iso == "now()":
        end_clause = "now()"
    else:
        end_clause = f"'{end_iso}'"

    q = (
        f'SELECT "{field}" FROM "mqtt_consumer" '
        f"WHERE \"topic\" = '{topic}' "
        f"AND time >= '{start_iso}' AND time <= {end_clause} "
        f"ORDER BY time ASC"
    )

    return influx_query(client, q)


# -------------------------------
# Short workload estimator
# -------------------------------
def estimate_short_workload(client, src_node, dst_node, duration_s,
                            bandwidth_req_mbps, data_amount_mb,
                            lookback_days=90, max_samples=50,
                            align_tolerance_s=5):

    if duration_s <= 0:
        return 0.0, 0.0, int(data_amount_mb * 1_000_000)

    target = float(bandwidth_req_mbps)
    low = target * 0.9
    high = target * 1.1

    end = "now()"
    start = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat() + "Z"

    tx_points = fetch_influx(client, src_node, "tx_mW", start, end)
    rx_points = fetch_influx(client, dst_node, "rx_mW", start, end)
    rate_points = fetch_influx(client, dst_node, "rx_bitrate_mbps", start, end)

    if not tx_points or not rx_points or not rate_points:
        return 0.0, 0.0, int(data_amount_mb * 1_000_000)

    tx_series = [
        (datetime.fromisoformat(p["time"].replace("Z", "+00:00")),
         float(p.get("tx_mW", 0) or 0))
        for p in tx_points
    ]
    rx_series = [
        (datetime.fromisoformat(p["time"].replace("Z", "+00:00")),
         float(p.get("rx_mW", 0) or 0))
        for p in rx_points
    ]

    matched_tx = []
    matched_rx = []

    for p in rate_points:
        try:
            rate = float(p.get("rx_bitrate_mbps", 0) or 0)
        except Exception:
            continue

        if low <= rate <= high:
            t_rate = datetime.fromisoformat(p["time"].replace("Z", "+00:00"))

            tx_near = min(
                tx_series,
                key=lambda x: abs((x[0] - t_rate).total_seconds()),
                default=None
            )
            rx_near = min(
                rx_series,
                key=lambda x: abs((x[0] - t_rate).total_seconds()),
                default=None
            )

            if tx_near and rx_near:
                if (abs((tx_near[0] - t_rate).total_seconds()) <= align_tolerance_s and
                        abs((rx_near[0] - t_rate).total_seconds()) <= align_tolerance_s):
                    matched_tx.append(tx_near[1])
                    matched_rx.append(rx_near[1])

    if not matched_tx:
        return 0.0, 0.0, int(data_amount_mb * 1_000_000)

    matched_tx = matched_tx[-max_samples:]
    matched_rx = matched_rx[-max_samples:]

    avg_tx = mean(matched_tx)
    avg_rx = mean(matched_rx)

    tx_mW_seconds = avg_tx * (duration_s)
    rx_mW_seconds = avg_rx * (duration_s)

    rx_bytes = int(data_amount_mb * 1_000_000)

    return tx_mW_seconds, rx_mW_seconds, rx_bytes


def to_influx_time(dt: datetime) -> str:
    """Convert datetime to InfluxDB-compatible UTC ISO string."""
    return dt.astimezone(tz=None).replace(tzinfo=None).isoformat() + "Z"


def analyze_workload(workload: dict, influx_db: str = INFLUX_DB):
    """Analyze a single workload and return prediction JSON."""
    exec_id = workload.get("exec_unit_id", "exec_unknown")
    src_node = workload["src_node"]
    dst_node = workload["dst_node"]

    # --- Time Logic ---
    start_dt = datetime.fromisoformat(workload["start_time"].replace("Z", "+00:00")) + timedelta(seconds=10)
    end_dt = datetime.fromisoformat(workload["end_time"].replace("Z", "+00:00")) + timedelta(seconds=20)
    duration_s = ((end_dt - start_dt) - timedelta(seconds=10)).total_seconds()

    # --- Influx Client (Remains Identical) ---
    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD,
        database=influx_db,
    )
    client.ping()

    # --- Data Fetching Logic (Remains Identical) ---
    if duration_s < SHORT_WORKLOAD_THRESHOLD_S:
        tx_mW_seconds, rx_mW_seconds, rx_bytes = estimate_short_workload(
            client,
            src_node,
            dst_node,
            duration_s,
            workload["bandwidth_req_mbps"],
            workload["data_amount_mb"],
        )
    else:
        start = start_dt.replace(tzinfo=None).isoformat() + "Z"
        end = end_dt.replace(tzinfo=None).isoformat() + "Z"
        
        src_tx_points = fetch_influx(client, src_node, "tx_mW", start, end)
        dst_rx_points = fetch_influx(client, dst_node, "rx_mW", start, end)
        dst_rx_rate_points = fetch_influx(client, dst_node, "rx_bitrate_mbps", start, end)
        dst_tx_rate_points = fetch_influx(client, dst_node, "tx_bitrate_mbps", start, end)

        tx_mW_seconds = integrate_to_total(src_tx_points, "tx_mW")
        rx_mW_seconds = integrate_to_total(dst_rx_points, "rx_mW")
        
        rx_megabit_seconds = integrate_to_total(dst_rx_rate_points, "rx_bitrate_mbps")
        rx_MB = rx_megabit_seconds / 8.0
        rx_bytes = int(round(rx_MB * 1_000_000))

        tx_megabit_seconds = integrate_to_total(dst_tx_rate_points, "tx_bitrate_mbps")
        tx_MB = tx_megabit_seconds / 8.0
        tx_bytes = int(round(tx_MB * 1_000_000))

    # --- Conversion & Efficiency Calculation (The Added Part) ---
    tx_Wh = tx_mW_seconds / 3_600_000
    rx_Wh = rx_mW_seconds / 3_600_000
    total_energy_Wh = tx_Wh + rx_Wh

    # Calculate Work: (AmountOfDataTransferred in bytes / Energy_wh)
    work_val = rx_bytes / total_energy_Wh if total_energy_Wh > 0 else 0.0

    # --- Result Dictionary (Updated with the new keys) ---
    prediction = workload.copy()
    prediction["energy_results"] = {
        "total_tx_Wh": tx_Wh,
        "total_rx_Wh": rx_Wh,
        "total_energy_Wh": total_energy_Wh,
        "MB": rx_bytes / 1_000_000,
        "work_bytes_per_wh": work_val
    }

    return prediction


# -------------------------------
# MAIN
# -------------------------------
def main():
    #workload = load_latest_json()

    exec_id = workload.get("exec_unit_id", "exec_unknown")
    src_node = workload["src_node"]
    dst_node = workload["dst_node"]

    start_dt = datetime.fromisoformat(workload["start_time"].replace("Z", "+00:00")) + timedelta(seconds=10)
    end_dt = datetime.fromisoformat(workload["end_time"].replace("Z", "+00:00")) + timedelta(seconds=20)

    start = start_dt.isoformat()
    end = end_dt.isoformat()

    duration_s = ((end_dt - start_dt) - timedelta(seconds=10)).total_seconds()
    window_hours = duration_s / 3600.0

    #print(f"\nAnalysis Window: {start} → {end}")
    #print(f"Source Node: {src_node}  |  Destination Node: {dst_node}")
    #print(f"Duration: {window_hours:.6f} hours")

    client = InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD,
        database=INFLUX_DB,
    )

    client.ping()
    print("Connected to InfluxDB\nFetching data")

    src_tx_points = fetch_influx(client, src_node, "tx_mW", start, end)
    dst_rx_points = fetch_influx(client, dst_node, "rx_mW", start, end)
    dst_rx_rate_points = fetch_influx(client, dst_node, "rx_bitrate_mbps", start, end)
    dst_tx_rate_points = fetch_influx(client, dst_node, "tx_bitrate_mbps", start, end)

    #print(f"TX power points: {len(src_tx_points)}")
    #print(f"RX power points: {len(dst_rx_points)}")
    #print(f"RX bitrate points: {len(dst_rx_rate_points)}\n")

    if duration_s < SHORT_WORKLOAD_THRESHOLD_S:
        tx_mW_seconds, rx_mW_seconds, rx_bytes = estimate_short_workload(
            client,
            src_node,
            dst_node,
            duration_s,
            workload["bandwidth_req_mbps"],
            workload["data_amount_mb"],
        )
    else:
        tx_mW_seconds = integrate_to_total(src_tx_points, "tx_mW")
        rx_mW_seconds = integrate_to_total(dst_rx_points, "rx_mW")

        rx_megabit_seconds = integrate_to_total(dst_rx_rate_points, "rx_bitrate_mbps")
        rx_MB = rx_megabit_seconds / 8.0
        rx_bytes = int(round(rx_MB * 1_000_000))

        tx_megabit_seconds = integrate_to_total(dst_tx_rate_points, "tx_bitrate_mbps")
        tx_MB = tx_megabit_seconds / 8.0
        tx_bytes = int(round(tx_MB * 1_000_000))

    tx_Wh = tx_mW_seconds / 3_600_000
    rx_Wh = rx_mW_seconds / 3_600_000
    total_energy_Wh = tx_Wh + rx_Wh

    # Calculate Work: (AmountOfDataTransferred in bytes / Energy_wh)
    work_val = (tx_bytes + rx_bytes) / total_energy_Wh if total_energy_Wh > 0 else 0.0
    
    #print("=" * 72)
    #print("SUMMARY (destination-side received bytes only)")
    #print("=" * 72)
    #print(f"TX Energy: {tx_mW_seconds:.2f} mW·s → {tx_Wh:.6f} Wh")
    #print(f"RX Energy: {rx_mW_seconds:.2f} mW·s → {rx_Wh:.6f} Wh")
    #print(f"TOTAL Energy: {total_energy_Wh:.6f} Wh\n")
    #print(f"Actual Data Transferred (destination receive):")
    #print(f"  RX: {rx_bytes / 1_000_000:.3f} MB  ({rx_bytes} bytes)")
    #print("=" * 72)

    prediction = workload.copy()
    prediction["energy_results"] = {
        "total_tx_Wh": tx_Wh,
        "total_rx_Wh": rx_Wh,
        "total_energy_Wh": total_energy_Wh,
        "MB": rx_bytes / 1_000_000,
        "work_bytes_per_wh": work_val,
    }

    #out_path = f"results/prediction_{exec_id}.json"
    #with open(out_path, "w") as f:
        #json.dump(prediction, f, indent=2)

    #print(f"\nSaved prediction file → {out_path}")


if __name__ == "__main__":
    main()
