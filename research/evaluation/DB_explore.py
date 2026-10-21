from influxdb import InfluxDBClient
import pandas as pd

# -------------------------
# InfluxDB credentials
# -------------------------
INFLUX_HOST = "10.64.44.196"
INFLUX_PORT = 8086
INFLUX_USER = "ilbekas"
INFLUX_PASS = "!I[j~gtN25m{"
INFLUX_DB = "workloads_IoT"

# -------------------------
# Connect to InfluxDB
# -------------------------
client = InfluxDBClient(
    host=INFLUX_HOST,
    port=INFLUX_PORT,
    username=INFLUX_USER,
    password=INFLUX_PASS,
    database=INFLUX_DB
)

print("Connected to InfluxDB")

# -------------------------
# Query workloads
# -------------------------
query = "SELECT * FROM workloads"

try:
    result = client.query(query)
except Exception as e:
    print("Query failed:", e)
    exit()

points = list(result.get_points())

if len(points) == 0:
    print("No data found in workloads measurement.")
    exit()

# -------------------------
# Convert to pandas
# -------------------------
df = pd.DataFrame(points)

if "time" in df.columns:
    df["time"] = pd.to_datetime(df["time"])

print("\n===== DATASET OVERVIEW =====")
print("Rows:", len(df))
print("Columns:", list(df.columns))

# -------------------------
# Numeric overview
# -------------------------
print("\n===== BASIC STATISTICS =====")
print(df.select_dtypes(include=["number"]).describe())
# -------------------------
# Correlation matrix (still useful)
# -------------------------
print("\n===== CORRELATION MATRIX =====")
print(df.corr(numeric_only=True))

# ==========================================================
# THROUGHPUT ANALYSIS (NEW SECTION)
# ==========================================================
print("\n\n===== THROUGHPUT ANALYSIS =====")

if "throughput_mbps" not in df.columns:
    print("Throughput column not found in dataset.")
else:
    thr = df["throughput_mbps"]

    print("\n[1] Missing values:")
    print("  NaNs:", thr.isna().sum())

    print("\n[2] Basic statistics:")
    print(thr.describe())

    print("\n[3] Min / Max values:")
    print("  Min throughput:", thr.min())
    print("  Max throughput:", thr.max())

    print("\n[4] Median and mean:")
    print("  Mean throughput:", thr.mean())
    print("  Median throughput:", thr.median())

    print("\n[5] Variability (std deviation):")
    print("  Std dev:", thr.std())

    print("\n[6] Percentiles:")
    print("  25%:", thr.quantile(0.25))
    print("  50%:", thr.quantile(0.50))
    print("  75%:", thr.quantile(0.75))
    print("  95%:", thr.quantile(0.95))

    print("\n[7] Throughput vs system load indicators (quick insight):")

    if "duration_s" in df.columns:
        print("  Correlation (throughput vs duration):",
              df["throughput_mbps"].corr(df["duration_s"]))

    if "packet_loss_percent" in df.columns:
        print("  Correlation (throughput vs packet loss):",
              df["throughput_mbps"].corr(df["packet_loss_percent"]))

    if "total_energy_Wh" in df.columns:
        print("  Correlation (throughput vs energy):",
              df["throughput_mbps"].corr(df["total_energy_Wh"]))

    print("\n[8] Simple stability indicator:")
    cv = thr.std() / thr.mean() if thr.mean() != 0 else 0
    print("  Coefficient of variation:", cv)