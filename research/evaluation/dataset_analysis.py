from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import os

# -------------------------
# Setup output directory
# -------------------------
os.makedirs("plots", exist_ok=True)

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
    print("No data found.")
    exit()

# -------------------------
# Convert to pandas
# -------------------------
df = pd.DataFrame(points)

print("\n===== DATASET OVERVIEW =====")
print("Rows:", len(df))
print("Columns:", list(df.columns))

print("\n===== BASIC STATS =====")
print(df.describe())

print("\n===== SAMPLE DATA =====")
print(df.head())

# -------------------------
# Time conversion
# -------------------------
df["time"] = pd.to_datetime(df["time"])

# -------------------------
# Derived features
# -------------------------
df["energy_per_MB"] = df["total_energy_Wh"] / df["data_amount_mb"]
df["J_per_bit"] = (df["total_energy_Wh"] * 3600) / (df["data_amount_mb"] * 8 * 1e6)

# -------------------------
# Correlation matrix
# -------------------------
corr = df.corr(numeric_only=True)

print("\n===== CORRELATION MATRIX =====")
print(corr)

plt.figure(figsize=(10, 8))
plt.imshow(corr, interpolation='nearest')
plt.colorbar()
plt.title("Feature Correlation")
plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
plt.yticks(range(len(corr.columns)), corr.columns)
plt.tight_layout()
plt.savefig("plots/correlation_matrix.png")
plt.close()

# -------------------------
# Histograms (distributions)
# -------------------------
features = [
    "data_amount_mb",
    "duration_s",
    "total_energy_Wh",
    "J_per_bit",
    "throughput_mbps"
]

for f in features:
    plt.figure()
    df[f].hist(bins=50)
    plt.title(f"Distribution of {f}")
    plt.xlabel(f)
    plt.ylabel("Frequency")
    plt.savefig(f"plots/hist_{f}.png")
    plt.close()

# -------------------------
# Scatter plots (key relationships)
# -------------------------
pairs = [
    ("data_amount_mb", "total_energy_Wh"),
    ("data_amount_mb", "duration_s"),
    ("throughput_mbps", "duration_s"),
    ("J_per_bit", "throughput_mbps")
]

for x, y in pairs:
    plt.figure()
    plt.scatter(df[x], df[y], alpha=0.3)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(f"{y} vs {x}")
    plt.savefig(f"plots/scatter_{x}_{y}.png")
    plt.close()

# -------------------------
# Time-based workload intensity
# -------------------------
df["minute"] = df["time"].dt.floor("min")

workload_counts = df.groupby("minute").size()

plt.figure()
workload_counts.plot()
plt.title("Workloads Over Time (Parallelism Proxy)")
plt.xlabel("Time")
plt.ylabel("Number of Workloads")
plt.savefig("plots/workload_intensity.png")
plt.close()

# -------------------------
# Rolling average (smoother)
# -------------------------
rolling = workload_counts.rolling(window=10).mean()

plt.figure()
rolling.plot()
plt.title("Smoothed Workload Intensity")
plt.xlabel("Time")
plt.ylabel("Workload Count (Rolling Avg)")
plt.savefig("plots/workload_intensity_smoothed.png")
plt.close()

print("\nAll plots saved in ./plots/")