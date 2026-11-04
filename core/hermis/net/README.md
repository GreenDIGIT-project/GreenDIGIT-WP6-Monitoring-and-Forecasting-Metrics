# HERMIS — Wi‑Fi Energy/Throughput Pipeline
**Order:** Sampling → MakeData → Modeling → Monitoring

End‑to‑end workflow to collect Wi‑Fi + power data, build clean datasets, train energy/throughput models, and run live monitoring with alerts.

---

## Quick start
```bash
# 1) Sampling — collect raw measurements
python3 sampling.py

# 2) MakeData — build clean train/test CSVs
python3 make_data.py --mode train     # or: --mode both

# 3) Modeling — train and export selected artifacts
python3 modeling.py

# 4) Monitoring — online inference + alerts
python3 monitoring.py --minutes 30    # omit --minutes to run until Ctrl+C
```
Artifacts land under `results/`, `data/`, and `models/selected/` and are consumed by later stages.

---

## Requirements
- Linux host, Python 3.8+
- Python: `numpy`, `pandas`, `scikit-learn`, `requests`
- Tools: `iperf3` (binary on PATH)
- Optional: Shelly smart plug for power readings
- Network access between AP/STA peers as defined in `config.py`

---

## Configure once
Edit **`config.py`** for paths, NIC, peers, and timing:
- Paths: `RESULTS_DIR`, `DATA_DIR`, `MODELS_DIR`
- Radio/Peers: `INTERFACE`, `IP_AP`, `IP_STA`
- Timing: `SAMPLE_INTERVAL`, `WARMUP`, `COOLDOWN`, `DURATION`, `IDLE_DURATION`
- Models: `CPU_MODEL_JSON` (if used by your setup)

> Run commands from the project root so relative paths work.

---

## Stage 1 — Sampling
**Script:** `sampling.py`  
Collects idle / TX / RX measurements. Role (AP/STA) is auto‑detected from config. Drives traffic via iperf3 and reads radio + power metrics.

**Output:** CSVs in `results/` with columns like:
`scenario, throughput_mbps_tx, throughput_mbps_rx, tx_power, rssi, cpu_power, network_power, energy_per_bit, ...`

**Run:**
```bash
python3 sampling.py
```

---

## Stage 2 — MakeData
**Script:** `make_data.py`  
Consolidates `results/*.csv` into clean datasets:

- Train: `data/idle.csv`, `data/tx.csv`, `data/rx.csv`
- (Optional) Test: `data/idle_test.csv`, `data/tx_test.csv`, `data/rx_test.csv`

Normalizes units (e.g., µJ/bit), drops empties, warns on missing columns.

**Run:**
```bash
python3 make_data.py --mode train   # or: --mode both
```

---

## Stage 3 — Modeling
**Script:** `modeling.py`  
Trains selected regressors/classifiers on the MakeData outputs and exports artifacts to **`models/selected/`**:

- `energy_tx.pkl`, `energy_rx.pkl` (energy per bit models)
- `throughput_tx.pkl`, `throughput_rx.pkl` (throughput models)
- `idle_mean.txt` (baseline)

A full run log + `manifest.json` is also saved under `models/HERMIS_runs/<timestamp>/`.

**Inputs:** `data/tx.csv`, `data/rx.csv` (and optionally `data/idle.csv`)  
**Run:**
```bash
python3 modeling.py
```

---

## Stage 4 — Monitoring
**Script:** `monitoring.py`  
Loads the **selected** artifacts and a **LinkCapacityProvider** built from `data/tx.csv`/`data/rx.csv` (TX keyed by TX power, RX keyed by RSSI, using high‑percentile peaks).

Loops every `SAMPLE_INTERVAL` seconds, logging observed vs. predicted throughput, link capacity/load, energy/bit, and power (idle/tx/rx/total). Supports K‑of‑N alerting with cooldowns.

**Output:** `results/monitoring_<timestamp>.csv`  
**Run:**
```bash
python3 monitoring.py
# or
python3 monitoring.py --minutes 30 --outfile results/monitoring_run.csv
```

---

## Repository layout (key files)
- `sampling.py` — stage 1
- `make_data.py` — stage 2
- `modeling.py` — stage 3
- `monitoring.py` — stage 4
- `extract.py`, `iperf_utils.py`, `net_utils.py` — helpers for parsing, traffic generation, and networking
- `link_capacity.py` — capacity lookup from prior data (e.g., q95 peaks)
- `log_utils.py` — structured/color logging
- `config.py` — global settings

---

## Troubleshooting
- **Artifacts missing in `models/selected/`** → re‑run `modeling.py`.
- **Capacity shows NaN** → ensure `data/tx.csv` & `data/rx.csv` exist from MakeData and include `tx_power` / `rssi` with valid throughputs.
- **iperf3 not found** → install the `iperf3` package/binary and ensure it’s on PATH.
- **Empty/short CSVs** → extend `DURATION`, increase `SAMPLE_INTERVAL`, or check peer connectivity.

---

## License
Internal / research use; adapt as needed.
