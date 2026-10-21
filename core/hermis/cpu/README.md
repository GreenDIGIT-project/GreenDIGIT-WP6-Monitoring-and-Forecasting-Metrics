# CPU Power Modeling & Validation

Measure and model CPU power consumption using a Shelly smart plug alongside system tools (e.g., `tegrastats` / `turbostat`).

## Table of Contents
- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Outputs](#outputs)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Extending](#extending)
- [License](#license)

## What it does
1. **Detects the device** (Jetson / UP Board / Raspberry Pi) and its local IP.
2. **Finds and queries** a Shelly plug (Gen1/Gen2) by IP to read instantaneous real power (W).
3. **Samples** at predefined CPU load levels (e.g., 10–100%) while measuring frequency (kHz/MHz), temperature (°C) and voltage (V).
4. **Filters outliers**, fits a simple power model, and saves a model JSON.
5. **(Optionally)** performs validation using platform tools (tegrastats/turbostat) and exports error metrics.

## Architecture
- `main.py`: Orchestrates sampling, model fitting, and (non‑RPi) validation.
- `config.py`: Central settings (sampling durations, load levels, output directories, IP→Shelly mapping).
- `logging_utils.py`: Consistent, colorized logging helpers.
- `cpu_utils_cleaned.py`: CPU load generation and sampling of CPU %, frequency, temperature, voltage; collects power from Shelly.
- `regression.py`: Outlier removal (IQR) and regression (origin fit), plus model serialization.
- `shelly.py`: Detects Shelly generation and reads power/voltage from the device REST API.

> Run the program from the project root so relative paths and imports resolve correctly.

## Requirements
- Linux host with **root** privileges for:
  - writing to `/sys/devices/system/cpu/.../cpufreq/*` (governor and min/max frequency)
  - running `tegrastats` (Jetson) or `turbostat` (UP Board)
- Python 3.8+
- Python packages: `numpy`, `pandas`, `requests`, `colorama`, `psutil`

## Installation
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install numpy pandas requests colorama psutil
# UP Board: install linux-tools (turbostat) from your distro.
# Jetson: tegrastats is typically preinstalled.
```

## Configuration
Edit `config.py`:
- `SAMPLE_DURATION`, `SAMPLES_PER_LEVEL`, `CPU_LEVELS` — experiment timing and load sweep
- `OUTPUT_DIR`, `MODEL_DIR` — output folders
- `RASPBERRY_MAPPING` — mapping of **local IP** to `(MQTT topic, Shelly IP)`

> Ensure the node's **local IP** exists in the mapping and the Shelly is reachable on your LAN.

## How it works

### 1) CPU preparation
The program sets the CPU governor and min/max frequencies on all cores. Root access is required.

### 2) Idle sampling
Collects multiple samples at idle to estimate \(P_{idle}\) and the average idle frequency.

### 3) Load sweep
For each entry in `CPU_LEVELS`:
- Apply synthetic load to reach the target percent on all cores.
- During each sampling window, record: CPU %, Shelly power (W), average frequency, temperature, voltage.
- Reject samples outside tolerance and remove outliers using **IQR**.
- Compute per‑level aggregates.

### 4) Modeling
- Separate variable CPU power as \(P_{cpu} = P_{total} - P_{idle}\).
- Fit an **origin‑through** linear model \(P_{cpu} \approx a\cdot CPU(\%)\) to obtain slope \(a\).
- Compute an offset/bias term \(b\) from idle vs. loaded behavior and derive:
  - \(P_{cpu} = a\cdot CPU(\%) + b\)
  - \(P_{var} = P_{idle} - b\)
  - \(P_{total} = P_{cpu} + P_{var}\)

### 5) Validation (Jetson/UP Board)
Sweep 0–100% load and compare measured power (platform tools) to model predictions. Export MAE / RMSE / R².

## Outputs
- `output/cpu_power_samples.csv` — raw samples per window  
  (e.g., `CPU_Usage_Percent`, `Ptotal_W`, `Pcpu_W`, `AvgFreq_MHz`, `Voltage_V`, `Temp_C`)
- `output/per_level_summary.csv` — per‑level aggregates  
  (e.g., `Level_%`, `AvgPower_W`, `AvgFreq_MHz`, `AvgVolt_V`, `AvgTemp_C`, `Samples_Kept`)
- `output/validation_results.csv` — validation results (if validation ran)
- `consumption-model/cpu_info.json` — saved model with fields `{ "a", "b", "p_other" }`

## Usage
Run from the project root with sufficient privileges:
```bash
sudo -E python3 main.py
```
The program logs node info (IP, Shelly IP/generation), sampling progress, a final model summary, and the generated files.

## Troubleshooting
- **Permission denied** in `configure_cpu_governor`: run as **root**.
- **Could not determine IP / mapping**: check the active NIC (e.g., `eth0`/`enp2s0`) and that the IP exists in `RASPBERRY_MAPPING`.
- **Shelly generation not detected** or timeouts: verify network reachability and the configured Shelly IP.
- **tegrastats / turbostat missing**: install/enable the corresponding tool on your platform.
- **Too many rejected samples (tolerance)**: adjust tolerance or increase `SAMPLES_PER_LEVEL`.
- **Empty aggregates**: ensure valid samples exist at each level and the Shelly returns power values.

## Extending
- Add more sensors or power sources.
- Try different regression strategies (e.g., linear with intercept).
- Create batch‑run variants to scan parameters or repeat experiments.

## License
Internal/research use — adapt as needed.
