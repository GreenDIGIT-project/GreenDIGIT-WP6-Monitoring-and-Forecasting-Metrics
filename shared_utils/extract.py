import time
import subprocess
import re
import csv
from os.path import expanduser
from collections import deque

from .net_utils import (get_cpu_utilization, get_power, estimate_cpu_power, estimate_network_power)
import utils.net_utils as nu

from .log_utils import info, error, success

def moving_avg(buffer):
    return sum(buffer) / len(buffer) if buffer else 0.0

def get_wifi_role(interface='wlan0'):
    try:
        out = subprocess.check_output(['iw', 'dev', interface, 'info']).decode()
        match = re.search(r'type\s+(\w+)', out)
        if match:
            t = match.group(1).lower()
            if t == 'ap':
                return 'AP'
            elif t == 'managed':
                return 'STA'
        return 'Unknown'
    except Exception as e:
        error(f"Failed to get role: {e}")
        return 'Unknown'

def get_interface_bytes(interface='wlan0'):
    """
    Return (rx_bytes, tx_bytes) for the FIRST station shown by:
        iw dev <iface> station dump
    Use this when you know there's only one peer.
    """
    cmd = ["iw", "dev", interface, "station", "dump"]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        print("iw error:", proc.stderr.strip())
        return None, None

    rx_bytes = None
    tx_bytes = None
    in_station = False

    for line in proc.stdout.splitlines():
        line = line.strip()

        # first station we see → start recording
        if line.startswith("Station "):
            in_station = True
            continue

        if not in_station:
            continue

        m_tx = re.match(r"tx bytes:\s+(\d+)", line)
        if m_tx:
            tx_bytes = int(m_tx.group(1))
            continue

        m_rx = re.match(r"rx bytes:\s+(\d+)", line)
        if m_rx:
            rx_bytes = int(m_rx.group(1))
            continue

    return rx_bytes, tx_bytes

def get_tx_power_conf_from_nrc(nrc_cli_bin, nrc_cli_dir):
    try:
        p = subprocess.run(
            [nrc_cli_bin],
            input="show config\nexit\n",
            text=True,
            capture_output=True,
            cwd=nrc_cli_dir,
            timeout=3
        )
        out = (p.stdout or "") + "\n" + (p.stderr or "")
        print("DEBUG CLI OUTPUT:\n", out)

        # Tx Power Type : -5 [LIMIT]
        m = re.search(r"Tx\s*Power\s*Type\s*:\s*~?\s*([-\d.]+)\s*\[[A-Z_]+\]", out, re.IGNORECASE)
        return float(m.group(1)) if m else None
    except Exception:
        return None

def infer_activity(prev_rx, prev_tx, curr_rx, curr_tx, byte_threshold=100, ratio_threshold=1.5, min_total_bytes=200):
    delta_rx = curr_rx - prev_rx
    delta_tx = curr_tx - prev_tx
    total = delta_rx + delta_tx
    if total < min_total_bytes:
        return 'idle'
    if delta_tx > ratio_threshold * delta_rx:
        return 'tx'
    elif delta_rx > ratio_threshold * delta_tx:
        return 'rx'
    else:
        return 'tx_rx'
    
def get_rssi_from_iw(interface='wlan0'):
    try:
        out = subprocess.check_output(['iw', interface, 'station', 'dump'], text=True)
        match = re.search(r'signal:\s*(-\d+)\s*dBm', out)
        return int(match.group(1)) if match else None
    except Exception as e:
        error(f"Failed to parse RSSI from iw: {e}")
        return None

# def get_tx_power_from_cli(mcs, cli_path='~/nrc_pkg/script/cli_app'):
#     cli = expanduser(cli_path)
#     try:
#         out = subprocess.check_output([cli, 'show', 'autotxgain'], text=True)
#         pattern = fr'Tx power index for MCS {mcs}\s*:\s*(\d+)'
#         match = re.search(pattern, out)
#         return int(match.group(1)) if match else None
#     except Exception as e:
#         error(f"Failed to extract TX Power from autotxgain: {e}")
#         return None

def get_tx_power_from_cli(cli_path='~/nrc_pkg/script/cli_app'):
    cli = expanduser(cli_path)
    try:
        out = subprocess.check_output([cli, 'show', 'config'], text=True)
        match = re.search(r'TX_Gain\s*:\s*(\d+)', out)
        return int(match.group(1)) if match else None
    except Exception as e:
        error(f"Failed to extract TX_Gain from config: {e}")
        return None

def get_last_mcs(role, scenario, cli_path='~/nrc_pkg/script/cli_app'):
    try:
        cli_full_path = expanduser(cli_path)
        if scenario == 'rx':
            out = subprocess.check_output([cli_full_path, 'show', 'mac', 'rx', 'stats'], text=True)
        else:
            out = subprocess.check_output([cli_full_path, 'show', 'mac', 'tx', 'stats'], text=True)
        match = re.search(r'last MCS:(\d+)', out)
        return int(match.group(1)) if match else None
    except Exception as e:
        error(f"Failed to extract last MCS: {e}")
        return None

def clear_mac_stats(cli_path='~/nrc_pkg/script/cli_app'):
    """
    Executes CLI commands to clear both TX and RX MAC statistics.

    Args:
        cli_path (str): Path to the CLI application.

    Returns:
        bool: True if both TX and RX stats were cleared successfully, False otherwise.
    """
    try:
        cli_full_path = expanduser(cli_path)

        # Clear TX MAC statistics
        tx_result = subprocess.run(
            [cli_full_path, 'show', 'mac', 'tx', 'clear'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if "OK" not in tx_result.stdout:
            error("[ERROR] Failed to clear TX stats")
            print(tx_result.stdout)
            print(tx_result.stderr)
            return False

        # Clear RX MAC statistics
        rx_result = subprocess.run(
            [cli_full_path, 'show', 'mac', 'rx', 'clear'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if "OK" not in rx_result.stdout:
            error("[ERROR] Failed to clear RX stats")
            print(rx_result.stdout)
            print(rx_result.stderr)
            return False

        # Success message
        success("MAC TX and RX statistics cleared successfully.")
        return True

    except Exception as e:
        error(f"Exception occurred while clearing MAC stats: {e}")
        return False

def extract_loop_to_csv(csv_writer, duration, scenario_hint, shelly_ip, shelly_gen, peak,
                        interface='wlan0', sampling_interval=4):
    
    cpu_util_buf = deque(maxlen=5) 
    prev_tx_power = None   
    prev_scenario = None

    role = get_wifi_role(interface)
    info(f"Starting extract loop for {duration}s | Role: {role}")
    start_time = time.time()
    end_time = start_time + duration

    # ασφάλεια για link_load
    safe_peak = float(peak) if (peak is not None and peak > 0) else 0.0

    while time.time() < end_time:
        rx1, tx1 = get_interface_bytes(interface)
        cpu_util = get_cpu_utilization(sample_duration=sampling_interval)
        cpu_util_buf.append(cpu_util)
        rx2, tx2 = get_interface_bytes(interface)

        last_mcs = get_last_mcs(role, scenario_hint)
        rssi = get_rssi_from_iw()
        tx_power = get_tx_power_from_cli()

        # reset buffers αν αλλάξει παραμετροποίηση
        if (prev_tx_power is None) or (tx_power != prev_tx_power) or (scenario_hint != prev_scenario):
            cpu_util_buf.clear()
            info(f"Reset MA due to change: tx_power {prev_tx_power}->{tx_power} or scenario {prev_scenario}->{scenario_hint}")
            prev_tx_power = tx_power
            prev_scenario = scenario_hint

        # MA μόνο για CPU
        cpu_util_buf.append(float(cpu_util))
        avg_cpu = sum(cpu_util_buf) / len(cpu_util_buf)

        scenario = infer_activity(rx1, tx1, rx2, tx2)
        delta_rx = rx2 - rx1
        delta_tx = tx2 - tx1

        throughput_bps_tx = ((delta_tx) * 8) / sampling_interval  # bits/sec
        throughput_bps_rx = ((delta_rx) * 8) / sampling_interval  # bits/sec
        throughput_mbps_rx = throughput_bps_rx / 1e6  # convert to Mbps
        throughput_mbps_tx = throughput_bps_tx / 1e6  # convert to Mbps

        # Link Load calculation (TX + RX)
        delta = delta_tx if scenario_hint == "tx" else delta_rx
        # link load (ασφαλές σε peak=0)
        if safe_peak > 0:
            delta = delta_tx if scenario_hint == "tx" else delta_rx
            link_load = min((delta * 8) / (safe_peak * 1e6 * sampling_interval), 1.0)
        else:
            link_load = 0.0

        total_power = get_power(shelly_ip, shelly_gen)
        cpu_power = estimate_cpu_power(avg_cpu)
        net_power = estimate_network_power(total_power, avg_cpu)

        # Energy per bit [J/bit] ανάλογα με το scenario
        if scenario == "tx" and throughput_bps_tx > 0:
            eb = net_power / throughput_bps_tx
        elif scenario == "rx" and throughput_bps_rx > 0:
            eb = net_power / throughput_bps_rx
        else:
            eb = None

        # Log output
        eb_str = f"{eb:.2e} J/bit" if eb is not None else "N/A"

        info(
            f"[{scenario.upper()}] "
            f"TX: {delta_tx} B | RX: {delta_rx} B | "
            f"TX Th: {throughput_mbps_tx:.2f} Mbps | RX Th: {throughput_mbps_rx:.2f} Mbps\n"
            f"→ TX Power: {tx_power} | RSSI: {rssi} dBm | Link Load: {link_load:.3f}\n"
            f"→ CPU Util: {cpu_util:.1f}% (MA: {avg_cpu:.1f}%)\n"
            f"→ Power [Total: {total_power:.2f} W | CPU: {cpu_power:.2f} W | Other: {nu.P_OTHER:.2f} W | Net: {net_power:.2f} W]\n"
            f"→ Energy per bit: {eb_str}"
        )

        # CSV output
        csv_writer.writerow({
            'timestamp': time.strftime("%H:%M:%S"),
            'role': role,
            'scenario': scenario,
            'tx_bytes': delta_tx,
            'rx_bytes': delta_rx,
            'throughput_mbps_tx' : throughput_mbps_tx,
            'throughput_mbps_rx' : throughput_mbps_rx,
            'tx_power': tx_power,
            'rssi': rssi,
            'link_load': link_load,
            'cpu_util': cpu_util,
            'cpu_power': cpu_power,
            'p_other': nu.P_OTHER,
            'network_power': net_power,
            'energy_per_bit': eb
        })
