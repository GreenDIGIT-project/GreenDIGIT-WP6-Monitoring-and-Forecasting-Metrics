import socket
import subprocess
import threading
import time
import os
from .log_utils import info, success, warning, error, critical, debug
from config import RESULTS_DIR

# ─────────────────────────────
# 📁 Paths & Output Configuration
# ─────────────────────────────
os.makedirs(RESULTS_DIR, exist_ok=True)

# ─────────────────────────────
# 🔁 Sync Configuration
# ─────────────────────────────
SYNC_PORT = 9090
SYNC_TIMEOUT = 60

def run_iperf_udp(role, remote_ip, local_ip, duration=20, log_prefix="iperf", bandwidth=None):
    """
    Runs iperf3 in UDP mode as server or client, with optional bandwidth setting and log output to file.

    Args:
        role (str): 'server' or 'client'
        remote_ip (str): IP of the peer node
        local_ip (str): Local bind IP
        duration (int): Time in seconds to run iperf
        log_prefix (str): Prefix for the output log file
        bandwidth (str, optional): e.g., '2M', '500K', etc.
    """
    subprocess.call("pkill -f iperf3", shell=True)

    if role == "server":
        cmd = ["iperf3", "-B", local_ip, "-s", "-1"]
    else:
        time.sleep(2)
        cmd = ["iperf3", "-B", local_ip, "-c", remote_ip, "-t", str(duration), "-u"]
        if bandwidth:
            cmd.extend(["-b", bandwidth, "--pacing-timer", "500"])

    info(f"[IPERF][{role.upper()}] Starting iperf3: UDP → {remote_ip}, Duration: {duration}s, Bandwidth: {bandwidth}")
    log_file = os.path.join(RESULTS_DIR, f"{log_prefix}_{role}.log")
    info(f"[IPERF][{role.upper()}] Logging output to: {log_file}")
    with open(log_file, "w") as logfile:
        debug(f"[IPERF][{role.upper()}] Command: {' '.join(cmd)}")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in iter(process.stdout.readline, ''):
            debug(f"[IPERF] {line.strip()}")
            logfile.write(line)
        process.stdout.close()
        process.wait()

def sync_start(role, remote_ip):
    """Performs TCP sync between client/server."""
    if role == "server":
        info(f"[SYNC][SERVER] Waiting for client connection on port {SYNC_PORT}...")
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", SYNC_PORT))
        server_socket.listen(1)
        server_socket.settimeout(SYNC_TIMEOUT)
        try:
            conn, addr = server_socket.accept()
            success(f"[SYNC][SERVER] Connection established from {addr}")
            conn.close()
            return True
        except socket.timeout:
            warning(f"[SYNC][SERVER] Timeout waiting for client (after {SYNC_TIMEOUT}s)")
            return False
        finally:
            server_socket.close()
    else:
        info(f"[SYNC][CLIENT] Attempting to connect to server: {remote_ip}:{SYNC_PORT}")
        start = time.time()
        while True:
            try:
                s = socket.create_connection((remote_ip, SYNC_PORT), timeout=3)
                s.close()
                success(f"[SYNC][CLIENT] Sync successful")
                return True
            except (socket.error, socket.timeout):
                if time.time() - start > SYNC_TIMEOUT:
                    warning(f"[SYNC][CLIENT] Timeout while connecting to server (after {SYNC_TIMEOUT}s)")
                    return False
                time.sleep(1)


def start_iperf_with_sync(role, remote_ip, local_ip, duration=20):
    """Wrapper to sync and run iperf."""
    sync_start(role, remote_ip)
    run_iperf_udp(role, remote_ip, local_ip, duration=duration)