import time
import threading
import queue
import random 
import subprocess
import json
import os
import uuid
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
from state_builder import StateBuilder
from agent import RLAgent
from link_config import LINKS 
from remote_set_tx_power import remote_set_tx_power
from influxdb import InfluxDBClient

# --- FastAPI Imports ---
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Load environment variables
env_path ='/home/greendigit/app/greendigit-l3-iot-uth-deployment/.env'
load_dotenv(env_path)

# --- Configuration ---
NUM_LINKS = len(LINKS) 
STATE_DIM = 1 + (NUM_LINKS * 4) 
CONTROLLER_PORT = 8899

INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", 8086))
INFLUX_DB   = os.getenv("INFLUX_DB_WORKLOADS")
INFLUX_USER = os.getenv("INFLUX_USER")
INFLUX_PASS = os.getenv("INFLUX_PASS")

TRAFFIC_GEN_HOST = os.getenv("TRAFFIC_GEN_HOST", "10.64.44.196")
TRAFFIC_GEN_PORT = os.getenv("TRAFFIC_GEN_PORT", "8000")

# --- Helper Logger ---
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# --- Components ---
sb = StateBuilder()
agent = RLAgent(state_dim=STATE_DIM, num_links=NUM_LINKS)

workload_queue = queue.Queue()
pending_queue = deque()

# --- FastAPI App Definition ---
app = FastAPI()

# Pydantic Model for incoming JSON validation
class WorkloadRequest(BaseModel):
    bandwidth: str
    data_amount: str
    start_time: str
    mode: str = "best" # Default value if missing
    noise_std: float = 0.15

@app.post("/execute")
async def execute_workload(req: WorkloadRequest):
    """
    Endpoint to receive execution requests.
    FastAPI automatically validates JSON against WorkloadRequest model.
    """
    try:
        # 1. Parsing Data Amount
        try:
            size_mb = int(req.data_amount.upper().replace("M", ""))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid data_amount format. Use '20M'.")

        # 2. Generate ID
        internal_id = f"req_{uuid.uuid4().hex[:6]}"
        size_bits = size_mb * 8_000_000 

        workload = {
            'id': internal_id,
            'size_mb': size_mb,
            'size_bits': size_bits,
            'mode': req.mode,
            'noise_std': req.noise_std
        }
        
        workload_queue.put(workload)
        log(f"  [API] Received Request: {req.data_amount} (Mode: {req.mode}, Noise: {req.noise_std}) -> Queued as {internal_id}")
        
        return {"status": "Queued", "internal_id": internal_id}
        
    except Exception as e:
        log(f"API Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def run_api_server():
    uvicorn.run(app, host="0.0.0.0", port=CONTROLLER_PORT, log_level="warning")

# --- Background State Fetcher ---
class StateCache:
    def __init__(self, interval=0.5):
        self.latest_state = None
        self.latest_link_data = None
        self.lock = threading.Lock()
        self.interval = interval
        self.running = True

    def start(self):
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def _loop(self):
        log("State Cache: Started background polling...")
        while self.running:
            try:
                s_vec, l_data = sb.build_state(workload_size_bits=0)
                with self.lock:
                    self.latest_state = s_vec
                    self.latest_link_data = l_data
            except Exception as e:
                log(f"State Cache Error: {e}")
            time.sleep(self.interval)

    def get_state(self, current_workload_size=0.0):
        with self.lock:
            if self.latest_state is None:
                return None, None 
            s = list(self.latest_state)
            return s, self.latest_link_data

state_cache = StateCache(interval=0.2) 

def apply_link_config(link_name, tx_power):
    log(f"  [CMD] {link_name} -> Set TX Power: {tx_power} dBm")
    sender_ip = LINKS[link_name]["ap_ip"]
    remote_set_tx_power(sender_ip, LINKS[link_name].get("ssh_user", "pi"), "raspberry", tx_power)

def get_workload_results(exec_id):
    if not exec_id: return None
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASS, database=INFLUX_DB)
    try:
        query = f"SELECT duration_s, packet_loss_percent, total_energy_Wh, MB FROM \"workloads\" WHERE \"exec_unit_id\" =~ /^{exec_id}/ LIMIT 1"
        result = client.query(query)
        points = list(result.get_points())
        if points:
            p = points[0]
            return {
                "duration_s": float(p.get("duration_s", 0)),
                "packet_loss_percent": float(p.get("packet_loss_percent", 0)),
                "total_energy_Wh": float(p.get("total_energy_Wh", 0)),
                "MB": float(p.get("MB", 0))
            }
        else:
            return None
    except Exception as e:
        log(f"  [Results Error] InfluxDB query failed: {e}")
        return None
    finally:
        client.close()

def start_transmission(link_name, size_mb, mode, bandwidth_mbps):
    traffic_gen_url = f"http://{TRAFFIC_GEN_HOST}:{TRAFFIC_GEN_PORT}/execute-legacy"
    log(f"  [CMD] {link_name} -> Forwarding execution to {traffic_gen_url} (Size: {size_mb} MB, BW: {bandwidth_mbps} Mbps, Mode: {mode})")

    if link_name in LINKS:
        sta_topic = LINKS[link_name]["sta_topic"]
        destination_node = sta_topic.split("/")[-1]
    else:
        destination_node = "node04"

    data_amount_str = f"{int(size_mb)}M"
    start_time_str = "2026-01-01T00:00:00Z"

    payload = {
        "destination_node": destination_node,
        "bandwidth": f"{bandwidth_mbps}M", # Εδώ στέλνουμε το Bandwidth που διάλεξε το RL
        "data_amount": data_amount_str,
        "start_time": start_time_str,
        "mode": mode
    }

    command = ["curl", "-X", "POST", traffic_gen_url, "-H", "Content-Type: application/json", "-d", json.dumps(payload)]
    exec_id = None
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)        
        try:
            resp_json = json.loads(result.stdout)
            exec_id = resp_json.get("exec_id")
            if exec_id:
                log(f"  [TRACKING] Workload ID received: {exec_id}")
        except json.JSONDecodeError:
            log("  [ERROR] Could not parse server response JSON.")
    except Exception as e:
        log(f"ERROR starting transmission: {e}")

    return exec_id

def get_transmission_status(exec_id):
    if not exec_id: return 'RUNNING'
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASS, database=INFLUX_DB)
    try:
        query = f"SELECT * FROM \"workloads\" WHERE \"exec_unit_id\" =~ /^{exec_id}/ LIMIT 1"
        result = client.query(query)
        points = list(result.get_points())
        
        if points:
            log(f"  [DB SUCCESS] Found record for {exec_id}!")
            return 'COMPLETED'
        else:
            return 'RUNNING'
    except Exception as e:
        log(f"  [DB ERROR] Check failed for {exec_id}: {e}")
        return 'RUNNING'
    finally:
        client.close()

def calculate_reward(mb_received, duration, energy_wh, pkt_loss):
    if duration <= 0 or energy_wh <= 0: return 0
    efficiency = (mb_received / energy_wh) * (1 - (pkt_loss/100))
    return efficiency/100

def control_loop():
    log("System Online. Waiting for cache warm-up...")
    state_cache.start()
    while state_cache.get_state()[0] is None: time.sleep(0.1)
    
    log(f"Controller Service Ready (FastAPI). Listening on port {CONTROLLER_PORT}...")
    
    link_names = sorted(LINKS.keys())
    active_tasks = {} 

    while True:
        # 1. Queue Handling
        try:
            while True:
                wl = workload_queue.get_nowait()
                pending_queue.append(wl)
                log(f"\n>>> Enqueued Workload: {wl['id']} ({wl['size_mb']} MB) | Queue: {len(pending_queue)}")
        except queue.Empty:
            pass

        # 2. Monitor Tasks
        completed_tasks = []
        for tid, task in list(active_tasks.items()):
            status = get_transmission_status(task['exec_id'])
            
            elapsed = time.time() - task['start_ts']
            
            # --- DEAD LINK SENSOR ---
            if status == 'RUNNING' and elapsed > 60.0:
                _, link_states = state_cache.get_state(0)
                if link_states:
                    l_state = link_states.get(task['link'])
                    if l_state and l_state.busy == 0:
                        log(f"  [DEAD LINK SENSOR] Task {tid} on {task['link']} dropped to IDLE prematurely. iperf likely failed! Marking as FAILED.")
                        status = 'FAILED'
            # -------------------------------------

            if time.time() - task.get('last_log_time', 0) > 10.0:
                log(f"  [Monitor] {tid} ({task['exec_id']}) on {task['link']}: {status} | Elapsed: {elapsed:.1f}s")
                task['last_log_time'] = time.time()

            if status == 'COMPLETED':
                completed_tasks.append(tid)
                results = get_workload_results(task['exec_id'])
                
                if results:
                    duration = results['duration_s']
                    energy_wh = results['total_energy_Wh']
                    mb = results['MB']
                    loss_pct = results['packet_loss_percent']
                    log(f"  [Stats] Duration: {duration}s | Energy: {energy_wh:.4f}Wh | MB: {mb} | Loss: {loss_pct}%")
                    reward = calculate_reward(mb, duration, energy_wh, loss_pct)
                else:
                    log(f"  [Warn] No metrics found in DB for {tid}. Using fallback reward = 0.")
                    reward = 0.0
                
                log(f"  [Complete] Task {tid} finished. Reward: {reward:.4f}")
                
                # UPDATE THE BRAIN
                loss = agent.train_step(task['initial_state'], task['action_idx'], task['action_power'], task['action_bw'], reward)
                log(f"  [Training] Model updated (Loss: {loss:.4f})")
                agent.save_model()
                
            elif status == 'FAILED':
                completed_tasks.append(tid)
                reward = 0.0 
                log(f"  [Complete] Task {tid} FAILED (Runtime Error sensed). Reward: {reward:.4f}")
                
                loss = agent.train_step(task['initial_state'], task['action_idx'], task['action_power'], task['action_bw'], reward)
                log(f"  [Training] Model updated with FAILURE penalty (Loss: {loss:.4f})")
                agent.save_model()

            elif status == 'RUNNING':
                if task.get('mode') == 'train':
                    pass
                elif task.get('mode') == 'best':
                    current_state, _ = state_cache.get_state(0)
                    
                    # Ζητάμε νεα βέλτιστη απόφαση. (Το new_bw αγνοείται εδώ γιατί το iperf 
                    # δεν μπορεί να αλλάξει ταχύτητα όσο εκπέμπει)
                    _, new_pow, _ = agent.get_action(current_state, training=False)
                    
                    if abs(new_pow - task['action_power']) >= 1.0:
                        log(f"  [Dynamic Tuning] {tid}: {task['action_power']} -> {new_pow} dBm (BW remains fixed at {task['action_bw']}M)")
                        apply_link_config(task['link'], new_pow)
                        task['action_power'] = new_pow

        for tid in completed_tasks:
            del active_tasks[tid]

        # 3. Schedule Pending
        if pending_queue:
            busy_links = set(t['link'] for t in active_tasks.values())
            if len(busy_links) < NUM_LINKS:
                wl = pending_queue[0]
                w_bits = wl['size_bits']
                state_vec, _ = state_cache.get_state(w_bits)
                
                action_mask = [0 if name in busy_links else 1 for name in link_names]
                use_exploration = (wl['mode'] == 'train')
                
                # Ο πράκτορας αποφασίζει: Link, Power, ΚΑΙ Bandwidth
                link_idx, tx_power, chosen_bw = agent.get_action(
                    state_vec, 
                    training=use_exploration, 
                    action_mask=action_mask,
                    noise_std=wl.get('noise_std', 0.15)
                )
                selected_link = link_names[link_idx]
                
                if selected_link in busy_links:
                     pass 
                else:
                    pending_queue.popleft() 
                    log(f"  [Scheduling] Dequeued {wl['id']} -> Assign to {selected_link} @ {tx_power} dBm / {chosen_bw} Mbps (Mode: {wl['mode']})")
                    
                    apply_link_config(selected_link, tx_power)
                    exec_id = start_transmission(selected_link, wl['size_mb'], wl['mode'], chosen_bw)
                    
                    if exec_id:
                        active_tasks[wl['id']] = {
                            'link': selected_link,
                            'initial_state': state_vec, 
                            'action_idx': link_idx,
                            'action_power': tx_power,
                            'action_bw': chosen_bw,
                            'start_ts': time.time(),
                            'exec_id': exec_id,
                            'mode': wl['mode'],
                            'last_log_time': time.time()
                        }
                        log(f"  [Active] Task {wl['id']} (Exec ID: {exec_id}) active.")
                    else:
                        log(f"  [ERROR] Failed to start task {wl['id']}. It will be dropped.")

        time.sleep(0.1)

if __name__ == "__main__":
    # Start Control Loop in a separate thread
    control_thread = threading.Thread(target=control_loop, daemon=True)
    control_thread.start()
    
    # Start API Server on the main thread
    run_api_server()