import random
import time
import requests
import argparse
from datetime import datetime, timezone

# --- Configuration ---
MIN_DATA = 40
MAX_DATA = 50
FIXED_BW = "2.4M"  # Σταθερό Bandwidth σύμφωνα με τις οδηγίες

def send_workload(mode, api_url, noise_std):
    """
    Στέλνει ένα τυχαίο workload (μαζί με το noise_std) και επιστρέφει τον εκτιμώμενο χρόνο
    στον οποίο αναμένεται να ολοκληρωθεί πλήρως στο σύστημα (time.time()).
    """
    data_mb = random.randint(MIN_DATA, MAX_DATA)
    data_str = f"{data_mb}M"
    start_now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    payload = {
        "bandwidth": FIXED_BW,
        "data_amount": data_str,
        "start_time": start_now,
        "mode": mode,
        "noise_std": noise_std  # <--- ΠΡΟΣΘΗΚΗ ΤΟΥ ΘΟΡΥΒΟΥ ΣΤΟ PAYLOAD
    }
    
    print(f"  -> [ΣΤΑΛΘΗΚΕ] Data: {data_str} | BW: {FIXED_BW} | Mode: {mode.upper()} | Noise: {noise_std}")
    try:
        r = requests.post(api_url, json=payload, timeout=10)
        if r.status_code != 200:
            print("     [Error Response]:", r.text)
    except Exception as e:
        print(f"     [Connection Error]: {e}")
        
    expected_tx_time = (data_mb * 8) / 1.2
    overhead = 50.0 
    return time.time() + expected_tx_time + overhead


def run_constant_active_phase(target_active, total_workloads, mode, api_url, noise_std):
    print(f"\n=== ΦΑΣΗ: {target_active} ΕΝΕΡΓΑ WORKLOADS ΤΑΥΤΟΧΡΟΝΑ ===")
    workloads_sent = 0
    active_finish_times = []
    
    while workloads_sent < total_workloads or len(active_finish_times) > 0:
        while len(active_finish_times) < target_active and workloads_sent < total_workloads:
            ft = send_workload(mode, api_url, noise_std)
            active_finish_times.append(ft)
            workloads_sent += 1
            time.sleep(0.5) 
            
        if not active_finish_times:
            break
            
        earliest_finish = min(active_finish_times)
        now = time.time()
        if earliest_finish > now:
            time.sleep(earliest_finish - now)
            
        now = time.time()
        active_finish_times = [ft for ft in active_finish_times if ft > now]


def run_random_phase(total_workloads, mode, api_url, noise_std):
    print(f"\n=== ΦΑΣΗ: RANDOM (Εκκίνηση με 3, δημιουργία 0,1,2 μετά από κάθε λήξη) ===")
    workloads_sent = 0
    active_finish_times = []
    
    initial = min(3, total_workloads)
    for _ in range(initial):
        active_finish_times.append(send_workload(mode, api_url, noise_std))
        workloads_sent += 1
        time.sleep(0.5)
        
    while workloads_sent < total_workloads or len(active_finish_times) > 0:
        if not active_finish_times and workloads_sent < total_workloads:
            active_finish_times.append(send_workload(mode, api_url, noise_std))
            workloads_sent += 1
            time.sleep(0.5)
            
        if not active_finish_times:
            break
            
        earliest = min(active_finish_times)
        now = time.time()
        if earliest > now:
            time.sleep(earliest - now)
            
        now = time.time()
        finished_count = len([ft for ft in active_finish_times if ft <= now])
        active_finish_times = [ft for ft in active_finish_times if ft > now]
        
        for _ in range(finished_count):
            if workloads_sent >= total_workloads:
                break
            
            if len(active_finish_times) == 0:
                spawns = random.choice([1, 2])
            else:
                spawns = random.choice([0, 1, 2])
                
            spawns = min(spawns, total_workloads - workloads_sent) 
            for _ in range(spawns):
                active_finish_times.append(send_workload(mode, api_url, noise_std))
                workloads_sent += 1
                time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="IoT Workload Generator - Complete Training Curriculum")
    parser.add_argument("--port", type=str, default="8899", help="Η πόρτα του Controller (default: 8899)")
    parser.add_argument("--mode", type=str, choices=['train', 'best'], default='train', help="Επιλογή mode: 'train' ή 'best'. Default: 'train'")
    parser.add_argument("--total", type=int, default=15, help="Συνολικά workloads ανά Φάση. Default: 15")
    parser.add_argument("--repeat", type=int, default=1, help="Πόσες φορές θα επαναληφθεί ολόκληρος ο κύκλος εκπαίδευσης. Default: 1")
    parser.add_argument("--noise", type=float, default=0.15, help="Ένταση Γκαουσιανού Θορύβου (προεπιλογή 0.15)") # <--- ΝΕΑ ΠΑΡΑΜΕΤΡΟΣ
    args = parser.parse_args()
    
    selected_mode = args.mode
    total_wls = args.total
    repeat_cycles = args.repeat
    noise_val = args.noise
    
    # ΑΛΛΑΓΗ: Νέα διεύθυνση IP του Controller
    api_url = f"http://localhost:{args.port}/execute"

    print(f"=========================================================")
    print(f"  ΕΝΑΡΞΗ ΚΥΚΛΟΥ ΕΚΠΑΙΔΕΥΣΗΣ (CURRICULUM TRAINING)        ")
    print(f"  Target API       : {api_url}")
    print(f"  Mode             : {selected_mode.upper()} (Noise: {noise_val})")
    print(f"  Workloads / Φάση : {total_wls}")
    print(f"  Συνολικοί Κύκλοι : {repeat_cycles}")
    print(f"=========================================================")

    for cycle in range(1, repeat_cycles + 1):
        print(f"\n>>>>>>>>>>>> ΚΥΚΛΟΣ {cycle}/{repeat_cycles} <<<<<<<<<<<<")
        
        for active_target in range(1, 2):
            run_constant_active_phase(active_target, total_wls, selected_mode, api_url, noise_val)
            
        # run_random_phase(total_wls, selected_mode, api_url, noise_val)
        
        if cycle < repeat_cycles:
            print("\n[INFO] Ο κύκλος ολοκληρώθηκε. Αναμονή 15 δευτερόλεπτα για αποσυμφόρηση δικτύου...")
            time.sleep(15)

    print(f"\n✅ Η εκπαίδευση ολοκληρώθηκε επιτυχώς!")

if __name__ == "__main__":
    main()