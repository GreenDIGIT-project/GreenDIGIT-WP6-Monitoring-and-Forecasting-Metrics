from __future__ import annotations
import os
import numpy as np # type: ignore
import threading
import socket, fcntl, struct, time, subprocess, psutil
import multiprocessing
import re
from pathlib import Path

#from logging_utils import info, error, success, warning
#from sensors.shelly import detect_shelly_generation, get_power
#from utils.regression import remove_outliers_iqr
#from config import RASPBERRY_MAPPING 

# --- add in cpu.py ---

import shutil

def _read_temp_from_sysfs() -> float | None:
    try:
        p = Path("/sys/class/thermal")
        temps = []
        for z in p.glob("thermal_zone*/temp"):
            try:
                v = int(z.read_text().strip())
                # values can be in millidegC (RasPi) or degC. Assume mdegC if > 1000
                temps.append(v / 1000.0 if v > 1000 else float(v))
            except Exception:
                pass
        return float(np.mean(temps)) if temps else None
    except Exception:
        return None

def _parse_tegrastats_temp(out: str) -> float | None:
    # Accept integers or decimals, optional spaces
    m = re.search(r"CPU@\s*([\d]+(?:\.\d+)?)\s*C", out, re.IGNORECASE)
    return float(m.group(1)) if m else None

def read_cpu_temperature() -> float | None:
    dev = detect_device()
    try:
        if dev == "raspberrypi":
            t = _read_temp_from_sysfs()
            return t
        elif dev == "jetson":
            out = subprocess.check_output("sudo tegrastats --interval 4000 | head -n 1",
                                          shell=True).decode()
            return _parse_tegrastats_temp(out)
        elif dev == "upboard":
            # try lm-sensors first
            if shutil.which("sensors"):
                try:
                    out = subprocess.check_output(["sensors"], stderr=subprocess.DEVNULL).decode()
                    # Try common labels
                    for pat in [r"Package id 0:\s*\+([\d\.]+)°C", r"Tctl:\s*\+([\d\.]+)°C",
                                r"Tdie:\s*\+([\d\.]+)°C", r"Core\s*\d+:\s*\+([\d\.]+)°C"]:
                        m = re.search(pat, out)
                        if m:
                            return float(m.group(1))
                except Exception:
                    pass
            # fallback: sysfs
            return _read_temp_from_sysfs()
        else:
            return _read_temp_from_sysfs()
    except Exception as e:
        warning(f"read_cpu_temperature error: {e}")
        return None

def _parse_vcgencmd_volts(out: str) -> float | None:
    # e.g., "volt=0.8625V"
    m = re.search(r"volt=([\d\.]+)V", out)
    return float(m.group(1)) if m else None

def _parse_sensors_vcore(out: str) -> float | None:
    # Try common labels that often show core voltage
    for pat in [r"Vcore:\s*\+([\d\.]+)\s*V", r"VCCIN:\s*\+([\d\.]+)\s*V",
                r"Vcpu:\s*\+([\d\.]+)\s*V"]:
        m = re.search(pat, out, re.IGNORECASE)
        if m:
            return float(m.group(1))
    return None

def read_cpu_voltage() -> float | None:
    dev = detect_device()
    try:
        if dev == "raspberrypi":
            if shutil.which("vcgencmd"):
                out = subprocess.check_output(["vcgencmd", "measure_volts", "core"],
                                              stderr=subprocess.DEVNULL).decode()
                return _parse_vcgencmd_volts(out)
            return None
        elif dev == "jetson":
            # Not readily available via tegrastats; return None
            return None
        elif dev == "upboard":
            if shutil.which("sensors"):
                try:
                    out = subprocess.check_output(["sensors"], stderr=subprocess.DEVNULL).decode()
                    v = _parse_sensors_vcore(out)
                    return v
                except Exception:
                    return None
            return None
        else:
            return None
    except Exception as e:
        warning(f"read_cpu_voltage error: {e}")
        return None

def detect_device():
    """
    Detects if we are on Jetson (tegrastats), UPBoard (turbostat), or Raspberry Pi.
    """
    try:
        out = subprocess.check_output("uname -a", shell=True).decode().lower()
        if "tegra" in out:
            return "jetson"
        elif os.path.exists("/usr/bin/turbostat"):
            return "upboard"
        elif "raspberrypi" in out:
            return "raspberrypi"

        # Fallback check for Raspberry Pi
        if os.path.exists("/proc/cpuinfo"):
            with open("/proc/cpuinfo") as f:
                cpuinfo = f.read().lower()
                if "raspberry pi" in cpuinfo or "bcm" in cpuinfo:
                    return "raspberrypi"
    except:
        pass
    return None

def read_jetson_power_tool():
    """Διαβάζει στιγμιαία ισχύ CPU από tegrastats (mW → W)."""
    try:
        out = subprocess.check_output("sudo tegrastats --interval 4000 | head -n 1", shell=True).decode()
        m = re.search(r"VDD_CPU_GPU_CV\s+(\d+)mW", out) # type: ignore
        return int(m.group(1)) / 1000 if m else None
    except Exception as e:
        warning(f"tegrastats error: {e}")
        return None

def read_upboard_power_tool():
    """Διαβάζει PkgWatt από turbostat (1 δείγμα)."""
    try:
        output = subprocess.check_output(
            ["sudo", "turbostat", "--Summary", "--quiet", "--interval", "4", "--num", "1"],
            stderr=subprocess.DEVNULL
        )
        lines = output.decode().splitlines()
        summary = [l for l in lines if l.strip() and not l.startswith("Avg_MHz")][-1]
        return float(summary.split()[-1])  # PkgWatt
    except Exception as e:
        warning(f"turbostat error: {e}")
        return None

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(), 0x8915,
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )[20:24])
    except OSError:
        return None

def cpu_burn(run_time, idle_time, stop_event):
    while not stop_event.is_set():
        start = time.perf_counter()
        while time.perf_counter() - start < run_time:
            _ = 123456789 ** 0.5
        time.sleep(idle_time)

def stress_cpu_custom(percent):
    info(f"Stressing CPU at {percent}% load (custom burner)")
    if percent == 0:
        return None

    cpu_cores = os.cpu_count()
    run_time = percent / 100
    idle_time = 1 - run_time

    stop_event = multiprocessing.Event()
    processes = []
    for _ in range(cpu_cores):
        p = multiprocessing.Process(target=cpu_burn, args=(run_time, idle_time, stop_event))
        p.start()
        processes.append(p)

    return stop_event, processes

def stop_stress(stress_handler):
    if stress_handler:
        stop_event, processes = stress_handler
        stop_event.set()
        for p in processes:
            p.terminate()
            p.join()
###

def _read_all_scaling_cur_freq_khz():
    """Διάβασε scaling_cur_freq από όλους τους πυρήνες (kHz)."""
    vals = []
    for p in sorted(Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_cur_freq")):
        try:
            vals.append(int(p.read_text().strip()))
        except Exception:
            pass
    # Fallback: policy0 αν δεν υπάρχουν per-CPU αρχεία
    if not vals:
        pol = Path("/sys/devices/system/cpu/cpufreq/policy0/scaling_cur_freq")
        if pol.exists():
            try:
                vals.append(int(pol.read_text().strip()))
            except Exception:
                pass
    return vals

def get_avg_cpu_freq_khz(sample_duration, step=0.1):
    """Μέση συχνότητα (kHz) σε όλους τους πυρήνες εντός του window."""
    import time
    end = time.time() + sample_duration
    bucket = []
    while time.time() < end:
        vals = _read_all_scaling_cur_freq_khz()
        if vals:
            bucket.append(sum(vals) / len(vals))
        time.sleep(step)
    return round(sum(bucket) / len(bucket), 2) if bucket else None
# ===

def collect_cpu_and_power_shelly_and_freq(duration, shelly_ip, shelly_gen):
    result = {}

    def collect_cpu(result_holder):
        result_holder["cpu"] = get_cpu_utilization(duration)

    def collect_power(result_holder):
        result_holder["power"] = get_power(shelly_ip, shelly_gen)

    def collect_freq(result_holder):
        result_holder["freq_khz"] = get_avg_cpu_freq_khz(duration, step=0.1)

    t1 = threading.Thread(target=collect_cpu, args=(result,))
    t2 = threading.Thread(target=collect_power, args=(result,))
    t3 = threading.Thread(target=collect_freq, args=(result,))

    t1.start(); t2.start(); t3.start()
    t1.join();  t2.join();  t3.join()

    temp_c = read_cpu_temperature()
    volt_v = read_cpu_voltage()

    return result.get("cpu"), result.get("power"), result.get("freq_khz"), temp_c, volt_v

# Measure CPU utilization percentage over a sample duration
def get_cpu_utilization(sample_duration):
    def read_cpu_times():
        with open('/proc/stat') as f:
            fields = list(map(int, f.readline().split()[1:]))
            idle = fields[3] + fields[4]
            total = sum(fields)
            return idle, total

    idle1, total1 = read_cpu_times()
    time.sleep(sample_duration)
    idle2, total2 = read_cpu_times()
    usage = 100.0 * (total2 - total1 - (idle2 - idle1)) / (total2 - total1) if total2 - total1 > 0 else 0
    return round(usage, 2)

def get_avg_cpu_temp_c(duration: float, step: float = 0.2):
    end = time.time() + duration
    vals = []
    while time.time() < end:
        v = read_cpu_temperature()
        if v is not None:
            vals.append(float(v))
        time.sleep(step)
    return (sum(vals) / len(vals)) if vals else None

def get_avg_cpu_volt_v(duration: float, step: float = 0.2):
    end = time.time() + duration
    vals = []
    while time.time() < end:
        v = read_cpu_voltage()
        if v is not None:
            vals.append(float(v))
        time.sleep(step)
    return (sum(vals) / len(vals)) if vals else None

def collect_idle_power_freq(n_samples, sample_duration, shelly_ip, shelly_gen):
    import statistics, time
    from threading import Thread

    powers, freqs = [], []

    try:
        stop_stress(None)
    except Exception:
        pass

    for i in range(n_samples):
        start = time.time()
        result = {}

        def _power():
            result["p"] = get_power(shelly_ip, shelly_gen)

        def _freq():
            result["f"] = get_avg_cpu_freq_khz(sample_duration, step=0.1)

        # ΝΕΟ: windowed sampling για Temp/Volt
        def _temp():
            result["t"] = get_avg_cpu_temp_c(sample_duration, step=0.2)

        def _volt():
            result["v"] = get_avg_cpu_volt_v(sample_duration, step=0.2)

        t1 = Thread(target=_power); t2 = Thread(target=_freq)
        t3 = Thread(target=_temp);  t4 = Thread(target=_volt)
        t1.start(); t2.start(); t3.start(); t4.start()
        t1.join();  t2.join();  t3.join();  t4.join()

        if result.get("p") is None:
            warning("Idle sample skipped: power read failed")
            continue

        powers.append(result["p"])
        if result.get("f") is not None:
            freqs.append(result["f"])

        temp_c = result.get("t")
        volt_v = result.get("v")

        info(
            f"Idle sample {i+1}/{n_samples}: "
            f"P={result['p']:.4f} W | "
            f"Freq={(result['f']/1000.0 if result.get('f') else 0):.0f} MHz | "
            f"Temp={f'{temp_c:.1f}°C' if temp_c is not None else 'N/A'} | "
            f"Volt={f'{volt_v:.3f}V' if volt_v is not None else 'N/A'}"
        )

        # μικρό κενό
        rem = sample_duration - (time.time() - start)
        if rem > 0:
            time.sleep(min(rem, 0.1))

    if not powers:
        raise RuntimeError("Idle power collection failed: no samples")

    # p_idle = float(np.percentile(powers, 5))
    p_idle = statistics.mean(powers)
    f_idle = statistics.mean(freqs) if freqs else None
    return p_idle, f_idle

def collect_samples_freq(cpu_levels, samples_per_level, sample_duration, tolerance, shelly_ip, shelly_gen):
    X_all, y_all, f_all = [], [], []
    t_all, v_all = [], []
    stats_per_level = {}

    for level in cpu_levels:
        time.sleep(1)
        info(f"→ Target CPU load: {level}% | Target samples: {samples_per_level}")
        stress = stress_cpu_custom(level)
        time.sleep(3)

        collected, attempts = 0, 0
        temp_cpu, temp_power, temp_freq = [], [], []
        temp_temp, temp_volt = [], []

        while collected < samples_per_level and attempts < samples_per_level * 3:
            attempts += 1
            cpu, power, freq_khz, temp_c, volt_v = collect_cpu_and_power_shelly_and_freq(sample_duration, shelly_ip, shelly_gen)
            
            if power is None:
                warning("Skipped: power read failed")
                continue

            info(
                f"[Level {level}] Sample {collected+1}/{samples_per_level} | "
                f"CPU={cpu:.2f}% | Power={power:.2f}W | "
                f"Freq={(freq_khz/1000 if freq_khz else 0):.0f}MHz | "
                f"Temp={f'{temp_c:.1f}°C' if temp_c is not None else 'N/A'} | "
                f"Volt={f'{volt_v:.3f}V' if volt_v is not None else 'N/A'}"
            )

            if level - tolerance <= cpu <= level + tolerance:
                temp_cpu.append(cpu)
                temp_power.append(power)
                temp_freq.append(freq_khz if freq_khz is not None else 0)
                temp_temp.append(temp_c if temp_c is not None else np.nan)
                temp_volt.append(volt_v if volt_v is not None else np.nan)
                collected += 1
            else:
                warning(f"Rejected: CPU={cpu:.2f}%")
            
        if stress:
            stop_stress(stress)

        if temp_cpu:
            cpu_arr = np.array(temp_cpu)
            power_arr = np.array(temp_power)
            filtered_cpu, filtered_power = remove_outliers_iqr(cpu_arr, power_arr, 1.0)

            # μάσκα outliers και για freq
            keep_idx = [i for i, (c,p) in enumerate(zip(cpu_arr, power_arr)) 
                        if (c in set(filtered_cpu)) and (p in set(filtered_power))]
            filtered_freq = np.array(temp_freq)[keep_idx]
            filtered_temp = np.array(temp_temp)[keep_idx]
            filtered_volt = np.array(temp_volt)[keep_idx]

            X_all.extend(filtered_cpu.tolist())
            y_all.extend(filtered_power.tolist())
            f_all.extend(filtered_freq.tolist())
            t_all.extend(filtered_temp.tolist())
            v_all.extend(filtered_volt.tolist())

            if len(filtered_power) > 0:
                avg_p = float(np.mean(filtered_power))
                avg_f = float(np.mean(filtered_freq)) if len(filtered_freq) > 0 else None
                avg_t = float(np.nanmean(filtered_temp)) if filtered_temp.size else None
                avg_v = float(np.nanmean(filtered_volt)) if filtered_volt.size else None
                stats_per_level[int(level)] = {
                    "avg_power_w": avg_p,
                    "avg_freq_khz": float(avg_f) if avg_f is not None else None,
                    "avg_temp_c": float(avg_t) if avg_t is not None else None,
                    "avg_volt_v": float(avg_v) if avg_v is not None else None,
                    "n": int(len(filtered_cpu)),
                }
        else:
            warning(f"No valid samples collected at level {level}")

    # ΠΑΝΤΑ επιστρέφει 4 τιμές
    return np.array(X_all), np.array(y_all), np.array(f_all), np.array(v_all), np.array(t_all), stats_per_level




