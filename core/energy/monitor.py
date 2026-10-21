import subprocess
import time
import re

# Function to run tegrastats and extract CPU, GPU, and SOC power consumption in real-time
def get_power_usage():
    try:
        # Run tegrastats with the specified interval, capture the output in real-time
        result = subprocess.Popen(['sudo', 'tegrastats', '--interval', '1000'], 
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        while True:
            # Read output line by line in real-time
            line = result.stdout.readline()
            if line == '':
                break

            # Regex patterns to match power consumption values in watts (mW / W)
            vdd_in_pattern = re.compile(r'VDD_IN (\d+)mW')  # VDD_IN total power consumption
            vdd_cpu_gpu_pattern = re.compile(r'VDD_CPU_GPU_CV (\d+)mW')  # Power for CPU + GPU
            vdd_soc_pattern = re.compile(r'VDD_SOC (\d+)mW')  # Power for the SOC

            # Search for power values in the output line
            vdd_in = re.search(vdd_in_pattern, line)
            vdd_cpu_gpu = re.search(vdd_cpu_gpu_pattern, line)
            vdd_soc = re.search(vdd_soc_pattern, line)
            
            # Extract the power consumption values and convert to watts (1mW = 0.001W)
            vdd_in_val = f"{int(vdd_in.group(1)) / 1000:.3f} W" if vdd_in else "Not available"
            vdd_cpu_gpu_val = f"{int(vdd_cpu_gpu.group(1)) / 1000:.3f} W" if vdd_cpu_gpu else "Not available"
            vdd_soc_val = f"{int(vdd_soc.group(1)) / 1000:.3f} W" if vdd_soc else "Not available"

            # Print the results in real-time
            print(f"Total Power (VDD_IN): {vdd_in_val}")
            print(f"CPU + GPU Power (VDD_CPU_GPU_CV): {vdd_cpu_gpu_val}")
            print(f"SOC Power (VDD_SOC): {vdd_soc_val}")
            print("-------------------------------")

            # Sleep for 1 second before fetching the next data point
            time.sleep(1)

    except Exception as e:
        print(f"Error fetching power usage: {e}")

# Main function to continuously monitor power usage in real-time
if __name__ == "__main__":
    try:
        get_power_usage()
    except KeyboardInterrupt:
        print("Monitoring stopped.")
