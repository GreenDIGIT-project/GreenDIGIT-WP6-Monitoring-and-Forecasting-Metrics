import os
import pandas as pd
import numpy as np

# Try importing matplotlib for plotting; fall back gracefully if not available
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Notice: 'matplotlib' library not found. Text metrics will print, but plots won't be saved.")
    print("To enable plots, run: pip install matplotlib\n")

def analyze_file(filepath):
    if not os.path.exists(filepath):
        print(f"Error: The file '{filepath}' does not exist. Please check the name and try again.\n")
        return

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Error reading file: {e}\n")
        return

    # Clean column names just in case there are leading/trailing spaces
    df.columns = [c.strip() for c in df.columns]

    # Validate that required columns exist (added required metrics for reconstruction)
    required_cols = [
        'throughput_efficiency', 'real_energy_consumed', 'achieved_data_mb', 
        'start_time', 'src_node', 'dst_node', 'achieved_duration_s', 'requested_bandwidth'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing expected columns in the CSV: {missing_cols}\n")
        return

    print("=" * 60)
    print(f" ANALYSIS FOR FILE: {filepath}")
    print("=" * 60)

    # ----------------------------------------------------
    # Metric 1: Average Throughput Efficiency (With Recovery Fix)
    # ----------------------------------------------------
    # Calculate fallback throughput (Mbps) = (MB * 8) / seconds
    df['calculated_throughput'] = np.where(
        df['achieved_duration_s'] > 0,
        (df['achieved_data_mb'] * 8) / df['achieved_duration_s'],
        0
    )
    
    # Calculate fallback efficiency = calculated throughput / requested bandwidth
    df['calculated_efficiency'] = np.where(
        df['requested_bandwidth'] > 0,
        df['calculated_throughput'] / df['requested_bandwidth'],
        0
    )
    
    # Use original value unless it is 0; then plug in reconstructed value
    df['recovered_efficiency'] = np.where(
        df['throughput_efficiency'] != 0,
        df['throughput_efficiency'],
        df['calculated_efficiency']
    )

    # --- VERSION A: With Recovery Fix (Your current logic) ---
    # Filter out entries that remain absolutely zero (true transmission failures)
    valid_te = df[df['recovered_efficiency'] > 0]['recovered_efficiency']
    
    if not valid_te.empty:
        avg_throughput_eff = valid_te.mean()
        print(f"1a) Average Throughput Efficiency (with recovery): {avg_throughput_eff:.4f}")
        print(f"    (Calculated using {len(valid_te)} active records out of {len(df)} total rows)")
    else:
        print("1a) Average Throughput Efficiency (with recovery): No valid non-zero transmissions found.")

    # --- VERSION B: Ignoring original Zeros completely ---
    # Filter out rows where the ORIGINAL column was 0 (or less)
    unpatched_te = df[df['throughput_efficiency'] > 0]['throughput_efficiency']

    if not unpatched_te.empty:
        avg_unpatched_eff = unpatched_te.mean()
        print(f"1b) Average Throughput Efficiency (ignoring zeros): {avg_unpatched_eff:.4f}")
        print(f"    (Calculated using {len(unpatched_te)} original non-zero records out of {len(df)} total rows)")
    else:
        print("1b) Average Throughput Efficiency (ignoring zeros): No original non-zero records found.")

    # ----------------------------------------------------
    # Metric 2: Energy Efficiency (Joule/bit) & Total Energy
    # ----------------------------------------------------
    # Convert Wh to Joules
    df['energy_joules'] = df['real_energy_consumed'] * 3600
    
    # Calculate bits
    df['bits_decimal'] = df['achieved_data_mb'] * 1_000_000 * 8
    df['bits_binary'] = df['achieved_data_mb'] * 1024 * 1024 * 8

    # --- Calculation A: Average of Per-Transmission Ratios ---
    df['joule_per_bit_dec'] = np.where(df['bits_decimal'] > 0, df['energy_joules'] / df['bits_decimal'], 0)
    df['joule_per_bit_bin'] = np.where(df['bits_binary'] > 0, df['energy_joules'] / df['bits_binary'], 0)

    valid_data_dec = df[(df['bits_decimal'] > 0) & (df['real_energy_consumed'] > 0)]
    valid_data_bin = df[(df['bits_binary'] > 0) & (df['real_energy_consumed'] > 0)]

    avg_ee_dec = valid_data_dec['joule_per_bit_dec'].mean() if not valid_data_dec.empty else 0
    avg_ee_bin = valid_data_bin['joule_per_bit_bin'].mean() if not valid_data_bin.empty else 0

    # --- Calculation B: Volume-Weighted System Metric ---
    total_energy_j = df['energy_joules'].sum()
    total_energy_wh = df['real_energy_consumed'].sum()
    total_bits_dec = df['bits_decimal'].sum()
    total_bits_bin = df['bits_binary'].sum()

    system_ee_dec = (total_energy_j / total_bits_dec) if total_bits_dec > 0 else 0
    system_ee_bin = (total_energy_j / total_bits_bin) if total_bits_bin > 0 else 0

    print("\n2) Energy Efficiency & Consumption:")
    print(f"   Total Energy Consumed:                        {total_energy_wh:.4f} Wh ({total_energy_j:.2f} J)")
    print("   --- Per-Transmission Average (Unweighted) ---")
    print(f"   - Avg Energy Efficiency (Decimal MB):           {avg_ee_dec:.4e} Joule/bit")
    print(f"   - Avg Energy Efficiency (Binary MiB):           {avg_ee_bin:.4e} Joule/bit")
    print("   --- True System Efficiency (Volume-Weighted) ---")
    print(f"   - System Energy Efficiency (Decimal MB):        {system_ee_dec:.4e} Joule/bit")
    print(f"   - System Energy Efficiency (Binary MiB):        {system_ee_bin:.4e} Joule/bit")
    
    # ----------------------------------------------------
    # Metric 3: Data Distribution over Time
    # ----------------------------------------------------
    try:
        df['start_time'] = pd.to_datetime(df['start_time'])
        df_sorted = df.sort_values('start_time')
    except Exception as e:
        print(f"\n3) Error parsing start_time column: {e}")
        return

    print("\n3) Data Distribution Chronological Timeline (~30 entries):")
    for _, row in df_sorted.iterrows():
        time_str = row['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"   [{time_str}] {row['achieved_data_mb']:8.2f} MB  ({row['src_node']} -> {row['dst_node']})")

    # Generate and save a distribution chart to a designated folder
    if HAS_MATPLOTLIB and len(df_sorted) > 0:
        output_folder = "plots"
        os.makedirs(output_folder, exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(filepath))[0]
        plot_filepath = os.path.join(output_folder, f"distribution_{base_name}.png")
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.stem(df_sorted['start_time'], df_sorted['achieved_data_mb'], 
                linefmt='b-', markerfmt='bo', basefmt='r-')
        
        ax.set_title(f"Data Distribution Over Time ({base_name})")
        ax.set_xlabel("Start Time")
        ax.set_ylabel("Achieved Data (MB)")
        ax.grid(True, linestyle='--', alpha=0.6)
        plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
        
        fig.tight_layout()
        fig.savefig(plot_filepath, dpi=150)
        plt.close(fig)
        
        print(f"\n[Plot Saved] Chart cleanly exported to: {plot_filepath}")
    
    print("=" * 60 + "\n")


def main():
    print("Welcome to the Simulation Metrics Analyzer!")
    print("Type 'exit' or 'quit' at any time to close the program.\n")
    
    while True:
        user_input = input("Enter the CSV file name (e.g., data.csv): ").strip()
        
        if user_input.lower() in ['exit', 'quit', '']:
            print("Exiting application. Goodbye!")
            break
            
        analyze_file(user_input)

if __name__ == "__main__":
    main()