# make_data.py
import argparse, os, sys, glob
import pandas as pd  # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from config import RESULTS_DIR, DATA_DIR
from shared_utils.log_utils import info, success, warning, error, debug


# --------------------------- Helpers ---------------------------

def safe_read_csv(path: str):
    try:
        df = pd.read_csv(path)
        debug(f"[read_csv] {path} -> rows={len(df)}, cols={list(df.columns)}")
        if not df.empty:
            debug(f"[head|{path}]\n{df.head(5).to_string(index=False)}")
        return df
    except Exception as e:
        warning(f"[read_csv] skip {path}: {e}")
        return None

def check_empty_columns(df: pd.DataFrame, context: str):
    """Warning για στήλες που είναι όλες NaN ή κενά strings."""
    empty_cols = []
    for col in df.columns:
        s = df[col]
        if s.dtype == object:
            nonempty = s.astype(str).str.strip().replace({"": pd.NA}).dropna()
            has_data = not nonempty.empty
        else:
            has_data = s.notna().any()
        if not has_data:
            empty_cols.append(col)
    if empty_cols:
        warning(f"[empty-columns|{context}] {len(empty_cols)} empty: {empty_cols}")
    else:
        debug(f"[empty-columns|{context}] none")

def normalize_energy(df: pd.DataFrame):
    """
    Κάνει:
      - energy_per_bit -> numeric (coerce), round(12)
      - energy_per_bit_uJ = 1e6 * energy_per_bit (round(6) για πρακτικότητα)
    """
    if "energy_per_bit" in df.columns:
        num = pd.to_numeric(df["energy_per_bit"], errors="coerce").round(12)
        df["energy_per_bit"] = num
        df["energy_per_bit_uJ"] = (1e6 * num).round(6)
        debug("[transform] energy_per_bit rounded to 12dp; added energy_per_bit_uJ = 1e6 * energy_per_bit")
    else:
        debug("[transform] 'energy_per_bit' not found; skip energy transforms")
    return df

def process_df(df: pd.DataFrame, context: str) -> pd.DataFrame:
    """Ενιαίο pipeline (train & test): empty-check -> normalize -> empty-check -> debug head."""
    df = df.copy()
    check_empty_columns(df, context=f"{context}:pre")
    df = normalize_energy(df)
    check_empty_columns(df, context=f"{context}:post")
    if not df.empty:
        debug(f"[{context} head]\n{df.head(5).to_string(index=False)}")
    return df

def write_by_scenario(df: pd.DataFrame, out_map: dict):
    """
    Γράφει αρχεία ανά scenario.
    out_map π.χ.: {'idle': 'idle.csv', 'tx': 'tx.csv', 'rx': 'rx.csv'}
    """
    if "scenario" not in df.columns:
        error("[write_by_scenario] 'scenario' column is required.")
        sys.exit(1)

    for scen, out_name in out_map.items():
        part = df[df["scenario"].astype(str).str.lower() == scen].copy()
        if part.empty:
            warning(f"[{scen}] No rows; skip write.")
            continue
        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, out_name)
        part.to_csv(path, index=False)
        success(f"[write] {path} rows={len(part)}")
        debug(f"[{scen} head]\n{part.head(5).to_string(index=False)}")


# --------------------------- Modes ---------------------------

def run_train_mode():
    # IDLE (πρώτο idle αρχείο, αν υπάρχει) — DROP αν ΔΕΝ έχει 'scenario'
    idle_candidates = sorted(glob.glob(os.path.join(RESULTS_DIR, "*idle_baseline*.csv")))
    idle_df_list = []
    if idle_candidates:
        idle_path = idle_candidates[0]
        df = safe_read_csv(idle_path)
        if df is None or df.empty:
            warning("[train-idle] No readable rows in idle baseline file.")
        elif "scenario" not in df.columns:
            warning(f"[train-idle] drop file without 'scenario': {idle_path}")
        else:
            idle_df_list.append(process_df(df, context=f"idle:{os.path.basename(idle_path)}"))
    else:
        warning("[train-idle] No idle baseline CSV found.")

    # SWEEPS (όλα) — DROP όσα ΔΕΝ έχουν 'scenario'
    sweep_files = sorted(glob.glob(os.path.join(RESULTS_DIR, "*sweep_*.csv")))
    sweep_df_list = []
    if not sweep_files:
        warning("[train-sweeps] No sweep CSVs found.")
    else:
        for path in sweep_files:
            df = safe_read_csv(path)
            if df is None or df.empty:
                warning(f"[train-sweeps] skip empty/None: {path}")
                continue
            if "scenario" not in df.columns:
                warning(f"[train-sweeps] drop file without 'scenario': {path}")
                continue
            sweep_df_list.append(process_df(df, context=f"sweep:{os.path.basename(path)}"))

    if not idle_df_list and not sweep_df_list:
        warning("[train] Nothing to write.")
        return

    full = pd.concat(idle_df_list + sweep_df_list, ignore_index=True)
    write_by_scenario(full, out_map={"idle": "idle.csv", "tx": "tx.csv", "rx": "rx.csv"})

def run_test_mode():
    # Συλλογή test groups — DROP όσα αρχεία ΔΕΝ έχουν 'scenario'
    groups = [
        ("*testset_idle*.csv", "idle"),
        ("*testset_tx*.csv",   "tx"),
        ("*testset_rx*.csv",   "rx"),
    ]
    dfs = []
    for pattern, scen in groups:
        files = sorted(glob.glob(os.path.join(RESULTS_DIR, pattern)))
        if not files:
            warning(f"[test] No files for pattern: {pattern}")
            continue
        for path in files:
            df = safe_read_csv(path)
            if df is None or df.empty:
                warning(f"[test] skip empty/None: {path}")
                continue
            if "scenario" not in df.columns:
                warning(f"[test] drop file without 'scenario': {path}")
                continue
            # Αν έχει 'scenario' αλλά με κενές τιμές, ΓΕΜΙΣΕ τες με την προσδοκώμενη (idle/tx/rx) μόνο για test
            df["scenario"] = df["scenario"].fillna(scen)
            dfs.append(process_df(df, context=f"test:{scen}:{os.path.basename(path)}"))

    if not dfs:
        warning("[test] Nothing to write.")
        return

    full = pd.concat(dfs, ignore_index=True)
    write_by_scenario(full, out_map={"idle": "idle_test.csv", "tx": "tx_test.csv", "rx": "rx_test.csv"})


# --------------------------- CLI ---------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["train", "test", "both"], default="train",
        help="train: γράφει idle/tx/rx.csv | test: γράφει idle_test/tx_test/rx_test.csv | both: και τα δύο"
    )
    args = parser.parse_args()

    info(f"[MODE] {args.mode.upper()}")
    if args.mode in ("train", "both"):
        run_train_mode()
    if args.mode in ("test", "both"):
        run_test_mode()
    info("make_data.py finished.")

if __name__ == "__main__":
    main()
