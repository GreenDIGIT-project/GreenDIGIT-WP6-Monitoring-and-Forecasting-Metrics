# utils/link_capacity.py
import csv, math
from collections import defaultdict

COL_TXP      = "tx_power"
COL_RSSI     = "rssi"
COL_THR_TX   = "throughput_mbps_tx"
COL_THR_RX   = "throughput_mbps_rx"

def _nearest_rank_quantile(xs, q=0.95):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = max(1, math.ceil(q * len(xs))) - 1
    return xs[k]

def _q95_group_by(csv_path, key_col, thr_col):
    groups = defaultdict(list)
    with open(csv_path, newline="") as f:
        r = csv.DictReader(f)
        
        # Peek χωρίς να χαθεί το dataset: διάβασέ το σε λίστα
        rows = list(r)

        print(f"[DEBUG] Διαβάζουμε από: {csv_path}")
        print(f"[DEBUG] Περιμένουμε key_col='{key_col}', thr_col='{thr_col}'")
        for i, row in enumerate(rows[:5]):
            print(f"[DEBUG] Row {i}: {key_col}={row.get(key_col)}, {thr_col}={row.get(thr_col)}")

        # Τώρα κάνε processing πάνω στη λίστα
        for row in rows:
            try:
                key = float(row[key_col])
                thr = float(row[thr_col])
                if math.isfinite(key) and math.isfinite(thr) and thr > 0:
                    groups[key].append(thr)
            except Exception:
                continue
    return {k: _nearest_rank_quantile(v) for k, v in groups.items() if v}

def _interp(peaks, x):
    if not peaks:
        return float("nan")
    keys = sorted(peaks.keys())
    x = float(x)
    if x in peaks:
        return peaks[x]
    if x <= keys[0]:
        return peaks[keys[0]]
    if x >= keys[-1]:
        return peaks[keys[-1]]
    lo = max(k for k in keys if k <= x)
    hi = min(k for k in keys if k >= x and k != lo)
    y_lo, y_hi = peaks[lo], peaks[hi]
    w = (x - lo) / (hi - lo)
    return y_lo + w * (y_hi - y_lo)

class LinkCapacityProvider:
    """
    Υπολογίζει q95 ("peak") throughput:
      - για TX με βάση το TX power (dBm) από το tx_csv
      - για RX με βάση το RSSI (dBm) από το rx_csv
    """
    def __init__(self, tx_csv="data/tx.csv", rx_csv="data/rx.csv"):
        # TX: group by TX power
        self.tx_peaks = _q95_group_by(tx_csv, COL_TXP, COL_THR_TX)
        # RX: group by RSSI
        self.rx_peaks = _q95_group_by(rx_csv, COL_RSSI, COL_THR_RX)

    def capacity(self, direction, value_dbm):
        """
        direction: 'tx' -> value_dbm = TX power (dBm)
                   'rx' -> value_dbm = RSSI (dBm)
        επιστρέφει: q95 throughput (Mbps) με γραμμική παρεμβολή όπου χρειάζεται
        """
        d = direction.lower()
        if d == "tx":
            return _interp(self.tx_peaks, float(value_dbm))
        elif d == "rx":
            return _interp(self.rx_peaks, float(value_dbm))
        else:
            raise ValueError("direction must be 'tx' or 'rx'")
