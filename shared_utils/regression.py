import numpy as np, os, json, csv, pandas as pd # type: ignore
import sys
sys.path.append('/home/pi/greendigit-l3-iot-uth-deployment')
from shared_utils.log_utils import info, success, warning

# Filter outliers using IQR method
def remove_outliers_iqr(x, y, threshold):
    q1, q3 = np.percentile(y, 25), np.percentile(y, 75)
    iqr = q3 - q1
    mask = (y >= q1 - threshold * iqr) & (y <= q3 + threshold * iqr)
    return x[mask], y[mask]

# Fit linear or quadratic regression model
def fit_linear_regression(X, y):
    # Fit linear model
    a, b = np.polyfit(X, y, 1)
    info(f"Linear regression selected (a = {a:.4f}, b = {b:.4f})")
    return round(a, 4), round(b, 4)

# Save regression model to JSON
def save_model(a, b, min_power, path):
    with open(path, "w") as f:
        json.dump({"a": a, "b": b, "p_other": min_power}, f, indent=4)
    success(f"Model saved to: {path}")

def fit_through_origin(X, y):
    """
    Least-squares παλινδρόμηση χωρίς σταθερά (μέσω (0,0)):
    α = (Σ x_i y_i) / (Σ x_i^2)
    """
    denom = float(np.dot(X, X))
    a = float(np.dot(X, y) / denom) if denom > 0 else 0.0
    info(f"Origin-fit selected (a = {a:.4f})")
    return round(a, 4)