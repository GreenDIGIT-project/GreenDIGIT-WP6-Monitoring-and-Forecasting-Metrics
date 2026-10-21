import importlib
import traceback

MODULES = [
    "scheduler_overlay",
    "feature_builder",
    "candidate_generator",
    "queue_state_adapter",
]

print("\n===== SAFE IMPORT CHECK =====\n")

for m in MODULES:
    try:
        importlib.import_module(m)
        print(f"[OK] {m}")
    except Exception as e:
        print(f"\n[FAIL] {m}")
        print("Error:", repr(e))
        print(traceback.format_exc())