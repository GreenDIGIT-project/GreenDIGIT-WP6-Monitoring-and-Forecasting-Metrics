# submit_metric.py
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from token_client import TokenClient
from config import settings

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def compute_work_bits_per_joule(amount_bits: Optional[float], energy_wh: Optional[float]) -> Optional[float]:
    """
    Work = AmountOfDataTransferred (bits) / Energy (Joules)
    1 Wh = 3600 J
    """
    if not amount_bits or not energy_wh or energy_wh <= 0:
        return None
    return amount_bits / (energy_wh * 3600.0)

def build_payload() -> Dict[str, Any]:
    """
    Matches the Execution Unit structure you provided.
    Common fields at top-level + a nested 'detail_network' block.
    Adjust values as needed.
    """
    # --- Common (Facts) ---
    CI_g = 0.0 #Provided by CIM
    CFP_g = 0.0 #Provided by CIM
    PUE = 1.7 #Default 1.7 (if not calculated), provided by CIM
    Site = "SLICES-GR-UTH"
    Energy_wh_common = 14.0
    StartExecTime = iso_now()
    time.sleep(0.01)  # simulate exec time
    StopExecTime = iso_now() #Provided by Custom Energy Extractor
    Status = "done" #Provided by Custom Energy Extractor  
    Owner = "vo:network-ops"
    ExecUnitID = "IoT-session-0002" 
    ExecUnitFinished = 1

    # --- detail_network ---
    Energy_wh_network = 40.0  # Wh
    AmountOfDataTransferred_bits = 8_000_000_000  # 8e9 bits (~1 GB)
    NetworkType = "IoT"
    MeasurementType = "energy consumption estimation"
    DestinationExecUnitID = "compute-job-33"

    Work_bits_per_joule = compute_work_bits_per_joule(
        amount_bits=AmountOfDataTransferred_bits,
        energy_wh=Energy_wh_network or Energy_wh_common
    )

    payload = {
        # common facts
        "CI_g": CI_g,
        "CFP_g": CFP_g,
        "PUE": PUE,
        "Site": Site,
        "Energy_wh": Energy_wh_common,
        "Work": Work_bits_per_joule,          # promoted value
        "StartExecTime": StartExecTime,
        "StopExecTime": StopExecTime,
        "Status": Status,
        "Owner": Owner,
        "ExecUnitID": ExecUnitID,
        "ExecUnitFinished": ExecUnitFinished,

        # network detail
        "detail_network": {
            "Energy_wh": Energy_wh_network,
            "AmountOfDataTransferred": AmountOfDataTransferred_bits,
            "Work": Work_bits_per_joule,      # same value for convenience
            "NetworkType": NetworkType,
            "MeasurementType": MeasurementType,
            "DestinationExecUnitID": DestinationExecUnitID,
        }
    }
    return payload

def main():
    # 1) Get token
    client = TokenClient()
    token = client.get_token()
    auth_header = {"Authorization": f"{client.token_type} {token}"}

    # 2) Submit payload
    payload = build_payload()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        **auth_header,
    }

    url = getattr(settings, "submit_url", f"{settings.base_url}{getattr(settings, 'submit_path', '/gd-cim-api/submit')}")
    with requests.Session() as s:
        s.verify = settings.verify_tls
        r = s.post(url, json=payload, headers=headers, timeout=settings.timeout_seconds)

    if r.status_code < 300:
        print("✅ Stored successfully")
        try:
            print("Response JSON:", r.json())
        except Exception:
            print("Response text:", r.text[:500])
    else:
        print(f"❌ Submit failed ({r.status_code})")
        print(r.text[:2000])

if __name__ == "__main__":
    main()
