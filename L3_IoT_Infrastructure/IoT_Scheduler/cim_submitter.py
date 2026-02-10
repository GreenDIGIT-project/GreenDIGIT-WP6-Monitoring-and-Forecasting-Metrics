import json
import requests
from token_client import TokenClient
# Assuming this handles the division logic; if not, we can do it inline
from submit_metric import compute_work_bits_per_joule 
from config import settings


#NETWORKING CIM SCHEMA
# # common fields proposed to be stored on the Facts table
# Site - site name from GOCDB (for network, if necessary, create site in GreenDIGIT project in GOCDB)
# Energy_wh - Energy consumed by exec unit (wh)
# Work - (AmountOfDataTransferred / Energy_wh) ## quantity of specific work per energy unit -- !!! (tx_bytes + rx_bytes) / total_energy_Wh
# StartExecTime - 
# StopExecTime - 
# Status - (done / running / failed / ?)
# Owner - ## a VO registered in EGI Operations Portal, it applies for grid and cloud;
# ## for network one/several VOs could be created or predefined names can be used without need to register in the Ops Portal
# ExecUnitID - ## the unique identifier of each exec unit
# ExecUnitFinished - 0 ('no') or 1 ('yes') ## for '1' the Status can be 'done' or 'failed'


# # NETWORK - additional fields in specific detail_network table
# AmountOfDataTransferred - number of bytes (bytes) -- !!! (Bytes RX)
# NetworkType - type of network resources used (e.g. IoT, 5G etc)
# MeasurementType - type for measurement approach (direct or energy consumption estimation)
# DestinationExecUnitID



def submit_to_cim(prediction: dict):
    """
    Maps the workload prediction result to the CIM payload and submits it.
    Follows the Facts and detail_network table schema.
    """
    try:
        # 1. Extract data and convert to correct units (Bytes)
        energy_results = prediction.get("energy_results", {})
        total_energy_wh = energy_results.get("total_energy_Wh", 0.0)
        
        # Schema requires Bytes: MB * 1024 * 1024
        amount_mb = energy_results.get("MB", 0.0)
        amount_bytes = int(amount_mb * 1024 * 1024)

        # Build CIM Payload following the exact schema
        payload = {
            # --- Facts Table Fields ---
            "Site": "SLICES-GR-UTH",
            "Energy_wh": total_energy_wh,
            "Work": prediction.get("work_bytes_per_wh"),
            "StartExecTime": prediction.get("start_time"),
            "StopExecTime": prediction.get("end_time"),
            "Status": "done",
            "Owner": "vo:network-ops",
            "ExecUnitID": prediction.get("exec_unit_id"),
            "ExecUnitFinished": 1,
            
            # --- detail_network Table Fields ---
            "detail_network": {
                "AmountOfDataTransferred": amount_bytes,
                "NetworkType": "IoT",
                "MeasurementType": "energy consumption estimation",
                "DestinationExecUnitID": prediction.get("exec_unit_id")
            }
        }

        # 4. Authentication and Submission
        client = TokenClient()
        token = client.get_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"{client.token_type} {token}"
        }

        r = requests.post(
            settings.submit_url, 
            json=payload, 
            headers=headers, 
            verify=settings.verify_tls,
            timeout=settings.timeout_seconds
        )
        print(r)

        if r.status_code < 300:
            print(f"Successfully submitted {prediction['exec_unit_id']} to CIM.")
            return True
        else:
            print(f"CIM Submission Failed ({r.status_code}): {r.text}")
            return False

    except Exception as e:
        print(f"Error in CIM submission: {e}")
        return False