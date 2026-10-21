import requests
from datetime import datetime, timedelta, timezone

CARBON_API_URL = "https://api.wattnet.eu/v1/footprints"

SITE_LAT = 39.36590004168098
SITE_LON = 22.92380157490816


# -----------------------------
# HARD-CODE YOUR TOKEN HERE
# -----------------------------
raw_token = """eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJubkxEVEtJVkJBN3o5N3Zrbi1RWE5zQXFwSUNVQlQtam5fZ25mX2Z4TWdZIn0.eyJleHAiOjE3Nzc5MTk4MDcsImlhdCI6MTc3NzgzMzQwNywianRpIjoidHJydGNjOmFiZTRkM2U0LWUyOTYtZmMyYi00ZTcyLTllYzE5YzdiYzg3NiIsImlzcyI6Imh0dHBzOi8vYXV0aC53YXR0bmV0LmV1L3JlYWxtcy93YXR0bmV0IiwiYXVkIjpbIm9hdXRoMi1wcm94eSIsImFjY291bnQiXSwic3ViIjoiOTQ0NWUzODYtOGQzOS00ZDYxLTlmYzctZWEwMmIzNGY0MTEwIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoib2F1dGgyLXByb3h5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2FwaS53YXR0bmV0LmV1Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLXdhdHRwcmludCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiY2xpZW50SG9zdCI6IjE3Mi4xNi4zNS4xMjEiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtb2F1dGgyLXByb3h5IiwiY2xpZW50QWRkcmVzcyI6IjE3Mi4xNi4zNS4xMjEiLCJjbGllbnRfaWQiOiJvYXV0aDItcHJveHkifQ.t0aZrZs_IkgLY93GtQJL1Z9ESSn6VXHibEGZ6XldD0qI7ZJgIKxy8s5DrouW1FhTVRG6fsQOCwcXWERjYFifzwJNnPDGanNc0OEirdVL1z0J8LFv3t2bdx1nZCq0fyFJsEts018AmYKpaLCamLSeCIaH7jqehL7d8rFYscE-ED9-8BLYYNGGVEf0x-A4se2N-McVo_J5lbcnj-oSPifbmRBDo25Yj6PyS-7aDixGbaex31sjnhHub4JQC4wvq--8Dyzi8syfFmpL6g7e6Gsvqp8C61qCtyWbrD6TGzXwQD_7RTDpDbfk7Ke32FM-wHMRRDdfIW8P6MQipMieJD06-Q
"""

def format_iso(dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"

def get_carbon_intensity():
    # Clean token (important for pasted OAuth tokens)
    clean_token = raw_token.replace("\n", "").replace("\r", "").strip()

    headers = {
        "Authorization": f"Bearer {clean_token}"
    }

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(minutes=10)

    params = {
        "lat": SITE_LAT,
        "lon": SITE_LON,
        "footprint_type": "carbon",
        "scope": "life-cycle",
        "start": format_iso(start_utc),
        "end": format_iso(now_utc),
        "aggregate": True,
        "use_global": True
    }

    try:
        print("\n[CarbonAPI] Sending request...")

        response = requests.get(
            CARBON_API_URL,
            params=params,
            headers=headers,
            timeout=10
        )

        print("[CarbonAPI] Status code:", response.status_code)

        if response.status_code != 200:
            print("[CarbonAPI] ERROR RESPONSE:")
            print(response.text)
            return None

        data = response.json()

        print("[CarbonAPI] Raw response received")

        if not data:
            print("[CarbonAPI] Empty response")
            return None

        # Extract carbon intensity (first zone result)
        value = float(data[0]["value"])

        print("\n==============================")
        print("🌍 Carbon Intensity:", value, "gCO2/kWh")
        print(" Start:", format_iso(start_utc))
        print("==============================\n")
        return value

    except Exception as e:
        print("[CarbonAPI] Exception:", str(e))
        return None


if __name__ == "__main__":
    get_carbon_intensity()