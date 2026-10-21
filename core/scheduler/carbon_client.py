import requests
from datetime import datetime

CARBON_API_URL = "https://api.wattnet.eu/v1/footprints"

SITE_LAT = 39.36590004168098
SITE_LON = 22.92380157490816

RAW_TOKEN = """
eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJubkxEVEtJVkJBN3o5N3Zrbi1RWE5zQXFwSUNVQlQtam5fZ25mX2Z4TWdZIn0.eyJleHAiOjE3ODQxODE5MzcsImlhdCI6MTc4NDA5NTUzNywianRpIjoidHJydGNjOmI2M2NiYzc3LTRkNDgtZDM4Zi05NzMzLTA5NGRkNDA1ZmJkOSIsImlzcyI6Imh0dHBzOi8vYXV0aC53YXR0bmV0LmV1L3JlYWxtcy93YXR0bmV0IiwiYXVkIjpbIm9hdXRoMi1wcm94eSIsImFjY291bnQiXSwic3ViIjoiOTQ0NWUzODYtOGQzOS00ZDYxLTlmYzctZWEwMmIzNGY0MTEwIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoib2F1dGgyLXByb3h5IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2FwaS53YXR0bmV0LmV1Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLXdhdHRwcmludCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiY2xpZW50SG9zdCI6IjE3Mi4xNi4zNS4xMjEiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtb2F1dGgyLXByb3h5IiwiY2xpZW50QWRkcmVzcyI6IjE3Mi4xNi4zNS4xMjEiLCJjbGllbnRfaWQiOiJvYXV0aDItcHJveHkifQ.elzuYNSuTolYXCw6ymzDzCfld2-U4lPsAuMY-kjRd-2fcXa0XkT25f3HcKYQeyo5kRPvJdfRlsLhWPIyhgtxfz4AZhjzZnhvzYZefyyQyms-dE9b08WeZRNan2TvfllkkR9NKlPzgMVW1Doy8Lotpu4y4HeX0eHkNDKgLnNSQgVQE3RqTqZZ0ZebTxydt9MLif2JG3lqvx3rm6Hl66cDUomndxMUns3gkSqwRkdMWBtzXmds7tBjh5045xZA0uhEvOSj8WUKxHkJQ5iyr7JuJWO86Hu_EqMq1Jb3UATE6sN1jBEn48mPy4aCI3veCWCU-lvZ4dtcRtTx1bf5M97HKw

"""

def _get_headers():
    clean_token = RAW_TOKEN.replace("\n", "").replace("\r", "").strip()

    return {
        "Authorization": f"Bearer {clean_token}",
        "Accept": "application/json",
    }


def _to_utc_iso8601(value: datetime) -> str:
    """
    Convert a timezone-aware datetime to the UTC ISO-8601 format
    expected by the API.
    """
    if value.tzinfo is None:
        raise ValueError("start_time and end_time must be timezone-aware")

    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def get_carbon_intensity(start_time: datetime, end_time: datetime):
    """
    Return the average carbon intensity, in gCO2/kWh,
    between start_time and end_time.

    Returns None when the API request or payload is invalid.
    """
    params = {
        "lat": SITE_LAT,
        "lon": SITE_LON,
        "footprint_type": "carbon",
        "scope": "life-cycle",
        "start": _to_utc_iso8601(start_time),
        "end": _to_utc_iso8601(end_time),
        "aggregate": False,
        "use_global": True,
    }

    try:
        response = requests.get(
            CARBON_API_URL,
            params=params,
            headers=_get_headers(),
            timeout=10,
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list) or not data:
            return None

        series = data[0].get("series")

        if not isinstance(series, list):
            return None

        intensity_values = []

        for block in series:
            if not isinstance(block, dict):
                continue

            for entry in block.get("values", []):
                if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                    continue

                _, intensity_value = entry

                try:
                    intensity_values.append(float(intensity_value))
                except (TypeError, ValueError):
                    continue

        if not intensity_values:
            return None

        return sum(intensity_values) / len(intensity_values)

    except requests.RequestException as exc:
        print(f"[CarbonAPI] Request error: {exc}")
        return None
    except (ValueError, TypeError, KeyError) as exc:
        print(f"[CarbonAPI] Invalid response: {exc}")
        return None