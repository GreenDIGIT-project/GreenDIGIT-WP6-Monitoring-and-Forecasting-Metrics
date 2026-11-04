# probe_paths.py
import os, requests, json
from dotenv import load_dotenv
load_dotenv()

BASE = os.getenv("BASE_URL", "https://mc-a4.lab.uvalight.net").rstrip("/")
EMAIL_KEY = os.getenv("EMAIL_FIELD", "email")
PASS_KEY  = os.getenv("PASSWORD_FIELD", "password")
EMAIL     = os.getenv("AUTH_EMAIL", "")
PASS      = os.getenv("AUTH_PASSWORD", "")
VERIFY    = os.getenv("VERIFY_TLS", "true").strip().lower() in {"1","true","yes","y","on"}
TIMEOUT   = int(os.getenv("TIMEOUT_SECONDS", "20"))

paths = [
    "/gd-cim-api/v1/token",
    "/gd-cim-api/v1/verify-token",
]

def try_call(url, mode):
    body = {EMAIL_KEY: EMAIL, PASS_KEY: PASS}
    try:
        if mode == "form":
            r = requests.post(url, data=body, timeout=TIMEOUT, verify=VERIFY)
        else:
            r = requests.post(url, json=body, timeout=TIMEOUT, verify=VERIFY)
        ctype = r.headers.get("content-type","")
        preview = r.text[:300]
        print(f"[{mode.upper()}] {url} -> {r.status_code} {ctype} :: {preview!r}")
        if r.ok:
            try:
                print("Parsed JSON:", json.dumps(r.json(), indent=2)[:500])
            except Exception:
                pass
        return r.ok
    except Exception as e:
        print(f"[{mode.upper()}] {url} -> ERROR: {e}")
        return False

for p in paths:
    url = f"{BASE}{p}"
    if try_call(url, "form"): break
    if try_call(url, "json"): break
