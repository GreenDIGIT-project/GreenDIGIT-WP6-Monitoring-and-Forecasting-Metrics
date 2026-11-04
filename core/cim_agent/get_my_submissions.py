# get_my_submissions.py
import requests
from token_client import TokenClient
from config import settings

def main():
    client = TokenClient()
    token = client.get_token()
    headers = {
        "Authorization": f"{client.token_type} {token}",
        "Accept": "application/json",
    }

    url = getattr(settings, "metrics_me_url", f"{settings.base_url}{getattr(settings, 'metrics_me_path', '/gd-cim-api/v1/metrics/me')}")
    with requests.Session() as s:
        s.verify = settings.verify_tls
        r = s.get(url, headers=headers, timeout=settings.timeout_seconds)

    if r.status_code < 300:
        print("✅ Your submissions:")
        try:
            print(r.json())
        except Exception:
            print(r.text)
    else:
        print(f"❌ Request failed ({r.status_code})")
        print(r.text[:2000])

if __name__ == "__main__":
    main()
