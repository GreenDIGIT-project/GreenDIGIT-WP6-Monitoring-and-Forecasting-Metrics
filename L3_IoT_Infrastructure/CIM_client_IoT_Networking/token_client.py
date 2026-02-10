import os
import json
import time
import requests
from typing import Optional, Dict, Any
from config import settings

class TokenClient:
    def __init__(self, cache_file: str = ".token_cache.json"):
        self.session = requests.Session()
        self.session.verify = settings.verify_tls
        self.cache_file = cache_file
        self._token: Optional[str] = None
        self._token_type: str = settings.token_type_default
        self._expires_after_seconds = 20 * 3600  # 20 hours 

    def _load_cache(self) -> Optional[Dict[str, Any]]:
        """Reads the token and timestamp from the local file."""
        if not os.path.exists(self.cache_file):
            return None
        try:
            with open(self.cache_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def _save_cache(self, token: str, token_type: str):
        """Saves the token and current timestamp to the local file."""
        cache_data = {
            "token": token,
            "token_type": token_type,
            "timestamp": time.time()
        }
        with open(self.cache_file, "w") as f:
            json.dump(cache_data, f)

    def get_token(self, force: bool = False) -> str:
        """
        Returns cached token if < 8 hours old. 
        Forces refresh if force=True or if token is older than 8 hours.
        """
        cache = self._load_cache()
        current_time = time.time()
        
        # 8 hours in seconds
        REFRESH_AFTER = 8 * 3600 

        if not force and cache:
            timestamp = cache.get("timestamp", 0)
            # Use cache only if it's within the 8-hour window
            if (current_time - timestamp) < REFRESH_AFTER:
                self._token = cache["token"]
                self._token_type = cache.get("token_type", settings.token_type_default)
                return self._token

        return self._fetch_new_token()

    def _fetch_new_token(self) -> str:
        params = {
            settings.email_field: settings.email,
            settings.password_field: settings.password,
        }
        headers = {"Accept": "application/json"}

        try:
            # We use .get() as per the Swagger UI screenshot
            resp = self.session.get(
                settings.auth_url, 
                params=params, 
                headers=headers, 
                timeout=settings.timeout_seconds
            )
            
            # If you still get a 405, it might be that the URL in settings.auth_url 
            # needs to be checked for a trailing slash or exact path matches.
            resp.raise_for_status()
            
        except requests.RequestException as e:
            # Check if the server suggested a different method
            if resp.status_code == 405:
                raise RuntimeError(f"Method Not Allowed. Check if {settings.auth_url} really accepts GET.") from e
            raise RuntimeError(f"Failed to reach auth endpoint: {e}") from e

        data = resp.json()
        # Per screenshot, response contains access_token, token_type, expires_in
        token = data.get("access_token") or data.get(settings.token_field)
        token_type = data.get("token_type") or settings.token_type_default

        if not token:
            raise RuntimeError(f"Token missing in response: {data}")

        self._save_cache(token, token_type)
        self._token = token
        self._token_type = token_type
        return token

    @property
    def token_type(self) -> str:
        # If token hasn't been loaded yet, trigger a check
        if not self._token:
            self.get_token()
        return self._token_type