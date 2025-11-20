import requests
from typing import Optional, Dict, Any
from config import settings

class TokenClient:
    """
    Minimal client that posts {email, password} to the token endpoint
    and returns the token string. Request shape/field names are configurable
    via .env to match the API docs.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.verify = settings.verify_tls
        self._token: Optional[str] = None
        self._token_type: str = settings.token_type_default

    def get_token(self) -> str:
        payload: Dict[str, Any] = {
            settings.email_field: settings.email,
            settings.password_field: settings.password,
        }

        # Choose body encoding
        kwargs = {
            "timeout": settings.timeout_seconds,
        }
        if settings.body_mode == "form":
            kwargs["data"] = payload
        else:  # default to JSON
            kwargs["json"] = payload

        try:
            resp = self.session.post(settings.auth_url, **kwargs)
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to reach auth endpoint: {e}") from e

        if resp.status_code >= 400:
            # Surface some context without dumping everything
            raise RuntimeError(
                f"Auth failed ({resp.status_code}). "
                f"URL={settings.auth_url} BodyMode={settings.body_mode}. "
                f"Response snippet: {resp.text[:400]}"
            )

        # Expect JSON
        try:
            data = resp.json()
        except ValueError:
            # Some APIs return the token as raw text
            text = resp.text.strip()
            if text:
                self._token = text
                return text
            raise RuntimeError("Auth response was not JSON and empty text body received.")

        # Extract token by configured key, then common fallbacks
        token = data.get(settings.token_field) or \
                data.get("access_token") or \
                data.get("token") or \
                data.get("jwt") or \
                data.get("id_token")

        if not token:
            raise RuntimeError(
                f"Token not found in response. Checked '{settings.token_field}' "
                f"and common alternatives. Keys present: {list(data.keys())}"
            )

        # Prefer API-provided token_type if present
        self._token_type = data.get("token_type") or data.get("type") or settings.token_type_default
        self._token = token
        return token

    @property
    def token_type(self) -> str:
        return self._token_type

    @property
    def token(self) -> Optional[str]:
        return self._token
