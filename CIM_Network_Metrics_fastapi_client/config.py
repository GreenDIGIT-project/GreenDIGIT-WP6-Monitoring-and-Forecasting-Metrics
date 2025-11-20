from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

def _as_bool(val: str, default: bool) -> bool:
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass
class Settings:
    # --- Base & Auth ---
    base_url: str = os.getenv("BASE_URL", "https://mc-a4.lab.uvalight.net").rstrip("/")
    # Note: default path uses the hyphenated endpoint confirmed by your probe
    auth_path: str = os.getenv("AUTH_PATH", "/gd-cim-api/get-token")
    email: str = os.getenv("AUTH_EMAIL", "")
    password: str = os.getenv("AUTH_PASSWORD", "")
    email_field: str = os.getenv("EMAIL_FIELD", "email")
    password_field: str = os.getenv("PASSWORD_FIELD", "password")
    token_field: str = os.getenv("TOKEN_FIELD", "access_token")
    token_type_default: str = os.getenv("TOKEN_TYPE", "Bearer")
    body_mode: str = os.getenv("AUTH_BODY_MODE", "json").strip().lower()

    # --- API endpoints for metrics ---
    submit_path: str = os.getenv("SUBMIT_PATH", "/gd-cim-api/submit")
    metrics_me_path: str = os.getenv("METRICS_ME_PATH", "/gd-cim-api/metrics/me")

    # --- Networking ---
    verify_tls: bool = _as_bool(os.getenv("VERIFY_TLS", "true"), True)
    timeout_seconds: int = int(os.getenv("TIMEOUT_SECONDS", "20"))

    # --- Convenience URLs ---
    @property
    def auth_url(self) -> str:
        return f"{self.base_url}{self.auth_path}"

    @property
    def submit_url(self) -> str:
        return f"{self.base_url}{self.submit_path}"

    @property
    def metrics_me_url(self) -> str:
        return f"{self.base_url}{self.metrics_me_path}"

settings = Settings()
