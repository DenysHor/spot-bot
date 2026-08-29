import hashlib
import hmac
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode


class SessionAuth:
    cookie_name = "spot_bot_session"

    def __init__(self, username: str, password: str, session_secret: str, secure_cookies: bool) -> None:
        self.username = username
        self.password = password
        self.enabled = bool(password)
        self.secure_cookies = secure_cookies
        key_source = session_secret or password
        self.key = hashlib.sha256(key_source.encode()).digest() if key_source else b""

    def valid_credentials(self, username: str, password: str) -> bool:
        return self.enabled and hmac.compare_digest(username, self.username) and hmac.compare_digest(password, self.password)

    def create_token(self, lifetime_seconds: int = 86400) -> str:
        expires = str(int(time.time()) + lifetime_seconds)
        payload = f"{self.username}:{expires}"
        signature = hmac.new(self.key, payload.encode(), hashlib.sha256).hexdigest()
        return urlsafe_b64encode(f"{payload}:{signature}".encode()).decode()

    def verify_token(self, token: str | None) -> bool:
        if not self.enabled:
            return True
        if not token:
            return False
        try:
            decoded = urlsafe_b64decode(token.encode()).decode()
            username, expires, signature = decoded.split(":", 2)
            payload = f"{username}:{expires}"
            expected = hmac.new(self.key, payload.encode(), hashlib.sha256).hexdigest()
            return (
                hmac.compare_digest(username, self.username)
                and int(expires) >= int(time.time())
                and hmac.compare_digest(signature, expected)
            )
        except (ValueError, TypeError):
            return False


def validate_cloud_security(secure_cookies: bool, password: str, session_secret: str) -> None:
    if not secure_cookies:
        return
    if len(password) < 12:
        raise RuntimeError("DASHBOARD_PASSWORD must contain at least 12 characters in cloud mode")
    if len(session_secret) < 32:
        raise RuntimeError("SESSION_SECRET must contain at least 32 characters in cloud mode")
