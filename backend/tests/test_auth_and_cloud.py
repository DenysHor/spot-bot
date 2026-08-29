import hashlib

from fastapi.testclient import TestClient

from app import main
from app.core.auth import SessionAuth, validate_cloud_security
from app.persistence.sqlite import SQLiteStore


def test_session_token_verification_and_tampering():
    auth = SessionAuth("admin", "password", "secret", secure_cookies=True)
    token = auth.create_token()

    assert auth.valid_credentials("admin", "password")
    assert auth.verify_token(token)
    assert not auth.verify_token(token + "tampered")
    assert not auth.valid_credentials("admin", "wrong")


def test_protected_api_login_and_logout(monkeypatch):
    monkeypatch.setattr(main.auth, "enabled", True)
    monkeypatch.setattr(main.auth, "username", "tester")
    monkeypatch.setattr(main.auth, "password", "safe-password")
    monkeypatch.setattr(main.auth, "key", hashlib.sha256(b"test-secret").digest())
    client = TestClient(main.app)

    assert client.get("/api/paper/portfolio").status_code == 401
    assert client.post("/api/auth/login", json={"username": "tester", "password": "wrong"}).status_code == 401

    login = client.post("/api/auth/login", json={"username": "tester", "password": "safe-password"})
    assert login.status_code == 200
    assert client.get("/api/paper/portfolio").status_code == 200
    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/paper/portfolio").status_code == 401


def test_sqlite_backup_retention(tmp_path):
    path = tmp_path / "data" / "spot.db"
    store = SQLiteStore(str(path))
    for _ in range(4):
        assert store.create_backup(retain=2) is not None

    backups = list((path.parent / "backups").glob("spot-*.db"))
    assert len(backups) == 2


def test_cloud_security_requires_strong_secrets():
    validate_cloud_security(False, "", "")
    try:
        validate_cloud_security(True, "short", "x" * 32)
        assert False, "Expected short cloud password to be rejected"
    except RuntimeError as exc:
        assert "12 characters" in str(exc)
    try:
        validate_cloud_security(True, "long-enough-password", "short")
        assert False, "Expected short session secret to be rejected"
    except RuntimeError as exc:
        assert "32 characters" in str(exc)
    validate_cloud_security(True, "long-enough-password", "x" * 32)
