"""
Firebase ID token verification.
If firebase_admin is available, validate tokens; otherwise, fallback stub that rejects tokens.
"""

from fastapi import HTTPException, status

from app.core.config import settings

try:
    import firebase_admin
    from firebase_admin import auth, credentials
except ImportError:  # pragma: no cover
    firebase_admin = None
    auth = None

_firebase_initialized = False


def _ensure_init() -> None:
    global _firebase_initialized
    if firebase_admin and not _firebase_initialized:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
        _firebase_initialized = True


async def verify_token(token: str) -> dict:
    """Verify Firebase token; dev mode bypasses."""
    if settings.dev_mode:
        return {"uid": "dev-user", "email": "dev@local"}
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    if firebase_admin is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Firebase verification not configured (install firebase_admin)",
        )
    try:
        _ensure_init()
        decoded = auth.verify_id_token(token)
        return {"uid": decoded["uid"], "email": decoded.get("email")}
    except Exception as exc:  # pragma: no cover - network/crypto
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Firebase token") from exc
