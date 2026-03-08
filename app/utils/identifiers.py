import re
import hashlib


def sanitize_identifier(name: str) -> str:
    """Strip unsafe chars and lowercase for SQL identifiers."""
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return safe.lower().strip("_")


def short_hash(text: str, length: int = 6) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:length]
