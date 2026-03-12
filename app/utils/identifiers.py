import re
import hashlib


def sanitize_identifier(name: str) -> str:
    """Sanitize arbitrary text into a safe SQL identifier."""
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)  # allow only alnum -> underscore
    name = re.sub(r"_+", "_", name)          # collapse repeats
    name = name.strip("_")
    if name == "":
        name = "col"
    if not re.match(r"^[a-z]", name):
        name = f"col_{name}"
    return name


def short_hash(text: str, length: int = 6) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:length]
