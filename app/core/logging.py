import logging


def setup_logging(level: str | int = "INFO") -> None:
    """Configure basic structured logging."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
