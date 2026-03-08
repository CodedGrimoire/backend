import logging
from fastapi import FastAPI

from app.api.routes import datasets
from app.api.routes import health


def create_app() -> FastAPI:
    """FastAPI application factory."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app = FastAPI(title="AI Spreadsheet API", version="0.1.0")
    # Public
    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    # Authenticated
    app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
    app.include_router(datasets.router, prefix="/api/v1/datasets", tags=["datasets"])
    return app


app = create_app()
