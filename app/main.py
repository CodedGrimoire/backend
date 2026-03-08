import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import datasets
from app.api.routes import health


def create_app() -> FastAPI:
    """FastAPI application factory."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app = FastAPI(title="AI Spreadsheet API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:8100",
            "http://127.0.0.1:8100",
            "*",  # relax for dev; tighten in production
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Public
    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    # Authenticated
    app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
    app.include_router(datasets.router, prefix="/api/v1/datasets", tags=["datasets"])
    return app


app = create_app()
