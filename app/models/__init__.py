from app.core.db.base import Base  # re-export for Alembic

# Import models so Alembic can discover metadata
from app.models.user import User  # noqa: F401
from app.models.dataset import (  # noqa: F401
    Dataset,
    DatasetChart,
    DatasetColumn,
    DatasetInsight,
    DatasetMetric,
    AuditLog,
)
