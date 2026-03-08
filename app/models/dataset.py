import uuid

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.db.base import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    table_name = Column(String, unique=True, nullable=False, index=True)
    row_count = Column(Integer, default=0)
    status = Column(String, default="ready")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    columns = relationship("DatasetColumn", cascade="all, delete-orphan")
    metrics = relationship("DatasetMetric", cascade="all, delete-orphan")
    charts = relationship("DatasetChart", cascade="all, delete-orphan")
    insights = relationship("DatasetInsight", cascade="all, delete-orphan")


Index("ix_dataset_owner_name", Dataset.owner_id, Dataset.name, unique=True)


class DatasetColumn(Base):
    __tablename__ = "dataset_columns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    original_name = Column(String, nullable=False)
    name = Column(String, nullable=False)
    db_type = Column(String, nullable=False)
    order = Column("order", Integer, nullable=False)
    is_nullable = Column(Boolean, default=True)
    sample_values = Column(JSON, nullable=True)


Index("ix_dataset_columns_dataset_order", DatasetColumn.dataset_id, DatasetColumn.order)


class DatasetMetric(Base):
    __tablename__ = "dataset_metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    expression = Column(String, nullable=False)
    description = Column(String, nullable=True)


class DatasetChart(Base):
    __tablename__ = "dataset_charts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    spec = Column(JSON, nullable=False)
    title = Column(String, nullable=True)


class DatasetInsight(Base):
    __tablename__ = "dataset_insights"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    text = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String, nullable=False)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


Index("ix_audit_dataset_created_at", AuditLog.dataset_id, AuditLog.created_at.desc())
