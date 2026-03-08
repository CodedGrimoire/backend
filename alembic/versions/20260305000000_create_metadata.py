"""create metadata tables

Revision ID: 20260305000000_create_metadata
Revises: 
Create Date: 2026-03-05 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260305000000_create_metadata"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("firebase_uid", sa.String(), nullable=False, unique=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )

    op.create_table(
        "datasets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False, unique=True),
        sa.Column("row_count", sa.Integer(), server_default="0"),
        sa.Column("status", sa.String(), server_default="ready"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index("ix_dataset_owner_name", "datasets", ["owner_id", "name"], unique=True)

    op.create_table(
        "dataset_columns",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("original_name", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("db_type", sa.String(), nullable=False),
        sa.Column("order", sa.Integer(), nullable=False),
        sa.Column("is_nullable", sa.Boolean(), server_default=sa.sql.expression.true(), nullable=False),
        sa.Column("sample_values", sa.JSON(), nullable=True),
    )
    op.create_index("ix_dataset_columns_dataset_order", "dataset_columns", ["dataset_id", "order"])

    op.create_table(
        "dataset_metrics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("expression", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
    )

    op.create_table(
        "dataset_charts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("spec", sa.JSON(), nullable=False),
        sa.Column("title", sa.String(), nullable=True),
    )

    op.create_table(
        "dataset_insights",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("text", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_audit_dataset_created_at", "audit_logs", ["dataset_id", sa.text("created_at DESC")])


def downgrade() -> None:
    op.drop_index("ix_audit_dataset_created_at", table_name="audit_logs")
    op.drop_table("audit_logs")
    op.drop_table("dataset_insights")
    op.drop_table("dataset_charts")
    op.drop_table("dataset_metrics")
    op.drop_index("ix_dataset_columns_dataset_order", table_name="dataset_columns")
    op.drop_table("dataset_columns")
    op.drop_index("ix_dataset_owner_name", table_name="datasets")
    op.drop_table("datasets")
    op.drop_table("users")
