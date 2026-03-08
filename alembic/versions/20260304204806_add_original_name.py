"""add original_name to dataset_columns

Revision ID: 20260304204806_add_original
Revises: 20260305000000_create_metadata
Create Date: 2026-03-04 20:48:06.059669
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260304204806_add_original'
down_revision = '20260305000000_create_metadata'
branch_labels = None
depends_on = None

def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    cols = [c["name"] for c in inspector.get_columns("dataset_columns")]
    if "original_name" in cols:
        return
    op.add_column('dataset_columns', sa.Column('original_name', sa.String(), nullable=True))
    op.execute("UPDATE dataset_columns SET original_name = name WHERE original_name IS NULL")
    op.alter_column('dataset_columns', 'original_name', nullable=False)

def downgrade() -> None:
    op.drop_column('dataset_columns', 'original_name')
