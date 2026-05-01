"""add_enrichment_columns

Revision ID: 7ab95f21c44b
Revises: 0f028c8226e5
Create Date: 2026-02-19 14:28:31.031712

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '7ab95f21c44b'
down_revision: Union[str, None] = '0f028c8226e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add enrichment columns to jurisdictions table
    op.add_column(
        'jurisdictions',
        sa.Column('licensing_authority_type', sa.String(length=30), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('licensing_authority_confidence', sa.String(length=20), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('dry_wet_data_source', sa.String(length=100), nullable=True),
        schema='jurisdiction',
    )


def downgrade() -> None:
    op.drop_column('jurisdictions', 'dry_wet_data_source', schema='jurisdiction')
    op.drop_column('jurisdictions', 'licensing_authority_confidence', schema='jurisdiction')
    op.drop_column('jurisdictions', 'licensing_authority_type', schema='jurisdiction')
