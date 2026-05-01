"""add_regulatory_override_source

Revision ID: b4e2a9f83c01
Revises: a3d1f8e72b90
Create Date: 2026-02-27 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b4e2a9f83c01'
down_revision: Union[str, None] = 'a3d1f8e72b90'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'jurisdictions',
        sa.Column('regulatory_override_source', sa.String(length=100), nullable=True),
        schema='jurisdiction',
    )


def downgrade() -> None:
    op.drop_column('jurisdictions', 'regulatory_override_source', schema='jurisdiction')
