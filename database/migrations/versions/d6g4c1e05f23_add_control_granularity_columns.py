"""add granular control and data_effective_date columns

Revision ID: d6g4c1e05f23
Revises: c5f3b0d94e12
Create Date: 2026-03-26 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d6g4c1e05f23"
down_revision = "c5f3b0d94e12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("state_classifications", schema="jurisdiction") as batch_op:
        batch_op.add_column(sa.Column("spirits_control", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("wine_control", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("beer_control", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("wholesale_control", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("retail_control", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("data_effective_date", sa.String(length=50), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("state_classifications", schema="jurisdiction") as batch_op:
        batch_op.drop_column("data_effective_date")
        batch_op.drop_column("retail_control")
        batch_op.drop_column("wholesale_control")
        batch_op.drop_column("beer_control")
        batch_op.drop_column("wine_control")
        batch_op.drop_column("spirits_control")
