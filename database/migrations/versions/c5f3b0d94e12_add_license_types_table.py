"""add license_types table and enhanced dim_states columns

Revision ID: c5f3b0d94e12
Revises: b4e2a9f83c01
Create Date: 2026-03-24 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c5f3b0d94e12"
down_revision = "b4e2a9f83c01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create license_types table
    op.create_table(
        "license_types",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("state_fips", sa.String(length=2), nullable=False),
        sa.Column("license_type_code", sa.String(length=50), nullable=False),
        sa.Column("license_type_name", sa.String(length=200), nullable=False),
        sa.Column("license_category", sa.String(length=30), nullable=False),
        sa.Column("permits_on_premise", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("permits_off_premise", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("permits_beer", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("permits_wine", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("permits_spirits", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("retail_channel", sa.String(length=50), nullable=True),
        sa.Column("abv_limit", sa.Numeric(precision=4, scale=2), nullable=True),
        sa.Column("quota_limited", sa.Boolean(), nullable=True),
        sa.Column("quota_notes", sa.String(length=200), nullable=True),
        sa.Column("transferable", sa.Boolean(), nullable=True),
        sa.Column("annual_fee_range", sa.String(length=50), nullable=True),
        sa.Column("issuing_authority", sa.String(length=100), nullable=True),
        sa.Column("statutory_reference", sa.String(length=200), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("research_status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("research_source", sa.String(length=500), nullable=True),
        sa.Column("last_verified_date", sa.Date(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("state_fips", "license_type_code", name="uq_license_type_natural_key"),
        schema="jurisdiction",
    )
    op.create_index(
        "ix_license_types_state_fips",
        "license_types",
        ["state_fips"],
        schema="jurisdiction",
    )

    # Add new columns to state_classifications
    with op.batch_alter_table("state_classifications", schema="jurisdiction") as batch_op:
        batch_op.add_column(sa.Column("grocery_liquor_allowed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("convenience_liquor_allowed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("grocery_beer_confidence", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("grocery_wine_confidence", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("grocery_liquor_confidence", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("retail_channel_notes", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("license_type_count", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("license_complexity_tier", sa.String(length=10), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("state_classifications", schema="jurisdiction") as batch_op:
        batch_op.drop_column("license_complexity_tier")
        batch_op.drop_column("license_type_count")
        batch_op.drop_column("retail_channel_notes")
        batch_op.drop_column("grocery_liquor_confidence")
        batch_op.drop_column("grocery_wine_confidence")
        batch_op.drop_column("grocery_beer_confidence")
        batch_op.drop_column("convenience_liquor_allowed")
        batch_op.drop_column("grocery_liquor_allowed")

    op.drop_index("ix_license_types_state_fips", table_name="license_types", schema="jurisdiction")
    op.drop_table("license_types", schema="jurisdiction")
