"""add_regulatory_detail_columns

Revision ID: a3d1f8e72b90
Revises: 7ab95f21c44b
Create Date: 2026-02-26 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'a3d1f8e72b90'
down_revision: Union[str, None] = '7ab95f21c44b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -- state_classifications: 17 new columns --

    # Three-tier enforcement
    op.add_column(
        'state_classifications',
        sa.Column('three_tier_enforcement', sa.String(length=30), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('three_tier_notes', sa.Text(), nullable=True),
        schema='jurisdiction',
    )

    # License types
    op.add_column(
        'state_classifications',
        sa.Column('has_on_premise_license', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('has_off_premise_license', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('has_manufacturer_license', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('has_distributor_license', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )

    # Sunday sales
    op.add_column(
        'state_classifications',
        sa.Column('sunday_sales_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('sunday_sales_hours', sa.String(length=100), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('sunday_sales_notes', sa.String(length=500), nullable=True),
        schema='jurisdiction',
    )

    # Grocery/convenience store rules
    op.add_column(
        'state_classifications',
        sa.Column('grocery_beer_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('grocery_wine_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('convenience_beer_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('convenience_wine_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('grocery_store_notes', sa.String(length=500), nullable=True),
        schema='jurisdiction',
    )

    # Beer ABV limits
    op.add_column(
        'state_classifications',
        sa.Column('beer_max_abv', sa.Numeric(precision=4, scale=2), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'state_classifications',
        sa.Column('beer_abv_notes', sa.String(length=500), nullable=True),
        schema='jurisdiction',
    )

    # -- jurisdictions: 5 propagated columns --
    op.add_column(
        'jurisdictions',
        sa.Column('three_tier_enforcement', sa.String(length=30), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('sunday_sales_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('grocery_beer_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('grocery_wine_allowed', sa.Boolean(), nullable=True),
        schema='jurisdiction',
    )
    op.add_column(
        'jurisdictions',
        sa.Column('beer_max_abv', sa.Numeric(precision=4, scale=2), nullable=True),
        schema='jurisdiction',
    )


def downgrade() -> None:
    # -- jurisdictions: drop 5 columns --
    op.drop_column('jurisdictions', 'beer_max_abv', schema='jurisdiction')
    op.drop_column('jurisdictions', 'grocery_wine_allowed', schema='jurisdiction')
    op.drop_column('jurisdictions', 'grocery_beer_allowed', schema='jurisdiction')
    op.drop_column('jurisdictions', 'sunday_sales_allowed', schema='jurisdiction')
    op.drop_column('jurisdictions', 'three_tier_enforcement', schema='jurisdiction')

    # -- state_classifications: drop 17 columns --
    op.drop_column('state_classifications', 'beer_abv_notes', schema='jurisdiction')
    op.drop_column('state_classifications', 'beer_max_abv', schema='jurisdiction')
    op.drop_column('state_classifications', 'grocery_store_notes', schema='jurisdiction')
    op.drop_column('state_classifications', 'convenience_wine_allowed', schema='jurisdiction')
    op.drop_column('state_classifications', 'convenience_beer_allowed', schema='jurisdiction')
    op.drop_column('state_classifications', 'grocery_wine_allowed', schema='jurisdiction')
    op.drop_column('state_classifications', 'grocery_beer_allowed', schema='jurisdiction')
    op.drop_column('state_classifications', 'sunday_sales_notes', schema='jurisdiction')
    op.drop_column('state_classifications', 'sunday_sales_hours', schema='jurisdiction')
    op.drop_column('state_classifications', 'sunday_sales_allowed', schema='jurisdiction')
    op.drop_column('state_classifications', 'has_distributor_license', schema='jurisdiction')
    op.drop_column('state_classifications', 'has_manufacturer_license', schema='jurisdiction')
    op.drop_column('state_classifications', 'has_off_premise_license', schema='jurisdiction')
    op.drop_column('state_classifications', 'has_on_premise_license', schema='jurisdiction')
    op.drop_column('state_classifications', 'three_tier_notes', schema='jurisdiction')
    op.drop_column('state_classifications', 'three_tier_enforcement', schema='jurisdiction')
