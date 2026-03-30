"""make email unique

Revision ID: ea977c719bba
Revises: f148cad15ad2
Create Date: 2024-08-03 00:39:08.107105

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ea977c719bba"
down_revision: Union[str, None] = "f148cad15ad2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(None, "users", ["email"])


def downgrade() -> None:
    op.drop_constraint(None, "users", type_="unique")
