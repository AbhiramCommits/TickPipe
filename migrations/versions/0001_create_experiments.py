"""create experiments table

Revision ID: 0001
Revises:
Create Date: 2026-09-16
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_JSON_TYPE = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "experiments",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("git_commit", sa.String(64), nullable=False),
        sa.Column("git_dirty", sa.Boolean(), nullable=False),
        sa.Column("dataset_fingerprint", sa.String(64), nullable=True),
        sa.Column("feature_set", _JSON_TYPE, nullable=False),
        sa.Column("params", _JSON_TYPE, nullable=False),
        sa.Column("metrics", _JSON_TYPE, nullable=False),
        sa.Column("code_hash", sa.String(64), nullable=False),
        sa.Column("seed", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )


def downgrade() -> None:
    op.drop_table("experiments")
