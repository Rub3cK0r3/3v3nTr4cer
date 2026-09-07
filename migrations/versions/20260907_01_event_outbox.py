"""Add durable event outbox.

Revision ID: 20260907_01
Revises:
"""
from alembic import context, op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260907_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    if not context.is_offline_mode():
        bind = op.get_bind()
        inspector = sa.inspect(bind)
        if inspector.has_table("event_outbox"):
            return
    op.create_table(
        "event_outbox",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.BigInteger(), nullable=False),
        sa.Column("published_at", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    op.create_index("idx_event_outbox_status_created_at", "event_outbox", ["status", "created_at"])
    op.create_index("idx_event_outbox_created_at", "event_outbox", ["created_at"])


def downgrade() -> None:
    op.drop_index("idx_event_outbox_created_at", table_name="event_outbox")
    op.drop_index("idx_event_outbox_status_created_at", table_name="event_outbox")
    op.drop_table("event_outbox")
