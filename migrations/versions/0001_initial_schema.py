"""Initial transcription schema

Revision ID: 0001
Revises:
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transcription_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("external_id", sa.String(length=128), nullable=False),
        sa.Column("queue_job_id", sa.String(length=128), nullable=True),
        sa.Column("audio_path", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=True),
        sa.Column("model", sa.String(length=64), nullable=True),
        sa.Column("provider", sa.String(length=32), nullable=True),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("word_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_transcription_jobs_external_id",
        "transcription_jobs",
        ["external_id"],
        unique=True,
    )
    op.create_index("ix_transcription_jobs_status", "transcription_jobs", ["status"])
    op.create_index(
        "ix_transcription_jobs_status_created",
        "transcription_jobs",
        ["status", "created_at"],
    )

    op.create_table(
        "transcript_segments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("start_seconds", sa.Float(), nullable=False),
        sa.Column("end_seconds", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("speaker", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["job_id"], ["transcription_jobs.id"], ondelete="CASCADE"
        ),
    )
    op.create_index("ix_transcript_segments_job_id", "transcript_segments", ["job_id"])
    op.create_index(
        "ix_transcript_segments_job_index", "transcript_segments", ["job_id", "index"]
    )


def downgrade() -> None:
    op.drop_table("transcript_segments")
    op.drop_table("transcription_jobs")
