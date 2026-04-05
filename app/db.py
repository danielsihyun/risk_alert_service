"""
Persistence layer using SQLite + SQLAlchemy.

Two tables:
  - runs: metadata about each processing run
  - alert_outcomes: per-account alert results with idempotency enforcement

The unique constraint on (account_id, month, alert_type) in alert_outcomes
is what powers replay safety — we check this before sending each alert.
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Boolean,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from app.config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Run(Base):
    __tablename__ = "runs"

    run_id = Column(String, primary_key=True)
    source_uri = Column(String, nullable=False)
    month = Column(String, nullable=False)          # "YYYY-MM-DD"
    dry_run = Column(Boolean, default=False)
    status = Column(String, default="running")       # running | succeeded | failed
    rows_scanned = Column(Integer, default=0)
    duplicates_found = Column(Integer, default=0)
    alerts_generated = Column(Integer, default=0)
    alerts_sent = Column(Integer, default=0)
    skipped_replay = Column(Integer, default=0)
    failed_deliveries = Column(Integer, default=0)
    unknown_region = Column(Integer, default=0)
    error = Column(Text, nullable=True)
    started_at = Column(DateTime, default=utc_now)
    completed_at = Column(DateTime, nullable=True)


class AlertOutcome(Base):
    __tablename__ = "alert_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False)
    account_id = Column(String, nullable=False)
    account_name = Column(String, nullable=True)
    month = Column(String, nullable=False)
    alert_type = Column(String, default="at_risk")
    channel = Column(String, nullable=True)
    status = Column(String, nullable=False)          # sent | failed | skipped_replay | dry_run
    error = Column(Text, nullable=True)
    sent_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("account_id", "month", "alert_type", name="uq_account_month_type"),
    )


# ---------------------------------------------------------------------------
# DB lifecycle
# ---------------------------------------------------------------------------

def init_db():
    """Create tables if they don't exist."""
    Base.metadata.create_all(engine)


def get_session() -> Session:
    return SessionLocal()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_already_sent(session: Session, account_id: str, month: str, alert_type: str = "at_risk"):
    """
    Check if an alert was already recorded for this (account_id, month, alert_type).

    Returns:
        None           — no prior record, safe to send
        "sent"         — already sent successfully, should skip
        "failed"       — previously failed, eligible for retry
        "dry_run"      — was a dry run, eligible for real send
    """
    existing = (
        session.query(AlertOutcome)
        .filter_by(account_id=account_id, month=month, alert_type=alert_type)
        .first()
    )
    if existing is None:
        return None
    return existing.status


def upsert_alert_outcome(
    session: Session,
    run_id: str,
    account_id: str,
    account_name: str,
    month: str,
    alert_type: str,
    channel: str | None,
    status: str,
    error: str | None = None,
):
    """
    Insert or update an alert outcome.

    On replay: if a prior record exists with status 'failed' or 'dry_run',
    we update it with the new result. If it was 'sent', the caller should
    have already skipped — but we guard against it here too.
    """
    existing = (
        session.query(AlertOutcome)
        .filter_by(account_id=account_id, month=month, alert_type=alert_type)
        .first()
    )

    if existing:
        # Only update if the previous attempt was failed or dry_run
        if existing.status in ("failed", "dry_run"):
            existing.run_id = run_id
            existing.account_name = account_name
            existing.channel = channel
            existing.status = status
            existing.error = error
            existing.sent_at = utc_now() if status == "sent" else None
        # If already sent, don't touch it
        return existing

    outcome = AlertOutcome(
        run_id=run_id,
        account_id=account_id,
        account_name=account_name,
        month=month,
        alert_type=alert_type,
        channel=channel,
        status=status,
        error=error,
        sent_at=utc_now() if status == "sent" else None,
    )
    session.add(outcome)
    return outcome
