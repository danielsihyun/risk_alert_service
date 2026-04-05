"""
Run orchestrator — the main pipeline that ties all modules together.

Flow:
  1. Create a run record in SQLite
  2. Read Parquet via storage abstraction
  3. Process data: deduplicate, filter At Risk, compute durations
  4. For each alert:
     a. Check idempotency (skip if already sent)
     b. Route to channel (fail if unknown region)
     c. Send to Slack (with retries) unless dry_run
     d. Persist outcome
  5. Send aggregated email for unknown-region alerts
  6. Update run record with final counts and status

This module is the only place that knows about all the other modules.
Each individual module (storage, processing, slack, db, email) is
independently testable and has no knowledge of the others.
"""

import datetime
import logging
import uuid

from app.config import ARR_THRESHOLD
from app.db import (
    init_db,
    get_session,
    Run,
    AlertOutcome,
    check_already_sent,
    upsert_alert_outcome,
    utc_now,
)
from app.email import send_unknown_region_notification
from app.processing import process_month, AlertRecord
from app.slack import format_alert_message, get_channel_for_region, send_slack_message
from app.storage import open_uri

logger = logging.getLogger(__name__)


def execute_run(source_uri: str, month: str, dry_run: bool = False) -> str:
    """
    Execute a full alert processing run.

    Args:
        source_uri: URI to the Parquet file (file://, gs://, s3://)
        month: target month as "YYYY-MM-DD" (first of month)
        dry_run: if True, compute alerts but don't send to Slack

    Returns:
        run_id string

    Raises:
        FileNotFoundError: if the source file doesn't exist
        ValueError: if the URI scheme is unsupported
    """
    # Validate inputs and load data BEFORE creating the run record.
    # This lets storage/validation errors propagate to the caller
    # so main.py can return appropriate HTTP status codes (400, 404).
    table = _load_data(source_uri, month)

    init_db()
    session = get_session()

    run_id = str(uuid.uuid4())
    run = Run(
        run_id=run_id,
        source_uri=source_uri,
        month=month,
        dry_run=dry_run,
        status="running",
        started_at=utc_now(),
    )
    session.add(run)
    session.commit()

    try:
        # ------------------------------------------------------------------
        # Step 1: Process — deduplicate, filter, compute durations
        # ------------------------------------------------------------------
        logger.info(f"[{run_id}] Processing alerts for {month}")
        result = process_month(table, month)

        run.rows_scanned = result.rows_scanned
        run.duplicates_found = result.duplicates_found
        run.alerts_generated = len(result.alerts)

        logger.info(
            f"[{run_id}] Found {len(result.alerts)} alerts "
            f"({result.duplicates_found} duplicates removed, "
            f"{result.rows_scanned} rows scanned)"
        )

        # ------------------------------------------------------------------
        # Step 3: Deliver each alert
        # ------------------------------------------------------------------
        counters = {"sent": 0, "skipped_replay": 0, "failed": 0, "unknown_region": 0, "dry_run": 0}
        unknown_region_alerts: list[AlertRecord] = []

        for alert in result.alerts:
            outcome_status, error = _deliver_alert(
                session, run_id, alert, dry_run
            )

            counters[outcome_status] += 1

            if outcome_status == "unknown_region":
                unknown_region_alerts.append(alert)
                counters["failed"] += 1  # unknown_region also counts as failed

        # ------------------------------------------------------------------
        # Step 4: Send aggregated email for unknown regions
        # ------------------------------------------------------------------
        if unknown_region_alerts:
            logger.info(
                f"[{run_id}] {len(unknown_region_alerts)} alerts with unknown region — "
                f"sending aggregated notification"
            )
            send_unknown_region_notification(unknown_region_alerts, run_id)

        # ------------------------------------------------------------------
        # Step 5: Finalize run
        # ------------------------------------------------------------------
        run.alerts_sent = counters["sent"]
        run.skipped_replay = counters["skipped_replay"]
        run.failed_deliveries = counters["failed"]
        run.unknown_region = counters["unknown_region"]
        run.status = "succeeded"
        run.completed_at = utc_now()
        session.commit()

        logger.info(
            f"[{run_id}] Run complete — "
            f"sent={counters['sent']}, skipped={counters['skipped_replay']}, "
            f"failed={counters['failed']}, unknown_region={counters['unknown_region']}, "
            f"dry_run={counters['dry_run']}"
        )

    except Exception as e:
        logger.error(f"[{run_id}] Run failed: {e}", exc_info=True)
        run.status = "failed"
        run.error = str(e)
        run.completed_at = utc_now()
        session.commit()

    finally:
        session.close()

    return run_id


def _load_data(source_uri: str, month: str):
    """
    Load Parquet data with efficient filtering.

    Only reads rows needed for:
      - The target month (for At Risk detection)
      - History months (for duration calculation)

    We don't know exactly how far back to go (an account could have been
    At Risk for the entire dataset), so we load all months up to and
    including the target month. We exclude future months since they're
    irrelevant.
    """
    target_date = datetime.date.fromisoformat(month)

    # Columns we actually need — skip any we don't use
    columns = [
        "account_id",
        "account_name",
        "account_region",
        "month",
        "status",
        "renewal_date",
        "account_owner",
        "arr",
        "updated_at",
    ]

    # Predicate pushdown: only load months up to and including target.
    # The parquet file stores month as date32[day], and datetime.date
    # matches this type for PyArrow predicate pushdown to work correctly.
    # If the source file used timestamp instead, this filter value would
    # need to be cast to match.
    filters = [("month", "<=", target_date)]

    return open_uri(source_uri, columns=columns, filters=filters)


def _deliver_alert(
    session,
    run_id: str,
    alert: AlertRecord,
    dry_run: bool,
) -> tuple[str, str | None]:
    """
    Handle delivery of a single alert: idempotency check, routing, sending.

    Returns:
        (outcome_status, error_message)

    Possible outcome_status values:
        "sent"            — successfully delivered to Slack
        "dry_run"         — not sent because dry_run=True
        "skipped_replay"  — already sent in a prior run
        "failed"          — Slack delivery failed after retries
        "unknown_region"  — region not in routing config, not sent
    """
    # ------------------------------------------------------------------
    # Idempotency check: skip if already sent successfully
    # ------------------------------------------------------------------
    prior_status = check_already_sent(session, alert.account_id, alert.month)

    if prior_status == "sent":
        # Already sent in a prior run — skip. No need to upsert since
        # the existing "sent" record should not be overwritten.
        return "skipped_replay", None

    # ------------------------------------------------------------------
    # Channel routing
    # ------------------------------------------------------------------
    channel = get_channel_for_region(alert.account_region)

    if channel is None:
        upsert_alert_outcome(
            session, run_id, alert.account_id, alert.account_name,
            alert.month, "at_risk", None, "failed", error="unknown_region",
        )
        session.commit()
        return "unknown_region", "unknown_region"

    # ------------------------------------------------------------------
    # Dry run — record but don't send
    # ------------------------------------------------------------------
    if dry_run:
        upsert_alert_outcome(
            session, run_id, alert.account_id, alert.account_name,
            alert.month, "at_risk", channel, "dry_run",
        )
        session.commit()
        return "dry_run", None

    # ------------------------------------------------------------------
    # Send to Slack
    # ------------------------------------------------------------------
    payload = format_alert_message(alert)
    success, error = send_slack_message(channel, payload)

    if success:
        upsert_alert_outcome(
            session, run_id, alert.account_id, alert.account_name,
            alert.month, "at_risk", channel, "sent",
        )
    else:
        upsert_alert_outcome(
            session, run_id, alert.account_id, alert.account_name,
            alert.month, "at_risk", channel, "failed", error=error,
        )

    session.commit()
    return ("sent" if success else "failed"), error


def get_run_results(run_id: str) -> dict | None:
    """
    Retrieve persisted run results for the /runs/{run_id} endpoint.

    Returns a dict with status, counts, and sample alerts/errors,
    or None if the run_id doesn't exist.
    """
    init_db()
    session = get_session()

    try:
        run = session.query(Run).filter_by(run_id=run_id).first()
        if not run:
            return None

        # Get sample alerts (up to 10 sent)
        sample_alerts = (
            session.query(AlertOutcome)
            .filter_by(run_id=run_id, status="sent")
            .limit(10)
            .all()
        )

        # Get sample errors (up to 10 failed)
        sample_errors = (
            session.query(AlertOutcome)
            .filter_by(run_id=run_id)
            .filter(AlertOutcome.status.in_(["failed"]))
            .limit(10)
            .all()
        )

        # Get skipped replays: alerts that were sent by a prior run for the
        # same month. These are the ones this run would have skipped.
        # We query by month + status="sent" + run_id != current run.
        if run.skipped_replay and run.skipped_replay > 0:
            sample_skipped = (
                session.query(AlertOutcome)
                .filter_by(month=run.month, alert_type="at_risk", status="sent")
                .filter(AlertOutcome.run_id != run_id)
                .limit(10)
                .all()
            )
        else:
            sample_skipped = []

        return {
            "run_id": run.run_id,
            "source_uri": run.source_uri,
            "month": run.month,
            "dry_run": run.dry_run,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "error": run.error,
            "counts": {
                "rows_scanned": run.rows_scanned,
                "duplicates_found": run.duplicates_found,
                "alerts_generated": run.alerts_generated,
                "alerts_sent": run.alerts_sent,
                "skipped_replay": run.skipped_replay,
                "failed_deliveries": run.failed_deliveries,
                "unknown_region": run.unknown_region,
            },
            "sample_alerts": [
                {
                    "account_id": a.account_id,
                    "account_name": a.account_name,
                    "channel": a.channel,
                    "status": a.status,
                    "sent_at": a.sent_at.isoformat() if a.sent_at else None,
                }
                for a in sample_alerts
            ],
            "sample_errors": [
                {
                    "account_id": a.account_id,
                    "account_name": a.account_name,
                    "channel": a.channel,
                    "status": a.status,
                    "error": a.error,
                }
                for a in sample_errors
            ],
            "sample_skipped": [
                {
                    "account_id": a.account_id,
                    "account_name": a.account_name,
                    "status": a.status,
                }
                for a in sample_skipped
            ],
        }

    finally:
        session.close()


def get_preview_results(source_uri: str, month: str) -> dict:
    """
    Compute alerts without sending or persisting.
    Returns the alert list directly for the /preview endpoint.
    """
    table = _load_data(source_uri, month)
    result = process_month(table, month)

    return {
        "month": month,
        "source_uri": source_uri,
        "rows_scanned": result.rows_scanned,
        "duplicates_found": result.duplicates_found,
        "total_alerts": len(result.alerts),
        "arr_threshold": ARR_THRESHOLD,
        "alerts": [
            {
                "account_id": a.account_id,
                "account_name": a.account_name,
                "account_region": a.account_region,
                "arr": a.arr,
                "duration_months": a.duration_months,
                "risk_start_month": a.risk_start_month,
                "renewal_date": a.renewal_date,
                "account_owner": a.account_owner,
                "channel": get_channel_for_region(a.account_region),
                "routable": get_channel_for_region(a.account_region) is not None,
            }
            for a in result.alerts
        ],
    }
