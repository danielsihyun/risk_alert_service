"""
Email notification stub for unknown-region alerts.

Per the spec: when accounts have null/unknown regions, they cannot be routed
to a Slack channel. After the run completes, a single aggregated notification
is sent to support@quadsci.ai listing all unroutable accounts.

This module implements a stub that:
  1. Logs the full email content to application logs
  2. Writes the email to a file (unknown_region_report_{run_id}.txt) for review

In production, this would be replaced with a real email sender using:
  - AWS SES (if deployed on AWS)
  - SendGrid / Postmark (vendor-neutral)
  - Google Workspace SMTP (if in a Google environment)

The interface stays the same — send_unknown_region_notification() takes
the alert list and run_id. Only the transport changes.
"""

import logging
import os
from datetime import datetime, timezone

from app.config import SUPPORT_EMAIL, DETAILS_BASE_URL
from app.processing import AlertRecord

logger = logging.getLogger(__name__)

# Directory where email reports are written. Configurable for Docker volume mounts.
EMAIL_REPORT_DIR = os.getenv("EMAIL_REPORT_DIR", "./email_reports")


def send_unknown_region_notification(alerts: list[AlertRecord], run_id: str) -> str:
    """
    Send (or stub) an aggregated notification for alerts with unknown regions.

    Current implementation:
      - Logs the full email content (visible in application logs)
      - Writes to email_reports/unknown_region_report_{run_id}.txt

    Args:
        alerts: list of AlertRecords that could not be routed due to missing/unknown region
        run_id: the run that produced these alerts

    Returns:
        The email body string (for inclusion in run results / audit trail)
    """
    if not alerts:
        return ""

    timestamp = datetime.now(timezone.utc).isoformat()

    subject = f"[Risk Alerts] {len(alerts)} account(s) with unknown region — Run {run_id}"

    lines = [
        f"To: {SUPPORT_EMAIL}",
        f"Subject: {subject}",
        "",
        f"Run ID: {run_id}",
        f"Timestamp: {timestamp}",
        f"Total unroutable accounts: {len(alerts)}",
        "",
        "The following At Risk accounts could not be routed to a Slack channel",
        "because their region is missing or not present in the routing configuration.",
        "",
        "─" * 60,
    ]

    for alert in alerts:
        lines.extend([
            f"  Account: {alert.account_name} ({alert.account_id})",
            f"  Region:  {alert.account_region or 'NULL'}",
            f"  ARR:     ${alert.arr:,}",
            f"  At Risk: {alert.duration_months} month(s) (since {alert.risk_start_month})",
            f"  Owner:   {alert.account_owner or 'Unassigned'}",
            f"  Details: {DETAILS_BASE_URL}/{alert.account_id}",
            "─" * 60,
        ])

    lines.extend([
        "",
        "Action required: Update account regions in the source system or add",
        "the region to the channel routing configuration.",
    ])

    body = "\n".join(lines)

    # --- Stub transport: log + write to file ---
    logger.info(f"[EMAIL STUB] Would send to: {SUPPORT_EMAIL} — Subject: {subject}")

    _write_report_file(run_id, body)

    return body


def _write_report_file(run_id: str, body: str) -> None:
    """Write the email report to a file for review."""
    try:
        os.makedirs(EMAIL_REPORT_DIR, exist_ok=True)
        filepath = os.path.join(EMAIL_REPORT_DIR, f"unknown_region_report_{run_id}.txt")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(body)
        logger.info(f"[EMAIL STUB] Report written to: {filepath}")
    except OSError as e:
        logger.warning(f"[EMAIL STUB] Failed to write report file: {e}")
