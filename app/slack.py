"""
Slack integration module.

Responsibilities:
  1. Format alert records into Slack message payloads
  2. Route messages to the correct channel based on region config
  3. POST to Slack webhooks with retry on transient failures (429, 5xx)
  4. Support two modes: base_url (mock/custom) and single_webhook (real Slack)

Retry strategy:
  - Retry on HTTP 429 (rate limited) and 5xx (server error)
  - Exponential backoff: 1s, 2s, 4s (configurable)
  - Honor Retry-After header when present (use it if it's larger than calculated backoff)
  - Give up after max retries and record as failed
"""

import logging
import time

import requests

from app.config import (
    SLACK_WEBHOOK_BASE_URL,
    SLACK_WEBHOOK_URL,
    REGION_CHANNEL_MAP,
    DETAILS_BASE_URL,
    SLACK_MAX_RETRIES,
    SLACK_INITIAL_BACKOFF,
    SLACK_BACKOFF_MULTIPLIER,
    SLACK_REQUEST_TIMEOUT,
    get_slack_mode,
)
from app.processing import AlertRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def format_alert_message(alert: AlertRecord) -> dict:
    """
    Build a Slack message payload from an AlertRecord.

    Format per spec:
      🚩 At Risk: {account_name} ({account_id})
      Region: {region}
      At Risk for: X months (since YYYY-MM-01)
      ARR: $X
      Renewal date: YYYY-MM-DD or "Unknown"
      Owner: {owner}
      Details: {url}
    """
    renewal = alert.renewal_date if alert.renewal_date else "Unknown"
    region = alert.account_region if alert.account_region else "Unknown"
    owner_line = f"Owner: {alert.account_owner}" if alert.account_owner else "Owner: Unassigned"
    details_url = f"{DETAILS_BASE_URL}/{alert.account_id}"

    text = (
        f"🚩 At Risk: {alert.account_name} ({alert.account_id})\n"
        f"Region: {region}\n"
        f"At Risk for: {alert.duration_months} month{'s' if alert.duration_months != 1 else ''} "
        f"(since {alert.risk_start_month})\n"
        f"ARR: ${alert.arr:,}\n"
        f"Renewal date: {renewal}\n"
        f"{owner_line}\n"
        f"Details: {details_url}"
    )

    return {"text": text}


# ---------------------------------------------------------------------------
# Channel routing
# ---------------------------------------------------------------------------

def get_channel_for_region(region: str | None) -> str | None:
    """
    Look up the Slack channel for a given region.

    Returns:
        Channel name string, or None if region is unknown/null/not in config.
        There is NO default channel — unknown regions must be recorded as failures.
    """
    if region is None:
        return None
    return REGION_CHANNEL_MAP.get(region)


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

def send_slack_message(channel: str, payload: dict) -> tuple[bool, str | None]:
    """
    POST a message to Slack with retry logic.

    Args:
        channel: Slack channel name (used in base_url mode to build the URL)
        payload: JSON payload to send

    Returns:
        (success: bool, error_message: str | None)
    """
    mode = get_slack_mode()

    if mode == "base_url":
        url = f"{SLACK_WEBHOOK_BASE_URL}/{channel}"
    elif mode == "single_webhook":
        url = SLACK_WEBHOOK_URL
    else:
        return False, "No Slack webhook configured (set SLACK_WEBHOOK_BASE_URL or SLACK_WEBHOOK_URL)"

    return _post_with_retry(url, payload)


def _post_with_retry(url: str, payload: dict) -> tuple[bool, str | None]:
    """
    POST to a URL with exponential backoff retry on transient failures.

    Retries on:
      - HTTP 429 (rate limited) — honors Retry-After header
      - HTTP 5xx (server error)

    Does NOT retry on:
      - HTTP 2xx (success)
      - HTTP 4xx other than 429 (client error — our problem, retrying won't help)
      - Connection errors after max retries
    """
    backoff = SLACK_INITIAL_BACKOFF
    last_error = None

    for attempt in range(SLACK_MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=SLACK_REQUEST_TIMEOUT)

            if resp.status_code == 200:
                return True, None

            if resp.status_code == 429 or resp.status_code >= 500:
                # Transient failure — retry
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait_time = max(float(retry_after), backoff)
                else:
                    wait_time = backoff

                last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                logger.warning(
                    f"Slack POST failed (attempt {attempt + 1}/{SLACK_MAX_RETRIES + 1}): "
                    f"{last_error}. Retrying in {wait_time:.1f}s"
                )

                if attempt < SLACK_MAX_RETRIES:
                    time.sleep(wait_time)
                    backoff *= SLACK_BACKOFF_MULTIPLIER
                continue

            # Non-retryable error (4xx other than 429)
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"

        except requests.exceptions.RequestException as e:
            last_error = f"Connection error: {str(e)}"
            logger.warning(
                f"Slack POST failed (attempt {attempt + 1}/{SLACK_MAX_RETRIES + 1}): "
                f"{last_error}. Retrying in {backoff:.1f}s"
            )
            if attempt < SLACK_MAX_RETRIES:
                time.sleep(backoff)
                backoff *= SLACK_BACKOFF_MULTIPLIER

    # Exhausted all retries
    return False, f"Max retries exceeded. Last error: {last_error}"
