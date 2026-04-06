"""
Centralized configuration loaded from environment variables.

All settings that change between environments (local dev, staging, production,
different customer deployments) are configured here. Nothing is hardcoded
in the business logic modules.

Required env vars for production:
  - SLACK_WEBHOOK_BASE_URL or SLACK_WEBHOOK_URL (at least one)

Optional env vars (have sensible defaults):
  - ARR_THRESHOLD: minimum ARR to generate an alert (default: 10000)
  - DETAILS_BASE_URL: base URL for account detail links
  - REGION_CHANNEL_MAP: JSON string mapping regions to Slack channels
  - DATABASE_URL: SQLite connection string
  - SUPPORT_EMAIL: email for unknown region notifications
"""

import json
import os


# Slack

# Base URL mode (for mock server or custom routing): POST to {base_url}/{channel}
SLACK_WEBHOOK_BASE_URL: str | None = os.getenv("SLACK_WEBHOOK_BASE_URL")

# Single webhook mode (for real Slack incoming webhooks): POST to this URL
SLACK_WEBHOOK_URL: str | None = os.getenv("SLACK_WEBHOOK_URL")


# Base URL takes precedence per the spec
def get_slack_mode() -> str:
    if SLACK_WEBHOOK_BASE_URL:
        return "base_url"
    elif SLACK_WEBHOOK_URL:
        return "single_webhook"
    else:
        return "none"


# Region → Channel routing

_DEFAULT_REGION_CHANNEL_MAP = {
    "AMER": "amer-risk-alerts",
    "EMEA": "emea-risk-alerts",
    "APAC": "apac-risk-alerts",
}


def _load_region_map() -> dict[str, str]:
    raw = os.getenv("REGION_CHANNEL_MAP")
    if raw:
        parsed = json.loads(raw)
        # Support both {"AMER": "channel"} and {"regions": {"AMER": "channel"}}
        if "regions" in parsed:
            return parsed["regions"]
        return parsed
    return _DEFAULT_REGION_CHANNEL_MAP


REGION_CHANNEL_MAP: dict[str, str] = _load_region_map()


# Alert thresholds

# Accounts with ARR below this threshold are excluded from alerts to reduce noise.
# Default: 10,000 — in the sample data, the only accounts below this are those
# with ARR=0 (likely inactive/churned), so this filters noise without missing
# legitimate at-risk accounts.
ARR_THRESHOLD: int = int(os.getenv("ARR_THRESHOLD", "10000"))


# URLs

# Base URL for account detail links in Slack messages
DETAILS_BASE_URL: str = os.getenv(
    "DETAILS_BASE_URL", "https://app.yourcompany.com/accounts"
)


# Persistence

DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///risk_alerts.db")


# Email (stub)

# In production, this would be the recipient for unknown-region notifications.
# Implementation uses a logging stub; see app/email.py for details.
SUPPORT_EMAIL: str = os.getenv("SUPPORT_EMAIL", "support@quadsci.ai")


# Retry settings for Slack

SLACK_MAX_RETRIES: int = int(os.getenv("SLACK_MAX_RETRIES", "3"))
SLACK_INITIAL_BACKOFF: float = float(os.getenv("SLACK_INITIAL_BACKOFF", "1.0"))
SLACK_BACKOFF_MULTIPLIER: float = float(os.getenv("SLACK_BACKOFF_MULTIPLIER", "2.0"))
SLACK_REQUEST_TIMEOUT: int = int(os.getenv("SLACK_REQUEST_TIMEOUT", "10"))
