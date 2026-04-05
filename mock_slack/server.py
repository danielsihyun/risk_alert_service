import json
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, Request, Response, HTTPException

# ----------------------------
# Config (via env vars)
# ----------------------------
# Probability of returning a 429 (rate limit)
FAIL_RATE_429 = float(os.getenv("MOCK_SLACK_FAIL_RATE_429", "0.10"))  # 10%
# Probability of returning a 500 (server error)
FAIL_RATE_500 = float(os.getenv("MOCK_SLACK_FAIL_RATE_500", "0.05"))  # 5%

MIN_RETRY_AFTER_SEC = int(os.getenv("MOCK_SLACK_MIN_RETRY_AFTER", "1"))
MAX_RETRY_AFTER_SEC = int(os.getenv("MOCK_SLACK_MAX_RETRY_AFTER", "5"))

# Where to write JSONL logs
LOG_PATH = os.getenv("MOCK_SLACK_LOG_PATH", "./mock_slack_requests.jsonl")

# Optional shared secret (recommended if exposed publicly)
AUTH_TOKEN = os.getenv("MOCK_SLACK_AUTH_TOKEN")  # if set, require header X-Mock-Slack-Token

app = FastAPI(title="Mock Slack Webhook Server")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_log(record: Dict[str, Any]) -> None:
    log_dir = os.path.dirname(LOG_PATH)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def maybe_fail() -> Response:
    """Randomly return a simulated transient failure."""
    r = random.random()
    if r < FAIL_RATE_500:
        return Response(content="mock slack: internal error", status_code=500)

    if r < FAIL_RATE_500 + FAIL_RATE_429:
        retry_after = random.randint(MIN_RETRY_AFTER_SEC, MAX_RETRY_AFTER_SEC)
        return Response(
            content="mock slack: rate limited",
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    return Response(status_code=200)


@app.get("/health")
def health():
    return {"ok": True, "time": utc_now_iso()}


@app.post("/slack/webhook/{channel}")
async def webhook(channel: str, request: Request):
    # Optional auth
    if AUTH_TOKEN:
        token = request.headers.get("X-Mock-Slack-Token")
        if token != AUTH_TOKEN:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # Accept any JSON payload
    try:
        payload = await request.json()
    except Exception:
        payload = {"_raw_body": (await request.body()).decode("utf-8", errors="replace")}

    resp = maybe_fail()

    record = {
        "ts": utc_now_iso(),
        "channel": channel,
        "status_code": resp.status_code,
        "retry_after": resp.headers.get("Retry-After"),
        "headers": {
            "user-agent": request.headers.get("user-agent"),
            "content-type": request.headers.get("content-type"),
        },
        "payload": payload,
    }
    append_log(record)

    return resp


@app.get("/logs")
def logs(limit: int = 200):
    """Return the last N log records (newest last)."""
    if not os.path.exists(LOG_PATH):
        return {"log_path": LOG_PATH, "records": []}

    with open(LOG_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()[-limit:]

    records = [json.loads(line) for line in lines]
    return {"log_path": LOG_PATH, "records": records}
