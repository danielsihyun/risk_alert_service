"""
Integration tests for the engine orchestrator and API endpoints.

Covers:
  - POST /runs end-to-end with mocked Slack
  - GET /runs/{run_id} response shape and counts
  - POST /preview returns alerts without persisting
  - GET /health
  - Replay safety: re-running same month skips already-sent alerts
  - Retry on failed alerts: previously failed alerts are retried
  - Unknown region: null/unmapped regions recorded as failed
  - Dry run: alerts computed and persisted but not sent
  - Error handling: bad URI scheme, missing file
"""

import datetime
import os
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

# Override config BEFORE importing app modules so tests use
# a fresh temp database and no real Slack endpoint.
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp_db.close()

os.environ["DATABASE_URL"] = f"sqlite:///{_tmp_db.name}"
os.environ["SLACK_WEBHOOK_BASE_URL"] = "http://mock-slack:9000/slack/webhook"
os.environ["ARR_THRESHOLD"] = "0"  # include all accounts in tests

from app.main import app
from app.db import init_db, get_session, AlertOutcome, Run, Base, engine as db_engine

client = TestClient(app)


# Fixtures

def _make_parquet(rows: list[dict]) -> str:
    """Write rows to a temp Parquet file and return the file:// URI."""
    defaults = {
        "account_name": "Test Account",
        "account_region": "AMER",
        "status": "Healthy",
        "renewal_date": None,
        "account_owner": "owner@test.com",
        "arr": 50000,
        "updated_at": datetime.datetime(2026, 1, 1, 0, 0, 0),
    }
    full_rows = [{**defaults, **r} for r in rows]

    df = pd.DataFrame(full_rows)
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    if "renewal_date" in df.columns:
        df["renewal_date"] = pd.to_datetime(df["renewal_date"])

    table = pa.Table.from_pandas(df, preserve_index=False)
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    pq.write_table(table, tmp.name)
    tmp.close()
    return f"file://{tmp.name}"


@pytest.fixture(autouse=True)
def _reset_db():
    """Drop and recreate all tables between tests for isolation."""
    Base.metadata.drop_all(db_engine)
    Base.metadata.create_all(db_engine)
    yield
    # Cleanup after test
    Base.metadata.drop_all(db_engine)


@pytest.fixture
def simple_parquet():
    """A minimal parquet with 2 at-risk accounts (1 AMER, 1 null region)."""
    uri = _make_parquet([
        {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
         "account_region": "AMER", "arr": 50000},
        {"account_id": "acct-2", "month": "2026-01-01", "status": "At Risk",
         "account_region": None, "arr": 30000},
        {"account_id": "acct-3", "month": "2026-01-01", "status": "Healthy",
         "account_region": "AMER", "arr": 40000},
    ])
    yield uri
    os.unlink(uri.replace("file://", ""))


@pytest.fixture
def multi_month_parquet():
    """3 months of history for duration calculation testing."""
    uri = _make_parquet([
        {"account_id": "acct-1", "month": "2025-11-01", "status": "At Risk",
         "account_region": "EMEA", "arr": 60000},
        {"account_id": "acct-1", "month": "2025-12-01", "status": "At Risk",
         "account_region": "EMEA", "arr": 60000},
        {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
         "account_region": "EMEA", "arr": 60000},
    ])
    yield uri
    os.unlink(uri.replace("file://", ""))


# Health

class TestHealth:
    def test_health(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


# Preview

class TestPreview:
    def test_preview_returns_alerts(self, simple_parquet):
        resp = client.post("/preview", json={
            "source_uri": simple_parquet,
            "month": "2026-01-01",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_alerts"] == 2
        assert data["rows_scanned"] == 3

        # Verify routable flag
        alerts_by_id = {a["account_id"]: a for a in data["alerts"]}
        assert alerts_by_id["acct-1"]["routable"] is True
        assert alerts_by_id["acct-1"]["channel"] == "amer-risk-alerts"
        assert alerts_by_id["acct-2"]["routable"] is False
        assert alerts_by_id["acct-2"]["channel"] is None

    def test_preview_does_not_persist(self, simple_parquet):
        """Preview should not create any run or alert_outcome records."""
        client.post("/preview", json={
            "source_uri": simple_parquet,
            "month": "2026-01-01",
        })
        session = get_session()
        try:
            assert session.query(Run).count() == 0
            assert session.query(AlertOutcome).count() == 0
        finally:
            session.close()

    def test_preview_with_duration(self, multi_month_parquet):
        resp = client.post("/preview", json={
            "source_uri": multi_month_parquet,
            "month": "2026-01-01",
        })
        data = resp.json()
        assert data["total_alerts"] == 1
        assert data["alerts"][0]["duration_months"] == 3
        assert data["alerts"][0]["risk_start_month"] == "2025-11-01"

    def test_preview_missing_file(self):
        resp = client.post("/preview", json={
            "source_uri": "file:///nonexistent.parquet",
            "month": "2026-01-01",
        })
        assert resp.status_code == 404

    def test_preview_bad_scheme(self):
        resp = client.post("/preview", json={
            "source_uri": "ftp:///bad.parquet",
            "month": "2026-01-01",
        })
        assert resp.status_code == 400


# Runs — full pipeline with mocked Slack

class TestRuns:
    @patch("app.slack.requests.post")
    def test_full_run_sends_alerts(self, mock_post, simple_parquet):
        """Full run: 1 sent (AMER), 1 failed (null region)."""
        mock_post.return_value = MagicMock(status_code=200)

        resp = client.post("/runs", json={
            "source_uri": simple_parquet,
            "month": "2026-01-01",
        })
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        # Verify Slack was called exactly once (only routable account)
        assert mock_post.call_count == 1
        call_url = mock_post.call_args[0][0]
        assert "amer-risk-alerts" in call_url

        # Check run results
        results = client.get(f"/runs/{run_id}").json()
        assert results["status"] == "succeeded"
        assert results["counts"]["alerts_sent"] == 1
        assert results["counts"]["failed_deliveries"] == 1
        assert results["counts"]["unknown_region"] == 1

    @patch("app.slack.requests.post")
    def test_run_returns_run_id(self, mock_post, simple_parquet):
        mock_post.return_value = MagicMock(status_code=200)
        resp = client.post("/runs", json={
            "source_uri": simple_parquet,
            "month": "2026-01-01",
        })
        assert resp.status_code == 200
        assert "run_id" in resp.json()
        assert len(resp.json()["run_id"]) > 0

    def test_run_missing_file(self):
        resp = client.post("/runs", json={
            "source_uri": "file:///nonexistent.parquet",
            "month": "2026-01-01",
        })
        assert resp.status_code == 404

    def test_run_bad_scheme(self):
        resp = client.post("/runs", json={
            "source_uri": "ftp:///bad.parquet",
            "month": "2026-01-01",
        })
        assert resp.status_code == 400

    def test_get_nonexistent_run(self):
        resp = client.get("/runs/nonexistent-id")
        assert resp.status_code == 404


# Replay safety

class TestReplaySafety:
    @patch("app.slack.requests.post")
    def test_replay_skips_already_sent(self, mock_post, simple_parquet):
        """Running the same month twice: second run skips sent alerts."""
        mock_post.return_value = MagicMock(status_code=200)

        # First run
        resp1 = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id_1 = resp1.json()["run_id"]
        results1 = client.get(f"/runs/{run_id_1}").json()
        assert results1["counts"]["alerts_sent"] == 1

        # Reset mock to track second run calls
        mock_post.reset_mock()

        # Second run — same month
        resp2 = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id_2 = resp2.json()["run_id"]
        results2 = client.get(f"/runs/{run_id_2}").json()

        # acct-1 was sent in run 1, should be skipped now
        assert results2["counts"]["skipped_replay"] == 1
        assert results2["counts"]["alerts_sent"] == 0
        # Slack should NOT have been called for acct-1
        assert mock_post.call_count == 0

    @patch("app.slack.requests.post")
    def test_replay_retries_failed(self, mock_post, simple_parquet):
        """Previously failed (Slack error) alerts are retried on replay."""
        # First run: Slack fails
        mock_post.return_value = MagicMock(status_code=500, text="server error",
                                           headers={})

        resp1 = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id_1 = resp1.json()["run_id"]
        results1 = client.get(f"/runs/{run_id_1}").json()
        # acct-1 should have failed (Slack 500)
        assert results1["counts"]["alerts_sent"] == 0

        # Second run: Slack succeeds
        mock_post.reset_mock()
        mock_post.return_value = MagicMock(status_code=200)

        resp2 = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id_2 = resp2.json()["run_id"]
        results2 = client.get(f"/runs/{run_id_2}").json()

        # acct-1 should now be sent (retry succeeded)
        assert results2["counts"]["alerts_sent"] == 1
        assert results2["counts"]["skipped_replay"] == 0


# Dry run

class TestDryRun:
    @patch("app.slack.requests.post")
    def test_dry_run_does_not_send(self, mock_post, simple_parquet):
        """Dry run persists outcomes but does not call Slack."""
        resp = client.post("/runs", json={
            "source_uri": simple_parquet,
            "month": "2026-01-01",
            "dry_run": True,
        })
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        # Slack should not have been called at all
        mock_post.assert_not_called()

        results = client.get(f"/runs/{run_id}").json()
        assert results["status"] == "succeeded"
        assert results["dry_run"] is True
        # No alerts sent, but unknown_region still fails
        assert results["counts"]["alerts_sent"] == 0

    @patch("app.slack.requests.post")
    def test_dry_run_then_real_run(self, mock_post, simple_parquet):
        """After a dry run, a real run should send the alerts (not skip)."""
        # Dry run first
        client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01", "dry_run": True,
        })
        mock_post.assert_not_called()

        # Real run
        mock_post.return_value = MagicMock(status_code=200)
        resp = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id = resp.json()["run_id"]
        results = client.get(f"/runs/{run_id}").json()

        # acct-1 should be sent (dry_run outcome is overwritten)
        assert results["counts"]["alerts_sent"] == 1
        assert results["counts"]["skipped_replay"] == 0


# Slack retry logic

class TestSlackRetry:
    @patch("app.slack.time.sleep")  # don't actually sleep in tests
    @patch("app.slack.requests.post")
    def test_retry_on_429(self, mock_post, mock_sleep, simple_parquet):
        """429 → 200 on second attempt should succeed."""
        mock_post.side_effect = [
            MagicMock(status_code=429, text="rate limited",
                      headers={"Retry-After": "1"}),
            MagicMock(status_code=200),
        ]

        resp = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id = resp.json()["run_id"]
        results = client.get(f"/runs/{run_id}").json()

        assert results["counts"]["alerts_sent"] == 1
        assert mock_post.call_count == 2  # 429 + 200
        mock_sleep.assert_called_once()

    @patch("app.slack.time.sleep")
    @patch("app.slack.requests.post")
    def test_retry_on_500(self, mock_post, mock_sleep, simple_parquet):
        """500 → 500 → 200 on third attempt should succeed."""
        mock_post.side_effect = [
            MagicMock(status_code=500, text="server error", headers={}),
            MagicMock(status_code=500, text="server error", headers={}),
            MagicMock(status_code=200),
        ]

        resp = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id = resp.json()["run_id"]
        results = client.get(f"/runs/{run_id}").json()

        assert results["counts"]["alerts_sent"] == 1
        assert mock_post.call_count == 3

    @patch("app.slack.time.sleep")
    @patch("app.slack.requests.post")
    def test_no_retry_on_4xx(self, mock_post, mock_sleep, simple_parquet):
        """Non-429 4xx errors should fail immediately without retry."""
        mock_post.return_value = MagicMock(status_code=400,
                                           text="bad request", headers={})

        resp = client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })
        run_id = resp.json()["run_id"]
        results = client.get(f"/runs/{run_id}").json()

        assert results["counts"]["alerts_sent"] == 0
        assert results["counts"]["failed_deliveries"] >= 1
        # Should NOT have retried
        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()


# Unknown region handling

class TestUnknownRegion:
    @patch("app.slack.requests.post")
    @patch("app.engine.send_unknown_region_notification")
    def test_unknown_region_triggers_email(self, mock_email, mock_post,
                                           simple_parquet):
        """Null-region accounts should trigger the aggregated email."""
        mock_post.return_value = MagicMock(status_code=200)
        mock_email.return_value = ""

        client.post("/runs", json={
            "source_uri": simple_parquet, "month": "2026-01-01",
        })

        mock_email.assert_called_once()
        alerts_arg = mock_email.call_args[0][0]
        assert len(alerts_arg) == 1
        assert alerts_arg[0].account_id == "acct-2"

    @patch("app.slack.requests.post")
    def test_unmapped_region_fails(self, mock_post):
        """A region not in the config map should fail as unknown_region."""
        uri = _make_parquet([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "account_region": "LATAM", "arr": 50000},
        ])
        mock_post.return_value = MagicMock(status_code=200)

        try:
            resp = client.post("/runs", json={
                "source_uri": uri, "month": "2026-01-01",
            })
            run_id = resp.json()["run_id"]
            results = client.get(f"/runs/{run_id}").json()

            assert results["counts"]["unknown_region"] == 1
            assert results["counts"]["alerts_sent"] == 0
            # Slack should not have been called
            mock_post.assert_not_called()
        finally:
            os.unlink(uri.replace("file://", ""))
