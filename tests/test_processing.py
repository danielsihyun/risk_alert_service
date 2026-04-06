"""
Tests for core business logic in app/processing.py.

Covers:
  - Duration calculation (consecutive months, gap breaks streak, status change, duration=1)
  - Deduplication (latest updated_at wins)
  - ARR threshold filtering
  - Nullable field handling (region, owner, renewal_date)

Run with: pytest
"""

import datetime

import pandas as pd
import pyarrow as pa

from app.processing import process_month, _compute_duration, _prev_month


def _make_table(rows: list[dict]) -> pa.Table:
    """Build a PyArrow Table from a list of row dicts with sensible defaults."""
    defaults = {
        "account_name": "Test Account",
        "account_region": "AMER",
        "status": "Healthy",
        "renewal_date": None,
        "account_owner": None,
        "arr": 50000,
        "updated_at": datetime.datetime(2026, 1, 1, 0, 0, 0),
    }
    full_rows = [{**defaults, **r} for r in rows]

    df = pd.DataFrame(full_rows)
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    if "renewal_date" in df.columns:
        df["renewal_date"] = pd.to_datetime(df["renewal_date"])

    return pa.Table.from_pandas(df, preserve_index=False)


class TestPrevMonth:
    def test_mid_year(self):
        assert _prev_month(datetime.date(2026, 6, 1)) == datetime.date(2026, 5, 1)

    def test_january_wraps_to_december(self):
        assert _prev_month(datetime.date(2026, 1, 1)) == datetime.date(2025, 12, 1)


class TestDuration:
    """Test _compute_duration with hand-crafted status lookups."""

    def test_single_month_at_risk(self):
        """No prior history -> duration = 1."""
        lookup = {("acct-1", datetime.date(2026, 1, 1)): "At Risk"}
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 1
        assert start == "2026-01-01"

    def test_consecutive_months(self):
        """Three consecutive At Risk months -> duration = 3."""
        lookup = {
            ("acct-1", datetime.date(2025, 11, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 12, 1)): "At Risk",
            ("acct-1", datetime.date(2026, 1, 1)): "At Risk",
        }
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 3
        assert start == "2025-11-01"

    def test_status_change_breaks_streak(self):
        """Healthy month in between -> streak resets."""
        lookup = {
            ("acct-1", datetime.date(2025, 10, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 11, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 12, 1)): "Healthy",
            ("acct-1", datetime.date(2026, 1, 1)): "At Risk",
        }
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 1
        assert start == "2026-01-01"

    def test_missing_month_breaks_streak(self):
        """Gap in data (missing month) -> streak resets."""
        lookup = {
            ("acct-1", datetime.date(2025, 10, 1)): "At Risk",
            # 2025-11 missing
            ("acct-1", datetime.date(2025, 12, 1)): "At Risk",
            ("acct-1", datetime.date(2026, 1, 1)): "At Risk",
        }
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 2
        assert start == "2025-12-01"

    def test_year_boundary(self):
        """Streak spanning year boundary."""
        lookup = {
            ("acct-1", datetime.date(2025, 11, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 12, 1)): "At Risk",
            ("acct-1", datetime.date(2026, 1, 1)): "At Risk",
        }
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 3
        assert start == "2025-11-01"

    def test_spec_example(self):
        """
        Exact example from the spec:
          2025-10 At Risk -> 2025-11 At Risk -> 2025-12 Healthy -> 2026-01 At Risk
          Expected duration: 1
        """
        lookup = {
            ("acct-1", datetime.date(2025, 10, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 11, 1)): "At Risk",
            ("acct-1", datetime.date(2025, 12, 1)): "Healthy",
            ("acct-1", datetime.date(2026, 1, 1)): "At Risk",
        }
        duration, start = _compute_duration("acct-1", datetime.date(2026, 1, 1), lookup)
        assert duration == 1
        assert start == "2026-01-01"


class TestDeduplication:
    def test_keeps_latest_updated_at(self):
        """Two rows for same (account_id, month) -> keep the one with latest updated_at."""
        table = _make_table([
            {
                "account_id": "acct-1",
                "month": "2026-01-01",
                "status": "Healthy",
                "updated_at": datetime.datetime(2026, 1, 1, 10, 0, 0),
            },
            {
                "account_id": "acct-1",
                "month": "2026-01-01",
                "status": "At Risk",
                "updated_at": datetime.datetime(2026, 1, 1, 12, 0, 0),  # later wins
            },
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.duplicates_found == 1
        assert len(result.alerts) == 1
        assert result.alerts[0].status == "At Risk"

    def test_reports_duplicate_count(self):
        """Three rows for same key -> 2 duplicates removed."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "updated_at": datetime.datetime(2026, 1, 1, 8, 0, 0)},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "updated_at": datetime.datetime(2026, 1, 1, 9, 0, 0)},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "updated_at": datetime.datetime(2026, 1, 1, 10, 0, 0)},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.duplicates_found == 2


class TestARRThreshold:
    def test_filters_below_threshold(self):
        """Accounts with ARR below threshold are excluded."""
        table = _make_table([
            {"account_id": "low-arr", "month": "2026-01-01", "status": "At Risk", "arr": 5000},
            {"account_id": "high-arr", "month": "2026-01-01", "status": "At Risk", "arr": 50000},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=10000)
        assert len(result.alerts) == 1
        assert result.alerts[0].account_id == "high-arr"

    def test_zero_threshold_includes_all(self):
        """ARR threshold of 0 includes everything."""
        table = _make_table([
            {"account_id": "zero-arr", "month": "2026-01-01", "status": "At Risk", "arr": 0},
            {"account_id": "high-arr", "month": "2026-01-01", "status": "At Risk", "arr": 50000},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert len(result.alerts) == 2

    def test_exact_threshold_included(self):
        """Account with ARR exactly at threshold is included (>=, not >)."""
        table = _make_table([
            {"account_id": "exact", "month": "2026-01-01", "status": "At Risk", "arr": 10000},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=10000)
        assert len(result.alerts) == 1


class TestNullableFields:
    def test_null_region(self):
        """Null region is preserved as None in the alert record."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "account_region": None},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.alerts[0].account_region is None

    def test_null_owner(self):
        """Null owner is preserved as None."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "account_owner": None},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.alerts[0].account_owner is None

    def test_null_renewal_date(self):
        """Null renewal_date is preserved as None."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk",
             "renewal_date": None},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.alerts[0].renewal_date is None


class TestProcessMonth:
    def test_no_at_risk_accounts(self):
        """All healthy -> no alerts."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2026-01-01", "status": "Healthy"},
            {"account_id": "acct-2", "month": "2026-01-01", "status": "Healthy"},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert len(result.alerts) == 0

    def test_only_target_month_alerts(self):
        """At Risk in a prior month but healthy in target -> no alert."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2025-12-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "Healthy"},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert len(result.alerts) == 0

    def test_duration_with_history(self):
        """Full pipeline: dedup + filter + duration across months."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2025-10-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2025-11-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2025-12-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk"},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert len(result.alerts) == 1
        assert result.alerts[0].duration_months == 4
        assert result.alerts[0].risk_start_month == "2025-10-01"

    def test_multiple_accounts(self):
        """Two accounts, different durations."""
        table = _make_table([
            # acct-1: 2 months at risk
            {"account_id": "acct-1", "month": "2025-12-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk"},
            # acct-2: 1 month at risk (was healthy before)
            {"account_id": "acct-2", "month": "2025-12-01", "status": "Healthy"},
            {"account_id": "acct-2", "month": "2026-01-01", "status": "At Risk"},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert len(result.alerts) == 2

        alerts_by_id = {a.account_id: a for a in result.alerts}
        assert alerts_by_id["acct-1"].duration_months == 2
        assert alerts_by_id["acct-2"].duration_months == 1

    def test_rows_scanned_counts_all_input_rows(self):
        """rows_scanned reflects the full input, not just target month."""
        table = _make_table([
            {"account_id": "acct-1", "month": "2025-11-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2025-12-01", "status": "At Risk"},
            {"account_id": "acct-1", "month": "2026-01-01", "status": "At Risk"},
            {"account_id": "acct-2", "month": "2026-01-01", "status": "Healthy"},
        ])
        result = process_month(table, "2026-01-01", arr_threshold=0)
        assert result.rows_scanned == 4
