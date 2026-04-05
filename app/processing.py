"""
Core business logic for processing account health data.

Responsibilities:
  1. Load only the rows needed from the Parquet file (target month + history)
  2. Deduplicate: if multiple rows for (account_id, month), keep latest updated_at
  3. Filter to "At Risk" accounts above the ARR threshold for the target month
  4. Compute consecutive At Risk duration by walking backward month-by-month
  5. Return structured alert data ready for Slack delivery

The duration calculation is the trickiest part. Rules from the spec:
  - Count backward from target month while status == "At Risk"
  - Stop when status changes OR a month is missing from the data
  - If no prior At Risk month exists -> duration = 1
"""

import datetime
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from app.config import ARR_THRESHOLD


@dataclass
class AlertRecord:
    """A single alert ready for routing and delivery."""
    account_id: str
    account_name: str
    account_region: str | None
    month: str                  # "YYYY-MM-DD"
    status: str
    renewal_date: str | None    # "YYYY-MM-DD" or None
    account_owner: str | None
    arr: int
    duration_months: int
    risk_start_month: str       # "YYYY-MM-DD"


@dataclass
class ProcessingResult:
    """Summary of the processing step, before Slack delivery."""
    alerts: list[AlertRecord]
    rows_scanned: int
    duplicates_found: int


def process_month(table: pa.Table, target_month: str, arr_threshold: int | None = None) -> ProcessingResult:
    """
    Main processing entry point.

    Args:
        table: PyArrow Table (already filtered at storage layer for efficiency,
               but may contain more rows than strictly needed)
        target_month: "YYYY-MM-DD" string, always first of month
        arr_threshold: minimum ARR to include (defaults to config value)

    Returns:
        ProcessingResult with alerts and metadata
    """
    if arr_threshold is None:
        arr_threshold = ARR_THRESHOLD

    target_date = datetime.date.fromisoformat(target_month)

    # Convert to pandas for processing
    df = table.to_pandas()
    rows_scanned = len(df)

    # Ensure month column is date type for consistent comparison
    df["month"] = pd.to_datetime(df["month"]).dt.date

    # ------------------------------------------------------------------
    # Step 1: Deduplicate — keep latest updated_at per (account_id, month)
    # ------------------------------------------------------------------
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    pre_dedup = len(df)
    df = df.sort_values("updated_at", ascending=False).drop_duplicates(
        subset=["account_id", "month"], keep="first"
    )
    duplicates_found = pre_dedup - len(df)

    # ------------------------------------------------------------------
    # Step 2: Filter to At Risk accounts for the target month
    # ------------------------------------------------------------------
    target_rows = df[(df["month"] == target_date) & (df["status"] == "At Risk")]

    # Apply ARR threshold
    if arr_threshold > 0:
        target_rows = target_rows[target_rows["arr"] >= arr_threshold]

    if target_rows.empty:
        return ProcessingResult(alerts=[], rows_scanned=rows_scanned, duplicates_found=duplicates_found)

    # ------------------------------------------------------------------
    # Step 3: Compute duration for each At Risk account
    # ------------------------------------------------------------------
    # Build a lookup: (account_id, month) -> status (from deduplicated data)
    status_lookup = df.set_index(["account_id", "month"])["status"].to_dict()

    alerts = []
    for _, row in target_rows.iterrows():
        account_id = row["account_id"]
        duration, start_month = _compute_duration(
            account_id, target_date, status_lookup
        )

        # Format nullable fields
        renewal = _format_date(row.get("renewal_date"))
        owner = row.get("account_owner")
        if pd.isna(owner):
            owner = None

        region = row.get("account_region")
        if pd.isna(region):
            region = None

        alerts.append(AlertRecord(
            account_id=account_id,
            account_name=row["account_name"],
            account_region=region,
            month=target_month,
            status="At Risk",
            renewal_date=renewal,
            account_owner=owner,
            arr=int(row["arr"]),
            duration_months=duration,
            risk_start_month=start_month,
        ))

    return ProcessingResult(
        alerts=alerts,
        rows_scanned=rows_scanned,
        duplicates_found=duplicates_found,
    )


def _compute_duration(
    account_id: str,
    target_date: datetime.date,
    status_lookup: dict,
) -> tuple[int, str]:
    """
    Walk backward from target_date month-by-month, counting consecutive
    "At Risk" months.

    Returns:
        (duration_months, risk_start_month as "YYYY-MM-DD")

    Rules:
        - Start at target month (already confirmed At Risk)
        - Step back one month at a time
        - Continue while status == "At Risk"
        - Stop if status changes OR month is missing from data
        - Minimum duration is 1 (the target month itself)
    """
    duration = 1
    current = target_date

    while True:
        prev = _prev_month(current)
        key = (account_id, prev)

        if key not in status_lookup:
            # Month missing from data — streak breaks
            break

        if status_lookup[key] != "At Risk":
            # Status changed — streak breaks
            break

        duration += 1
        current = prev

    # current is now the earliest At Risk month in the streak
    risk_start = current.strftime("%Y-%m-%d")
    return duration, risk_start


def _prev_month(d: datetime.date) -> datetime.date:
    """Return the first day of the previous month."""
    if d.month == 1:
        return datetime.date(d.year - 1, 12, 1)
    return datetime.date(d.year, d.month - 1, 1)


def _format_date(val) -> str | None:
    """Convert a date/timestamp/NaT to 'YYYY-MM-DD' string or None."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, str):
        return val
    return val.strftime("%Y-%m-%d") if hasattr(val, "strftime") else str(val)
