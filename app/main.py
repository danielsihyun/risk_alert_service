"""
FastAPI application — the HTTP interface to the alert pipeline.

Endpoints:
  POST /runs          — execute a full alert run (synchronous)
  GET  /runs/{run_id} — retrieve persisted run results
  POST /preview       — compute alerts without sending or persisting
  GET  /health        — liveness check
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.db import init_db
from app.engine import execute_run, get_run_results, get_preview_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize the database on application startup."""
    init_db()
    yield


app = FastAPI(
    title="Risk Alert Service",
    description="Monthly account risk alert pipeline with Slack integration",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    source_uri: str
    month: str       # "YYYY-MM-DD", always first of month
    dry_run: bool = False


class PreviewRequest(BaseModel):
    source_uri: str
    month: str       # "YYYY-MM-DD", always first of month


class RunResponse(BaseModel):
    run_id: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Liveness check."""
    return {"ok": True}


@app.post("/runs", response_model=RunResponse)
def create_run(req: RunRequest):
    """
    Execute a full alert processing run.

    - Reads Parquet from source_uri
    - Computes alerts for the specified month
    - Sends Slack messages (unless dry_run=true)
    - Persists run metadata and alert outcomes
    - Returns run_id after completion

    The request blocks until processing is complete.
    """
    try:
        run_id = execute_run(
            source_uri=req.source_uri,
            month=req.month,
            dry_run=req.dry_run,
        )
        return RunResponse(run_id=run_id)

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Data file not found: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.getLogger(__name__).error(f"Run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    """
    Retrieve persisted results for a completed run.

    Returns status, counts, and sample alerts/errors.
    """
    results = get_run_results(run_id)
    if results is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return results


@app.post("/preview")
def preview(req: PreviewRequest):
    """
    Preview alerts without sending to Slack or persisting.

    Same computation as /runs, but returns the alert list directly
    so the user can inspect what would be sent before committing.
    """
    try:
        return get_preview_results(
            source_uri=req.source_uri,
            month=req.month,
        )

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Data file not found: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.getLogger(__name__).error(f"Preview failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")
