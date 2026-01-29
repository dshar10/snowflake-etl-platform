"""
Snowflake Python Stored Procedure Skeleton

Purpose:
- Run an ETL pipeline inside Snowflake
- Write a standardized audit log entry (SUCCESS / FAILED)
- Serve as a template for all future Snowflake-native ETLs

This file is written as "procedure code" that can be pasted into:
CREATE OR REPLACE PROCEDURE ... LANGUAGE PYTHON ... HANDLER='run'
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
import json


# ============================================================
# Helpers: time + config safety
# ============================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_str(config: Dict[str, Any], key: str, default: Optional[str] = None) -> str:
    """
    Read a required string from config.
    If missing/blank, raise a clear error.
    """
    config = config or {}
    val = config.get(key, default)
    if val is None or str(val).strip() == "":
        raise ValueError(f"Missing required config key: {key}")
    return str(val).strip()


def _safe_variant(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only safe, non-secret config keys for logging.
    Redacts common secret-like fields.
    """
    deny = {"token", "apikey", "api_key", "password", "secret"}
    clean: Dict[str, Any] = {}
    for k, v in (obj or {}).items():
        if str(k).lower() in deny:
            clean[k] = "***REDACTED***"
        else:
            clean[k] = v
    return clean


# ------------------------------------------------------------
# This block performs a tiny but real ETL action whose purpose
# is to:
#   1) Prove the pipeline executed successfully
#   2) Produce a measurable metric for audit logging
#
# Key ideas:
# - Pipelines are config-driven (no hardcoded table names)
# - All table references should be fully qualified (FQN)
# - Snowpark `session.sql()` executes SQL inside Snowflake
# - `.collect()` materializes results back into Python
# - Row counts are commonly used as a generic ETL metric
#
# This pattern scales naturally:
# - COUNT(*)  -> MERGE / INSERT / DELETE metrics
# - Single table -> multi-step transformations
# - Manual run -> task-orchestrated execution
#
# Snowflake Core concepts covered:
# - Python Stored Procedures
# - Snowpark session execution
# - Fully Qualified Names (DB.SCHEMA.OBJECT)
# - Audit logging with TIMESTAMP_TZ (UTC)
# ============================================================
def _audit_table_fqn(config: Dict[str, Any]) -> str:
    """
    Build Fully Qualified Name for audit table: <DB>.<SCHEMA>.<TABLE>
    """
    audit_db = _require_str(config, "audit_db", default=None)
    audit_schema = _require_str(config, "audit_schema", default=None)
    audit_table = _require_str(config, "audit_table", default="UTIL_ETL_AUDIT_LOG")
    return f"{audit_db}.{audit_schema}.{audit_table}"


def _insert_audit_row(session, audit_table_fqn: str, row: Dict[str, Any]) -> None:
    """
    Insert a single audit record into the configured audit table.

    Args:
      session: Snowpark Session
      audit_table_fqn: Fully-qualified audit table name (DB.SCHEMA.TABLE)
      row: Dict with keys:
        - pipeline_name, status, started_at, finished_at, rows_processed, message, context_json

    Returns:
      None
    """
    # Important: table name cannot be parameter-bound, so we format it carefully.
    session.sql(
        f"""
        INSERT INTO {audit_table_fqn}
        (PIPELINE_NAME, STATUS, STARTED_AT, FINISHED_AT, ROWS_PROCESSED, MESSAGE, CONTEXT)
        SELECT
          %(pipeline_name)s,
          %(status)s,
          %(started_at)s::TIMESTAMP_TZ,
          %(finished_at)s::TIMESTAMP_TZ,
          %(rows_processed)s,
          %(message)s,
          PARSE_JSON(%(context_json)s)
        """
    ).bind(row).collect()


# ============================================================
# NOTES (drop-in template for all procedures)
# ============================================================
# NOTES: Minimal ETL + Audit Metrics Pattern
# - Do a small "real" action (like COUNT(*)) to prove the pipeline ran and produce a metric.
# - Use config-driven inputs, avoid hardcoding object names.
# - Use Snowpark session.sql() and collect() to execute SQL and retrieve results.
#
# NOTES: try / except ETL Control Flow + Audit Logging
# - try: run main ETL logic; log SUCCESS audit row.
# - except: capture error; log FAILED audit row; re-raise so Tasks show FAILED.
# - Never swallow exceptions after logging.
# - Always log timestamps in UTC (TIMESTAMP_TZ).
# ============================================================


def run(session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handler for Snowflake stored procedure.

    Expected config keys:
      - pipeline_name (optional)
      - audit_db (required)
      - audit_schema (required)
      - audit_table (optional; default UTIL_ETL_AUDIT_LOG)
      - count_table (required for this demo action)
    """
    config = config or {}

    pipeline_name = str(config.get("pipeline_name", "unknown_pipeline")).strip()
    started = _utcnow()
    rows_processed: Optional[int] = None

    audit_table_fqn = _audit_table_fqn(config)
# ============================================================
# NOTES: try / except ETL Control Flow + Audit Logging
# ------------------------------------------------------------
# This stored procedure uses a try / except pattern to ensure:
#   1) Business logic is executed safely
#   2) Every run produces an audit record
#   3) Failures are captured with context and error messages
#
# try:
#   - Contains the main ETL logic (ingest, transform, merge, etc.)
#   - Computes metrics such as rows_processed
#   - Captures successful completion timestamp
#   - Writes a SUCCESS audit record
#
# except Exception as e:
#   - Catches ANY runtime failure (SQL, API, Python, config)
#   - Captures failure timestamp
#   - Writes a FAILED audit record
#   - Re-raises the exception so Snowflake marks the run as FAILED
#
# Why this matters in Snowflake:
# - Tasks rely on exceptions to determine success/failure
# - Silent failures break orchestration and monitoring
# - Audit tables provide observability beyond task history
#
# Best practices applied:
# - Always log STARTED_AT and FINISHED_AT (UTC)
# - Always log STATUS = SUCCESS or FAILED
# - Never swallow exceptions after logging
# - Keep audit logging outside business logic
#
# Snowflake Core concepts covered:
# - Python Stored Procedure control flow
# - Exception handling
# - Task failure propagation
# - Operational auditing patterns
# ============================================================

    try:
        # --------------------------------------------
        # Minimal demo ETL action: count rows in a table
        # --------------------------------------------
        count_table = _require_str(config, "count_table", default=None)

        rows_processed = int(
            session.sql(f"SELECT COUNT(*) AS C FROM {count_table}").collect()[0]["C"]
        )

        finished = _utcnow()

        # SUCCESS audit row
        audit_row = {
            "pipeline_name": pipeline_name,
            "status": "SUCCESS",
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "rows_processed": rows_processed,
            "message": None,
            "context_json": json.dumps(_safe_variant(config)),
        }
        _insert_audit_row(session, audit_table_fqn, audit_row)

        return {
            "pipeline_name": pipeline_name,
            "status": "SUCCESS",
            "rows_processed": rows_processed,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
        }

    except Exception as e:
        finished = _utcnow()

        # FAILED audit row
        audit_row = {
            "pipeline_name": pipeline_name,
            "status": "FAILED",
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "rows_processed": rows_processed,
            "message": str(e),
            "context_json": json.dumps(_safe_variant(config)),
        }

        # Try to log failure, but don't mask the original error
        try:
            _insert_audit_row(session, audit_table_fqn, audit_row)
        except Exception:
            pass

        # Re-raise so Snowflake Tasks/UI show FAILED properly
        raise


        return {
            "pipeline_name": pipeline_name,
            "status": "FAILED",
            "error": str(e),
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
        }
