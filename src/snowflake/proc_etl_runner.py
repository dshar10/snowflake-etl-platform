"""
Snowflake Python Stored Procedure Skeleton

Purpose:
- Run an ETL pipeline inside Snowflake
- Write a standardized audit log entry (SUCCESS / FAILED)
- Serve as a template for all future Snowflake-native ETLs

This file is written as "procedure code" that can be pasted into
CREATE OR REPLACE PROCEDURE ... LANGUAGE PYTHON ... HANDLER='run'
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
import json

# NOTE:
# UTIL_ETL_AUDIT_LOG is assumed to exist (see sql/audit_logger.sql).
# In production, we will fully qualify it: <DB>.<SCHEMA>.UTIL_ETL_AUDIT_LOG


def _utcnow():
    return datetime.now(timezone.utc)


def _safe_variant(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only safe, non-secret config keys for logging.
    (Later: enforce a broader deny-list for tokens, passwords, etc.)
    """
    deny = {"token", "apikey", "api_key", "password", "secret"}
    clean = {}
    for k, v in (obj or {}).items():
        if str(k).lower() in deny:
            clean[k] = "***REDACTED***"
        else:
            clean[k] = v
    return clean


def _insert_audit_row(session, row: Dict[str, Any]) -> None:
    """
    Minimal audit logger insert.
    session: Snowpark Session
    row: dict matching UTIL_ETL_AUDIT_LOG columns
    """
    # TODO: Fully qualify table name (DB.SCHEMA.UTIL_ETL_AUDIT_LOG) via config

    session.sql(
        """
        INSERT INTO UTIL_ETL_AUDIT_LOG
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


def run(session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Snowflake procedure entrypoint.

    Parameters
    - session: Snowpark Session (provided by Snowflake)
    - config: VARIANT/OBJECT passed in from CALL ... (becomes dict)

    Returns
    - VARIANT-like dict with run status and metadata
    """
    config = config or {}
    pipeline_name = str(config.get("pipeline_name", "unknown_pipeline"))
    started = _utcnow()

    rows_processed = None

    try:
        # TODO (Day 3+): call your ETL framework logic here
        # Example later:
        # etl = SomeSnowflakeETL(config, session=session)
        # result = etl.run()

        rows_processed = 0  # placeholder
        finished = _utcnow()

        audit_row = {
            "pipeline_name": pipeline_name,
            "status": "SUCCESS",
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "rows_processed": rows_processed,
            "message": None,
            "context_json": json.dumps(_safe_variant(config)),
        }
        _insert_audit_row(session, audit_row)

        return {
            "pipeline_name": pipeline_name,
            "status": "SUCCESS",
            "rows_processed": rows_processed,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
        }

    except Exception as e:
        finished = _utcnow()

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
            _insert_audit_row(session, audit_row)
        except Exception:
            pass

        return {
            "pipeline_name": pipeline_name,
            "status": "FAILED",
            "error": str(e),
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
        }
