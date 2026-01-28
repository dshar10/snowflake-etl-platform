# Learning Notes – Data Engineering Platform

This document captures design decisions, reasoning, and learnings
while building the Snowflake-centric ETL platform.

---
############################## Day 2 ##############################
####################### PIPELINE FRAMEWORK ########################
###################################################################

## Audit Logging Design

### Why an Audit Table Exists
- Track whether pipelines ran
- Track success vs failure
- Measure duration and volume
- Enable debugging and observability

### Column Breakdown

#### RUN_ID
Purpose:
- Unique identifier per pipeline execution
- Enables retries, replays, and debugging

#### PIPELINE_NAME
Purpose:
- Identify which pipeline executed
- Enables grouping and monitoring across pipelines

#### STATUS
Purpose:
- Final outcome of execution
- Constrained to SUCCESS / FAILED for clarity

#### STARTED_AT / FINISHED_AT
Purpose:
- Measure execution duration
- Detect performance regressions
- Support SLA monitoring

#### ROWS_PROCESSED
Purpose:
- Verify data movement actually occurred
- Detect silent failures (SUCCESS + 0 rows)

#### MESSAGE
Purpose:
- Store human-readable error or status message
- Used for debugging, not analytics

#### CONTEXT (VARIANT)
Purpose:
- Capture runtime configuration safely
- Enable reproducibility and root-cause analysis
- Secrets are redacted before storage

#### CREATED_AT / CREATED_BY
Purpose:
- Governance and traceability
- Identify execution role or automation source

---

## ETL Framework Design

### BaseETL Responsibilities
- Define execution lifecycle
- Enforce validation
- Standardize logging and outcomes

### Why `transform()` Is Optional
- Some pipelines are pure ingest
- Keeps framework flexible without overengineering

---

## Snowflake-Specific Decisions

### Why Procedures Live Under `src/snowflake`
- Environment-specific execution logic
- Clear separation from generic ETL framework
- Scales to future environments (AWS, Airflow, etc.)

---

## Open Questions / Future Improvements
- Retry logic strategy
- Alerting thresholds
- Config table schema

--------------------------------------------------------

## Snowflake Procedure Template: `proc_etl_runner.py`


### Purpose
This script is a reusable template for Snowflake Python stored procedures.
It standardizes:
- ETL execution entrypoint (`run(session, config)`)
- Success/failure handling
- Audit logging to `UTIL_ETL_AUDIT_LOG`
- Returning a structured status object (VARIANT-like dict)

### Key Design Choices

#### `run(session, config)`
- Snowflake calls `run()` as the procedure handler.
- `session` is a Snowpark Session provided by Snowflake runtime.
- `config` is passed from `CALL PROC(OBJECT_CONSTRUCT(...))`.

#### UTC timestamps (`_utcnow`)
- Use timezone-aware UTC timestamps for consistency across Snowflake/AWS/local.
- Prevents confusion with local time zones.

#### Redaction of secrets (`_safe_variant`)
- Stores execution context in `CONTEXT VARIANT` for reproducibility/debugging.
- Redacts sensitive keys like `token`, `api_key`, `password`, `secret`.
- Goal: avoid leaking secrets into logs/audit tables.

#### Audit insert helper (`_insert_audit_row`)
- Single place to handle writing run metadata to Snowflake.
- Uses parameter binding via `.bind(row)` for safer SQL.
- TODO: fully qualify audit table name via config (DB.SCHEMA.TABLE).

### Execution Flow
1. Start timer + read pipeline name from config.
2. Try:
   - (later) execute ETL logic (extract/transform/load)
   - write SUCCESS audit record
   - return success result
3. Except:
   - write FAILED audit record (best effort)
   - return failure result (do not mask original error)

### Why audit fields matter
- PIPELINE_NAME + RUN_ID: identify a specific run
- STATUS: quick monitoring of success/failure
- STARTED_AT/FINISHED_AT: duration and SLA tracking
- ROWS_PROCESSED: detect silent failures (SUCCESS but 0 rows)
- MESSAGE: human-readable failure reason
- CONTEXT: config snapshot for reproducibility (redacted)

### Next Improvements (planned)
- Make audit table fully qualified using config values.
- Replace placeholder rows_processed with real counts.
- Integrate BaseETL framework and Snowflake-specific ETL implementations.
- Add validation checks and optional retries.
