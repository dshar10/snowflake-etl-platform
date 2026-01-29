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
-----------------------------------------------------------------------------------------------------------------------
## Day 3 – Config Discipline & Fully Qualified Names

### Step 1: Config Validation (`_require_str`)

Purpose:
- Enforce required configuration keys
- Fail fast with clear, human-readable errors
- Prevent silent misconfiguration

Why:
- Missing or empty config values are a top cause of ETL failures
- Without validation, errors appear later and are harder to debug

Design:
- Centralized config access logic
- Ensures values are present, non-empty, and string-safe
- Allows defaults where appropriate

Example:
Instead of failing later with unclear SQL errors, the procedure raises:
`ValueError: Missing required config key: audit_db`

---

### Step 2: Fully Qualified Audit Table Name (`_audit_table_fqn`)

Purpose:
- Build a deterministic `DB.SCHEMA.TABLE` reference for audit logging
- Avoid reliance on session defaults (database/schema)

Why:
- Snowflake object resolution depends on execution context
- Stored procedures may run under different roles or schemas
- Fully qualified names remove ambiguity and prevent environment bugs

Design:
- `audit_db` and `audit_schema` are required (environment-specific)
- `audit_table` has a default (`UTIL_ETL_AUDIT_LOG`)
- Table name is constructed once and reused

Key Insight:
- SQL identifiers (table names) cannot be parameter-bound
- Values can be bound; identifiers must be constructed explicitly

Result:
- Same Python code works across dev/test/prod
- No hardcoding
- Clear, explicit execution behavior
------------------------------------------------------------------------------------------------------
## Snowflake Core Exam Mapping – Day 3 (Config & Fully Qualified Names)
------------------------------------------------------------------------------------------------------
This section connects the Day 3 procedure design directly to Snowflake Core certification concepts.

---

### 1. Stored Procedure Input Validation

**What was implemented**
- Explicit validation of required config keys using `_require_str()`
- Procedure fails early with a clear error if required config is missing

**Snowflake Core Concept**
- Snowflake does not automatically validate OBJECT / VARIANT procedure parameters
- Validation logic must be implemented inside the procedure
- Errors raised in Python propagate back to Snowflake

**Exam Insight**
- Missing or invalid parameters should cause the procedure to fail immediately
- Early, explicit errors are preferred over downstream SQL failures

**Key Takeaway**
> Stored procedures must explicitly validate inputs; Snowflake will not do this for you.

---

### 2. Fully Qualified Object Names (DB.SCHEMA.TABLE)

**What was implemented**
- Audit table name constructed as `DATABASE.SCHEMA.TABLE`
- Avoided reliance on session defaults (current database/schema)

**Snowflake Core Concept**
- Unqualified object names are resolved using the current database and schema
- Stored procedures may execute under different roles and schema contexts
- Fully qualified names remove ambiguity and prevent environment-specific bugs

**Exam Insight**
- If an object name is not fully qualified, Snowflake uses the session’s current database and schema
- Best practice inside procedures and tasks is to always use fully qualified names

**Key Takeaway**
> Fully qualify object names in procedures to ensure deterministic behavior.

---

### 3. Configuration-Driven Design (Environment Portability)

**What was implemented**
- `audit_db`, `audit_schema`, and `audit_table` passed via config
- No hardcoded database or schema names

**Snowflake Core Concept**
- Snowflake supports multi-environment architectures (dev/test/prod)
- Procedures should be environment-agnostic
- Configuration should drive behavior, not hardcoded values

**Exam Insight**
- Same procedure code can run across environments if object names are passed dynamically
- Hardcoding database names reduces portability and is discouraged

**Key Takeaway**
> Use configuration to support multiple environments with the same procedure code.

---

### 4. Identifiers vs Bind Variables

**What was implemented**
- Table name constructed via string formatting
- Column values bound using `.bind(row)`

**Snowflake Core Concept**
- SQL identifiers (table names, schema names) cannot be parameter-bound
- Bind variables are for runtime values only
- Identifiers are resolved at compile time

**Exam Insight**
- Attempting to bind a table name will fail
- Correct pattern: construct identifiers, bind values

**Key Takeaway**
> Identifiers must be constructed explicitly; only values can be bound.

---

### 5. Error Handling & Procedure Robustness

**What was implemented**
- Primary ETL error preserved and returned
- Audit logging failure does not mask the real error

**Snowflake Core Concept**
- Errors in stored procedures propagate to the caller
- Secondary failures (logging) should not hide the primary failure
- Procedures can return structured results in addition to raising errors

**Exam Insight**
- Logging should be best-effort
- Business logic errors must remain visible to the caller

**Key Takeaway**
> Never let logging failures mask real execution errors.

---

### Summary – Snowflake Core Mental Model

- Procedures do not validate inputs automatically
- Unqualified object names depend on session context
- Fully qualified names are safer and recommended
- Identifiers cannot be bind variables
- Config-driven design enables environment portability
- Fail fast, log safely, return meaningful results
