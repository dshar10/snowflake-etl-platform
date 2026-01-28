-- ============================================================
-- ETL Audit Logger
-- Purpose:
--   Track every ETL run (SUCCESS/FAILED), duration, row counts,
--   and safe run context for debugging and monitoring.
-- ============================================================

CREATE TABLE IF NOT EXISTS UTIL_ETL_AUDIT_LOG (
  RUN_ID         STRING       DEFAULT UUID_STRING(),
  PIPELINE_NAME  STRING       NOT NULL,
  STATUS         STRING       NOT NULL,  -- SUCCESS | FAILED
  STARTED_AT     TIMESTAMP_TZ NOT NULL,
  FINISHED_AT    TIMESTAMP_TZ NOT NULL,
  ROWS_PROCESSED NUMBER,
  MESSAGE        STRING,
  CONTEXT        VARIANT,
  CREATED_AT     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  CREATED_BY     STRING       DEFAULT CURRENT_USER(),

  CONSTRAINT CK_STATUS CHECK (STATUS IN ('SUCCESS', 'FAILED'))
);

-- Helpful view for monitoring (latest runs first)
CREATE OR REPLACE VIEW UTIL_ETL_AUDIT_LOG_VW AS
SELECT
  RUN_ID,
  PIPELINE_NAME,
  STATUS,
  STARTED_AT,
  FINISHED_AT,
  DATEDIFF('second', STARTED_AT, FINISHED_AT) AS DURATION_SECONDS,
  ROWS_PROCESSED,
  MESSAGE,
  CREATED_AT,
  CREATED_BY
FROM UTIL_ETL_AUDIT_LOG
ORDER BY CREATED_AT DESC;
