# Snowflake ETL Platform

A Snowflake-centric ETL platform built with Python and AWS.

## Goals
- Build reusable, production-grade ETL pipelines
- Use Python stored procedures inside Snowflake (Snowpark)
- Support incremental loads, backfills, and auditing
- Integrate with AWS (S3, Lambda) where appropriate
- Enable future internal tooling and dashboards

## Structure
- `src/etl/` → reusable ETL framework code
- `sql/` → SQL assets (DDL, MERGE templates)
- `docs/` → procedure docs, architecture notes
- `tests/` → unit/integration tests
