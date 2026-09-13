# dbt project

**Status: executed. `dbt build` returns PASS=34 WARN=0 ERROR=0 SKIP=0.**

Two models (`stg_transactions`, `daily_account_summary`), 32 data tests, one
exposure, and an enforced model contract on the mart.

This project was authored before `dbt-core` could be installed — the machine
used blocks the public Python package index. The same transformation was
therefore implemented first in `sql/daily_account_summary.sql` and executed
through Python's bundled `sqlite3`. Once the internal artifact repository was
configured, dbt installed and both paths were run and compared: **257 rows
each, identical grain, zero value mismatches** across every measure column.

That comparison is the point. Two independently written implementations
agreeing is stronger evidence of correctness than either passing its own
tests, because they do not share a bug. The duplication was not planned; it
was a side effect of the constraint, and worth keeping.

## To run it where packages are available

```bash
pip install dbt-core dbt-duckdb
cp profiles.yml.example ~/.dbt/profiles.yml
cd dbt_project
dbt deps
dbt build                  # runs models and tests together
dbt build --full-refresh   # exercises the recovery path
dbt docs generate && dbt docs serve
```

## What differs between the two implementations

| Aspect | dbt project | Executed SQL |
|---|---|---|
| Engine | DuckDB or Databricks | SQLite |
| Materialisation | incremental, `delete+insert` | delete-and-insert, same shape |
| Tests | `schema.yml`, 20+ column tests | 11 assertions in `transform.py` |
| Lineage | `dbt docs` graph, exposures | documented in the contract |

The SQL logic is equivalent. The aggregation, the deterministic
`top_category` tie-break, the NULL handling for credit-only days, and the
currency sorting are identical in both.
