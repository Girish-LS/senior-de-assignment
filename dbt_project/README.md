# dbt project

**Status: authored, not executed in this environment.**

This project is complete — sources, staging model, incremental mart,
`schema.yml` tests, ownership metadata, and an exposure — but it has not been
run here. The machine used for this assessment blocks the public Python
package index, so `dbt-core` and `dbt-duckdb` cannot be installed.

Rather than claim an untested code path, the same transformation is
implemented in `sql/daily_account_summary.sql` and executed by
`ingestion/run_transform.py` using Python's bundled `sqlite3`. The eleven
assertions in `ingestion/transform.py` mirror the tests in
`models/marts/schema.yml` one for one, so the two definitions cannot drift
silently.

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
