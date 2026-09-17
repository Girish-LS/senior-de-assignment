# Databricks path

Bronze, silver and gold all in Unity Catalog. Ingestion runs as a notebook on
serverless compute, so the compute sits next to the storage; dbt builds silver
and gold on a SQL warehouse.

```
REST API ──► 01_ingest_bronze  ──► raw.bronze_transactions      (Delta)
             (notebook,             raw.quarantine_transactions
              serverless)           raw.pipeline_watermark
                                    raw.ingestion_run_metrics
                     │
                     ▼
             dbt ──► transactions_staging.stg_transactions   (silver)
                 └─► transactions_marts.daily_account_summary (gold)
```

## Why the notebook imports the repository instead of standing alone

`01_ingest_bronze` imports `validate_record`, `natural_key_hash`,
`flag_duplicates`, `apply_lookback` and `max_valid_transaction_date` from
`ingestion/`. It does not reimplement them.

That is deliberate and it is the same argument as having a single silver layer:
a rule should be stated exactly once. Two copies of the validation logic would
drift, and the day they drift is the day the local path and the Databricks path
disagree about what a valid transaction is — with no test able to tell you
which is right.

The cost is that the notebook needs the repository on the filesystem, which
means a Git folder rather than a standalone upload. Worth it.

---

## Setup

### 1. Add the repository as a Git folder

In the workspace: **Workspace → Create → Git folder**, URL
`https://github.com/Girish-LS/senior-de-assignment`, branch `main`.

A private repository needs a GitHub token first: **Settings → Linked accounts
→ Git provider**, with a personal access token carrying `repo` scope.

The notebook walks up from its own path to find the `ingestion` package, so it
works whether it sits in `databricks/notebooks/` or at the root.

### 2. Create the secret scope

Credentials never live in the notebook. `dbutils.secrets.get` is the Databricks
equivalent of the environment variables the local path uses, and its value is
redacted from all notebook output.

Using the CLI:

```bash
databricks secrets create-scope transactions-api
databricks secrets put-secret transactions-api api-base-url
databricks secrets put-secret transactions-api api-key
databricks secrets put-secret transactions-api auth-token
```

`auth-token` is optional — the notebook falls back to `api-key`, since the
brief states the bearer token may be the same value.

### 3. Run it

Open `databricks/notebooks/01_ingest_bronze`, attach serverless compute, and
run all. Widgets at the top control mode, catalog, schema, secret scope and
lookback.

First run: set **mode** to `full`. Expect 352 fetched, 349 valid, 3
quarantined, 5 duplicates flagged, watermark `2024-03-30T21:01:36Z`.

Second run: leave **mode** as `incremental`. Expect 17 fetched inside the
72-hour lookback and **zero new bronze rows** — that is `MERGE INTO` proving
idempotency.

### 4. Build silver and gold

```bash
cd dbt_project
dbt build --target databricks
```

`PASS=34 WARN=0 ERROR=0`. Two models, 32 data tests, an enforced model
contract, one exposure.

dbt reads the bronze tables directly because the notebook writes into the
`raw` schema, which is the source name declared in
`models/staging/schema.yml`. No intermediate step, no export, no manual upload.

---

## Orchestration

`databricks/jobs/transactions_pipeline_job.json` defines a two-task Job:
notebook, then dbt, with the dependency declared.

**It is committed as documentation of the intended orchestration and was not
deployed.** The notebook and the dbt project were both executed against a live
workspace; the Job wrapper was not.

Why two tasks rather than one script: ingestion and transformation fail for
different reasons and are owned differently. A transient API failure should
retry the fetch without rebuilding the mart, and a failing data quality test
should not re-hit the API. The retry counts differ for the same reason —
ingestion retries three times because API failures are usually transient, dbt
retries once because a failing quality test is not transient, it means the data
is wrong and a human should look.

`max_concurrent_runs` is 1 because the watermark is shared mutable state: two
concurrent runs could both read the same value and one would overwrite the
other's advance.

---

## Design decisions specific to this path

**Bronze stores every field as STRING.** It is a faithful record of what the
API sent; interpreting types is silver's job, done once and explicitly.
Storing `amount` as normalised decimal text also keeps money out of floating
point before anyone has decided on a precision — binary floating point cannot
represent decimal currency exactly.

**`MERGE INTO`, not `INSERT`.** Spark SQL has no `INSERT ... ON CONFLICT`.
MERGE is the Delta equivalent and it is what makes a rerun safe: the watermark
filter uses `gte`, so the boundary record is deliberately re-read every
incremental run, and a blind insert would duplicate it each time.

**The watermark comes from validated records only.** This dataset contains two
invalid records dated April and November 2024, later than every valid record.
A watermark taken over raw records lands on the November date; every subsequent
run then asks the API for records newer than that, receives nothing, and exits
successfully. A green pipeline and no data, permanently.

**A zero-record incremental run raises.** The lookback window always contains
the record that set the watermark, so an empty result means the filter is
malformed rather than that there is no new data. This guard exists because the
bug happened: an earlier version double-applied the `gte.` prefix, producing
`gte.gte.<timestamp>`, which matched nothing and reported success.

**Quarantine is idempotent within a run, append-only across runs.** A run's own
rows are cleared before it writes, so re-running a failed run does not
double-count — but "was bad yesterday, still bad today" is an audit fact worth
keeping.

---

## The local path still exists, and is still the default

The same pipeline runs against SQLite and DuckDB with **no Databricks account,
no credentials and no install step** — `sqlite3` ships with Python.

```bash
python -m unittest discover -s tests
python -m ingestion.ingest_transactions --source csv --csv-path data/transactions.csv
python -m ingestion.run_transform
```

This is not legacy. Reproducibility is a graded criterion, and a reviewer being
able to clone the repository and see results without provisioning anything is
worth more than a single canonical path. It also produced the strongest
correctness evidence in the submission: the mart is implemented twice, in
hand-written SQL and in dbt, and the outputs were compared row by row —
identical grain, zero value mismatches. Two independent implementations
agreeing do not share a bug.
