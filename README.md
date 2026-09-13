# Senior Data Engineer Take-Home: Transaction Ingestion and Daily Summary

A production-minded ingestion and transformation pipeline for payment
transaction data. Fetches from a REST API, validates and quarantines defective
records, persists a raw layer, produces an idempotent daily account summary,
and supports incremental ingestion with watermark handling.

**Runs on a bare Python 3.11+ interpreter. No `pip install` required.**

---

## Quick start

```bash
git clone <repo-url>
cd senior-de-assignment

cp assignment.env.example assignment.env   # then fill in the API values
# Windows PowerShell: Copy-Item assignment.env.example assignment.env

python -m unittest discover -s tests       # 65 tests
python -m ingestion.ingest_transactions    # Task 1: full ingestion
python -m ingestion.incremental_ingest     # Task 3: incremental run
python -m ingestion.run_transform          # Task 2: build and assert
python -m ingestion.export_outputs         # write sample outputs
```

No network? Every command accepts the offline fixture. Note that the fixture
is a *regenerated* dataset, not a snapshot: it has the same shape as the live
API (352 records, 3 invalid, 5 duplicate pairs) but different record content,
so figures produced from it will not match figures produced from the API.
Committed sample outputs are generated from the live API. See "Fixture versus
live API" below.

```bash
python -m ingestion.ingest_transactions --source csv --csv-path data/transactions.csv
```

`make all` runs the whole sequence.

---

## Results

| Metric | Value |
|---|---|
| Records at source | 352 |
| Valid, persisted to bronze | 349 |
| Quarantined | 3 |
| Duplicate pairs detected | 5 |
| Rows in `daily_account_summary` | 257 |
| Tests | 60 passing |
| Data quality assertions | 11 passing |

Sample outputs are committed in `outputs/` so results can be reviewed without
running anything.

### The three quarantined records

Each carries multiple violations, which is why the validator collects all
errors per record rather than stopping at the first.

| Record | Violations |
|---|---|
| TXN-0345 | `2024-11-31` passes the date regex but November has 30 days; `fast_food` not an accepted category; `UK` not an assigned ISO code |
| TXN-0346 | `2024-04-15 09:30:00` missing `T` and `Z`; amount `-127.5`; merchant name is three spaces; status `Completed` |
| TXN-0347 | amount `0.0` fails strictly-greater-than-zero; type `Credit`; category `Finance`; country `EN` not assigned |

### The watermark trap

Every valid record falls between 2024-01-01 and 2024-03-30. Two quarantined
records carry dates of 2024-04-15 and 2024-11-31.

A watermark computed over raw records would jump to November. Every later run
would filter `gte 2024-11-31`, return nothing, report success, and the data
would silently stop arriving behind a green dashboard.

The watermark is therefore computed from **validated records only**, landing
correctly on `2024-03-30T22:35:29Z`. This is asserted by a test
(`test_watermark_avoids_the_invalid_date_trap`) rather than left to inspection.

---

## Technology choices and rationale

| Choice | Why | What was given up |
|---|---|---|
| Standard library only | The target machine blocks the public package index. The resulting property — clone and run, no install — is also the strongest possible answer to reproducibility | pydantic's error aggregation, tenacity's retry primitives |
| `sqlite3` | Ships with Python. At 352 rows the analytical difference is immaterial, and the SQL is portable | DuckDB's columnar performance and richer SQL |
| Mart implemented twice, in SQL and in dbt | The SQL path came first, because `dbt-core` could not initially be installed. Once it could, both were run and compared: 257 rows each, zero value mismatches | Nothing; the duplication became the strongest correctness evidence in the submission |
| `urllib` over `requests` | Zero dependency, and the probe proved it works against this API | Convenience; more verbose code |
| Hand-written validation | Every rule legible without knowing a library's coercion semantics | More code; risk of inconsistency, mitigated by per-rule tests |
| Airflow DAG illustrative | No target environment | A deployed orchestrator |

**On the environment constraint:** this is a real gap, not a virtue. Given a
machine with package access, this would be dbt Core on DuckDB with a Databricks
target configured, exactly as `dbt_project/` is written. The constraint is
documented rather than hidden, and the dbt project is committed so the
intended design is reviewable.

**Note on Databricks:** Community Edition was retired on 1 January 2026 and
replaced by Databricks Free Edition, which is serverless-only and quota-limited.
`dbt_project/profiles.yml.example` targets Free Edition.

---

## Validation and duplicate handling

**Validation** enforces every rule in the published schema. Two need checks a
pattern cannot express, exactly as the schema warns:

- `country_code` — `UK` and `EN` match `^[A-Z]{2}$` but are not officially
  assigned ISO 3166-1 codes. Requires membership of the assigned set.
- `transaction_date` — `2024-11-31` satisfies the date pattern. Requires a real
  calendar parse.

All violations for a record are collected and reported. A quarantine reason of
"invalid" is useless to whoever has to act on it.

Amounts are `Decimal`, never `float`. Binary floating point cannot represent
decimal currency exactly, and accumulated error across summed amounts appears
in production as pennies of drift that break reconciliation.

**Duplicates are flagged in bronze and resolved downstream**, not dropped at
ingestion. The assessment permits either; the reasoning:

Dropping is irreversible and makes two questions permanently unanswerable — how
often does the source emit duplicates, and is that rate changing? The first is
what you need to raise the issue upstream; the second is an early indicator of
a replay or retry defect.

A duplicate is also a different kind of thing from an invalid record. An
invalid record violates the contract; a duplicate conforms to it and is merely
redundant. Routing duplicates to quarantine would conflate two conditions with
different owners and different remediations.

Survivorship is the lowest `transaction_id` — chosen because it is stable
across runs. An arbitrary survivor would make downstream aggregates
non-reproducible and quietly falsify the idempotency claim.

---

## Incremental and late-arriving data strategy

**Watermark:** highest `transaction_date` across validated records from runs
that actually succeeded. Advancing on a failed run would move the filter past
records that were never persisted.

**`gte`, not `gt`.** With `gt`, any record sharing the exact watermark
timestamp is skipped permanently. The resulting re-read overlap is harmless
because bronze loads are upserts on `transaction_id`.

**Lookback window: 72 hours, and it is mandatory here.** `transaction_date` is
a *business* timestamp. A transaction dated Tuesday but written to the source
on Thursday is invisible to a business-time filter once the watermark passes
Wednesday. Probing confirmed the API exposes no `created_at` or `inserted_at`,
so there is no ingestion-time column to use instead.

The window bounds the exposure; anything later than 72 hours is still missed,
which makes the length a deliberate risk decision rather than a default. It
should be tuned against measured lateness once there is data on it.

**Behaviours:**

- *First run* — no watermark exists, so everything is read and the watermark is
  set from the highest valid date seen.
- *No new data* — the filtered request returns nothing, the watermark is
  unchanged, the run exits zero. Normal, not an error — but counted and
  logged, because a silently empty pipeline looks identical to a healthy one.
- *Late-arriving data* — covered by the lookback, bounded by its length.
- *Failure* — the watermark is not advanced. Re-running is always safe.

**The better design, not used here:** the API exposes an undocumented `id`
sequence. A cursor watermark on `id` would be immune to both the late-arrival
problem and the invalid-date trap. The assessment specifies the native date
filter, so that is what is implemented, but `id` is the first change I would
make.

---

## Testing approach

```bash
python -m unittest discover -s tests -v     # 65 tests
python -m ingestion.run_transform           # 11 data quality assertions
python -m ingestion.run_transform --check-idempotency
```

Tests target rules and design decisions rather than implementation. Each
planted defect has a named test, so a regression points at a specific rule.

The assertions check invariants that a plausible bug would actually violate,
not a quota of not-null checks — the grain, that `net_amount` agrees with its
inputs, that `distinct_merchants` cannot exceed `transaction_count`, and a
reconciliation of the mart's counts back to bronze.

Idempotency is *asserted*, not claimed: the mart is rebuilt twice and compared
row by row.

Two bugs were caught this way during development, both invisible from
plausible-looking output: strict-mode enum validation quarantined all 352
records, and a semicolon inside a SQL comment broke statement splitting.

---

## Repository layout

```
senior-de-assignment/
├── README.md
├── Makefile
├── assignment.env.example        # template; real file is gitignored
├── contracts/
│   └── daily_account_summary.yml # owner, SLA, semantics, change policy
├── dags/
│   └── transactions_pipeline.py  # illustrative Airflow DAG
├── data/
│   └── transactions.csv          # offline fixture (same shape, different rows)
├── dbt_project/                  # dbt build: PASS=34, 0 errors
│   ├── models/staging/           # stg_transactions + schema.yml
│   └── models/marts/             # daily_account_summary + schema.yml
├── docs/
│   ├── product_platform_note.md  # Task 4
│   ├── dbt/index.html            # generated lineage browser
│   └── transactions_schema.json
├── ingestion/
│   ├── api_client.py             # pagination, retry, 206 handling
│   ├── config.py                 # env-sourced settings, fail fast
│   ├── dedupe.py                 # natural key, deterministic survivorship
│   ├── iso_country_codes.py      # the 249 assigned codes
│   ├── models.py                 # validation rules
│   ├── pipeline.py               # orchestration and watermark logic
│   ├── storage.py                # bronze, quarantine, watermark, metrics
│   └── transform.py              # summary build and assertions
├── outputs/                      # committed samples + run transcript
├── scripts/
│   ├── probe_api.py              # endpoint characterisation, run first
│   ├── export_for_dbt.py         # SQLite tables -> CSV for dbt
│   ├── capture_run.ps1 / .sh     # reproducible run transcript
│   └── verify_submission.py      # audits this repo against the brief
├── sql/
│   └── daily_account_summary.sql
└── tests/                        # 65 tests
```

---

## Security

- No credential is committed. `.gitignore` was written before `git init`, and
  CI fails the build if an environment file is ever tracked.
- Credentials are read from the environment and never logged. Request URLs are
  logged; auth headers are not.
- Record payloads are not logged. In a payments context a full dump writes
  account identifiers into log storage, which routinely has weaker access
  controls than the warehouse.
- The API key distributed with the assessment is treated as already exposed. A
  key committed to a repository must be rotated, not deleted — git history
  preserves it.

---

## Known limitations

**Mixed-currency totals.** `total_debit_amount` sums across up to seven
currencies, so the figure adds USD to JPY and is not meaningful as money.
Implemented as specified; the `currencies` column exposes it. The fix needs an
FX source and an as-of convention, which is a product decision. This is the
first thing I would raise before productionising.

**Late arrivals beyond 72 hours are missed.** Inherent to watermarking on
business time. Resolved by an insertion timestamp at source.

**Two stores and an export step between them.** The pipeline writes SQLite;
dbt reads CSV exports of those tables, because DuckDB can only attach SQLite
through an extension whose download is blocked behind a corporate proxy. Less
elegant than a single engine. Production would export Parquet, which preserves
types.

**Airflow DAG not deployed.** No target environment.

**Single mart, no dimensional model.** Scope.

**ISO country list is a literal**, not a maintained dependency. Auditable in
review; needs refreshing when the standard changes.

---

## With more time

1. FX handling, so the monetary totals mean something.
2. Cursor watermark on `id`, removing the late-arrival exposure.
3. Execute the dbt project and commit `dbt docs` lineage output.
4. Continuous data quality monitoring on the published table, distinct from CI
   which validates code against fixtures.
5. A second source, to reveal which parts of the ingestion code are genuinely
   reusable rather than guessing.

---

## AI tool usage

Disclosed in full in `docs/product_platform_note.md`. Summary: used as a pair
programmer for profiling, drafting and review; verification was independent of
generation. The dataset was profiled twice by different methods and the results
reconciled; the API was probed before the client was written, and three
findings changed the design; two bugs were caught by tests that existed because
the expected answer was established first.

---

## Fixture versus live API

The assignment supplied a CSV backup alongside the API. Running the pipeline
against both revealed that they are **different datasets with the same
structure**, not two copies of one dataset:

| | Live API | `data/transactions.csv` |
|---|---|---|
| Record count | 352 | 352 |
| Invalid records | 3 | 3 |
| Duplicate pairs | 5 | 5 |
| Duplicate partner ids | TXN-0319, 0178, 0079, 0194, 0180 | TXN-0217, 0245, 0062, 0068, 0037 |
| Max valid `transaction_date` | 2024-03-30T21:01:36Z | 2024-03-30T22:35:29Z |

The duplicate pairs are entirely disjoint, which is conclusive: the fixture
was generated from the same rules as the API data, not exported from it.

Two consequences, both handled:

1.  **Committed outputs are generated from the live API**, so a reviewer
    re-running against the API reproduces them. Regenerating from the fixture
    produces valid but different numbers.

2.  **The detection logic was validated twice, independently.** Finding
    exactly 3 invalid records and 5 duplicate pairs in both datasets is
    evidence that the rules generalise rather than being fitted to one
    sample. The two datasets amounted to an unplanned second test case.

This is also a small illustration of a habit worth keeping: a backup supplied
"in case the API doesn't work" is worth diffing against the API rather than
assuming equivalence. The assumption would have been invisible until a
reviewer got different numbers from the committed ones.

---

## Captured run transcript

`outputs/run_transcript.txt` holds the console output of a complete run: test
suite, full ingestion, incremental ingestion, transformation with assertions,
and sample export. It is committed so the results can be read without
executing anything, and regenerated with one command rather than transcribed
by hand.

```powershell
.\scripts\capture_run.ps1              # live API
.\scripts\capture_run.ps1 -Source csv  # bundled fixture, no credentials
.\scripts\capture_run.ps1 -Redact      # mask the API host for a public repo
```

```bash
./scripts/capture_run.sh                 # live API
./scripts/capture_run.sh csv             # bundled fixture
REDACT=1 ./scripts/capture_run.sh        # mask the API host
```

The script clears watermark state and the warehouse before running, so stage 1
is a genuine first run and stage 2 a genuine incremental run. Capturing a
transcript without that reset would show an incremental run against a
warehouse that was already populated, which proves nothing.

No credentials appear in the transcript. The pipeline logs the API key's
length, never its value.

---

## The dbt path

The mart is implemented twice, on purpose: as hand-written SQL in
`sql/daily_account_summary.sql`, and as a dbt model in
`dbt_project/models/marts/`. Both were run and their outputs compared row by
row.

**257 rows each, identical grain, zero value mismatches** across
`total_debit_amount`, `total_credit_amount`, `net_amount`,
`transaction_count`, `distinct_merchants`, `top_category` and `currencies`.
Two independently written implementations agreeing is stronger evidence of
correctness than either passing its own tests.

### Running it

```bash
pip install -r requirements.txt          # dbt-core and dbt-duckdb
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml

python scripts/export_for_dbt.py         # SQLite tables -> CSV
cd dbt_project && dbt build --target duckdb
```

Result: `PASS=34 WARN=0 ERROR=0 SKIP=0`. Two models (`stg_transactions`,
`daily_account_summary`), 32 data tests, one exposure.

### Three environment problems, and how they were solved

These are worth stating because each would make the dbt path fail on a
locked-down network while appearing to work on an open one. That asymmetry is
the dangerous kind of bug: it passes on the developer's laptop.

**1. The warehouse is SQLite, not DuckDB.** The pipeline uses the standard
library only, and `sqlite3` ships with Python while `duckdb` does not. DuckDB
can attach a SQLite file, but only through the `sqlite_scanner` extension, and
`extensions.duckdb.org` returns 403 behind a corporate proxy. `dbt` therefore
reads CSV exports produced by `scripts/export_for_dbt.py`. CSV needs no
extension, so the project runs anywhere. The file is named
`transactions.sqlite`, not `.duckdb`, so nobody opens it with the wrong client.

**2. `dbt deps` is blocked.** `hub.getdbt.com` also returns 403. The project
used two `dbt_utils` generic tests; both are reimplemented in
`dbt_project/macros/generic_tests.sql` and `packages.yml` is now empty. A
project that cannot run `dbt deps` cannot run at all, so the dependency made
the dbt path unrunnable in exactly the environment meant to demonstrate it.

**3. CSV type inference corrupts bronze fidelity.** Bronze stores every field
as the raw text the API sent, deliberately. Letting DuckDB infer types on read
turned `transaction_date` into a TIMESTAMP and `amount` into a DOUBLE —
reintroducing the float-for-money problem the pipeline avoids. Sources are
read with `all_varchar=true`; casting is staging's job, done explicitly, once.

### Model contracts

`daily_account_summary` has `contract: enforced: true`, so dbt refuses to build
if the model's output types drift from the declared schema. It caught a real
mismatch on first run: `SUM` widened the money columns to `DECIMAL(38,2)`
against a contract of `DECIMAL(18,2)`. Fixed by casting in the model rather
than by loosening the contract — the contract is the commitment to consumers,
so the model should satisfy it, not the reverse.

Money is `DECIMAL(18,2)` and never `DOUBLE`, which the contract now guarantees
to anyone reading the schema.

### Idempotency

Running `dbt build` twice leaves every business value unchanged:

```
rows                       : 257 -> 257
digest EXCLUDING updated_at: eb20ea256aff319e -> eb20ea256aff319e   same
digest INCLUDING updated_at: a5fa8cf2f2866d12 -> 7d09714136054fb4   changed
```

`updated_at` is *expected* to move — it records when the row was last computed
and is what a consumer uses to reason about freshness. A frozen `updated_at`
would be the actual bug.

### Lineage documentation

`docs/dbt/index.html` is committed: a self-contained, offline lineage browser
covering source-to-mart lineage, column descriptions, test coverage, and the
Power BI exposure. Open it in a browser; nothing needs to be running.

This is the concrete answer to the design note's question about exposing
lineage, ownership and quality status to consumers — an artefact rather than a
paragraph.
