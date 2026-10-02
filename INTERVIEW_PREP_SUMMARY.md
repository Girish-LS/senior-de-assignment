# `senior-de-assignment` — Interview Prep Summary

## 1. One-line pitch
A production-minded ingestion + transformation pipeline for payment transaction data: fetches from a REST API → validates/quarantines → bronze (raw) → silver (deduped/typed) → gold (`daily_account_summary` mart), built to run **either** fully offline (stdlib + SQLite/DuckDB, zero installs) **or** on Databricks (Delta/Unity Catalog), with the same logic implemented and cross-verified twice.

## 2. Architecture (say this out loud)
```
REST API ──► ingestion (Python, stdlib only) ──► bronze.bronze_transactions
             validate · quarantine · dedupe         bronze.quarantine_transactions
             · watermark                             bronze.pipeline_watermark / ingestion_run_metrics
                        │
                        ▼
             dbt ──► silver.stg_transactions   (dedupe, cast)
                 └──► gold.daily_account_summary (contract + 32 tests)
```
Two runtimes, same shape:
- **Local/offline:** Python stdlib → SQLite, then dbt on DuckDB. No pip install, no credentials, no network — `make all` runs everything.
- **Databricks:** ingestion notebook on serverless compute writes Delta tables to Unity Catalog; dbt builds silver/gold on a SQL warehouse. Verified end-to-end (352 fetched, 349 valid, 3 quarantined, watermark set, rerun inserts 0 new rows, `dbt build` PASS=34).

## 3. The file you opened: `daily_account_summary.sql` (the gold mart)
This is the centerpiece model — know it cold.

- **Grain:** one row per `account_id` per UTC calendar day, completed transactions only.
- **Materialization: `incremental` with `incremental_strategy='delete+insert'`** (not `merge`, not `append`). Be ready to explain why:
  - It's an **aggregate**, not a row-level fact. `merge` tempts you to patch an aggregate incrementally, which is wrong — if a late transaction lands for a past day, you must recompute the whole day's aggregate, not adjust it.
  - `append` is simply wrong — it would produce multiple rows per grouping key, violating the stated grain.
  - Delete+insert = "delete the whole day, recompute it" — correct semantics for aggregates.
- **Incremental predicate** filters `transaction_day >= max(transaction_date) - lookback_days`, matching the ingestion lookback — so a late-arriving row that lands in bronze also forces recomputation of its business day.
- **Deterministic `top_category`:** ranks merchant categories by debit spend with an explicit tie-break on category name (`order by debit desc, category asc`) — without the tie-break, two runs on identical input could return different results, which would falsify the idempotency claim. Credit-only days get `NULL` top_category rather than an arbitrary pick (`category_debit > 0` filter).
- **Known limitation, stated deliberately:** `total_debit_amount`/`total_credit_amount` sum across up to 7 currencies — arithmetically valid, financially meaningless (adds USD + JPY). This was implemented as specified rather than silently "fixed," because the fix (FX rates, as-of convention, native vs. reporting currency) is a **product decision**, not an engineering one. The `currencies` column is included specifically so no consumer mistakes the total for real money.
- **`updated_at`** cast to `timestamp` on every row — freshness is visible in the data itself, not just in logs.
- **Enforced model contract** (`contract: enforced: true`) — dbt refuses to build if output types drift. This caught a real bug: `SUM` widened money columns to `DECIMAL(38,2)` vs. a contract of `DECIMAL(18,2)`; fixed by casting in the model rather than loosening the contract (contract = the commitment to consumers).

## 4. Incremental / watermark design (likely to get grilled on this)
- **Watermark = max `transaction_date` over validated records only, from runs that succeeded.** This is the standout detail: two invalid records in the dataset have dates in April/November 2024, later than any valid record. A watermark computed over *raw* data would jump to November, every subsequent run would then filter past that date, return nothing, and report success forever — a silently broken pipeline behind a green dashboard. Computing it from validated data only avoids this trap, and it's asserted by a named test.
- **`gte`, not `gt`:** `gt` would permanently skip any record sharing the exact watermark timestamp. `gte` re-reads the boundary, which means loads **must be upserts** (idempotent on `transaction_id` / `MERGE INTO` on Databricks since Spark has no `INSERT...ON CONFLICT`).
- **72-hour lookback, mandatory:** `transaction_date` is business time, not ingestion time (API exposes no `created_at`); a transaction dated Tuesday but written Thursday becomes invisible once the watermark passes Wednesday. The lookback bounds (not eliminates) that exposure — anything later than 72h is still missed, a deliberate, documented risk.
- **Watermark only advances on success** — never mid-run — so a failure never skips unprocessed records.
- **A zero-record incremental run inside the lookback window raises an error** rather than reporting success — because the lookback window should always contain at least the record that set the watermark; zero results means the filter itself is broken. This guard was added after a real bug (`gte.` prefix applied twice → `gte.gte.<timestamp>` → matched nothing, reported success).
- **Better design not used:** API has an undocumented `id` sequence; a cursor watermark on `id` would be immune to both the late-arrival problem and the invalid-date trap. Not used because the assessment specifies the native date filter — named as the first thing to change.

## 5. Validation & deduplication
- Validates every schema rule; two rules need real logic beyond regex: `country_code` regex matches but isn't in the **assigned ISO 3166-1 set** (e.g., `UK`, `EN` aren't real codes), and `transaction_date` regex matches but isn't a real calendar date (e.g., `2024-11-31`).
- Collects **all** violations per record (not fail-fast) so quarantine reasons are actionable.
- Amounts stored as `Decimal`, never `float` — avoids binary floating point drift in money math.
- **Duplicates flagged, not dropped**, at ingestion — survivorship = lowest `transaction_id` (deterministic/reproducible). Rationale: dropping is irreversible and destroys the ability to monitor duplicate rate (a leading indicator of upstream retry/replay bugs); a duplicate is a different failure mode from an invalid record (conforms to the contract, just redundant) so it shouldn't share quarantine's remediation path.

## 6. Testing / correctness strategy
- 60–65 unit tests (stdlib `unittest`), targeting rules and design decisions, not just coverage — each planted defect maps to a named test.
- 11 data-quality assertions on the mart: grain, `net_amount` reconciles to inputs, `distinct_merchants <= transaction_count`, reconciliation back to bronze.
- **Idempotency is asserted, not claimed** — mart rebuilt twice, rows compared (excluding `updated_at`, which is expected to change — a frozen `updated_at` would itself be a bug).
- **The mart is implemented twice** — hand-written SQL (`sql/daily_account_summary.sql`, via sqlite3) and dbt (DuckDB/Databricks) — and both were run and diffed row-by-row: 257 rows each, identical grain, zero mismatches. This is the strongest correctness evidence in the project: two independent implementations agreeing don't share a bug.
- Caught two real bugs this way: strict-enum validation quarantining all 352 records, and a semicolon inside a SQL comment breaking statement splitting.

## 7. Portability lessons (Databricks vs DuckDB) — good "war story" material
Running on a second engine surfaced issues invisible on one engine alone:
1. `timestamp with time zone` doesn't exist in Spark SQL → contract fixed to plain `timestamp`, cast explicitly in-model. Lesson: a dbt contract is declared in the warehouse's own type system — "portable" SQL isn't proven portable until it's run on a second warehouse.
2. `cast(null as varchar)` needs an explicit length on Spark (`DATATYPE_MISSING_SIZE`) → used `string` instead (works on both).
3. `listagg(... order_by ...)` is silently ignored by Spark — `currencies` column is sorted on DuckDB, unsorted on Databricks. **Left as a documented limitation** rather than papered over, since tests only assert accepted values, not ordering.
4. A UTF-8 BOM from PowerShell's `Set-Content` broke Spark's SQL parser (DuckDB tolerated it).

## 8. Environment-constraint story (explains a lot of the design)
- Dev machine blocked the public package index (no `pip install`, no `dbt deps`, no DuckDB `sqlite_scanner` extension) — forced stdlib-only ingestion, hand-written validation instead of Pydantic, and reimplemented two `dbt_utils` generic tests locally instead of depending on the package. This is *why* the SQL mart was built before the dbt mart existed, and *why* there are two implementations to compare.
- CSV exports bridge SQLite → DuckDB (no sqlite attach extension available); uses `all_varchar=true` on read to avoid DuckDB's type inference corrupting bronze fidelity (it would otherwise turn `amount` into `DOUBLE`, reintroducing the float-money problem).

## 9. Design/product thinking (good for "tell me about trade-offs" questions)
- Explicitly did **not** build a generic ingestion framework for "the next 10 APIs" — deliberate anti-abstraction stance: a framework derived from one example encodes that example's accidents as requirements. Instead built a clean seam (generic HTTP client: pagination/retry/auth vs. source-specific validation/modeling). States the honest endpoint: at ~10 sources, switch to ADF/`dlt` rather than keep building bespoke tooling.
- Data contract (`contracts/daily_account_summary.yml`) declares owner, SLA, column semantics, change policy — "trust" framed as: can a consumer answer who owns it, when it updates, what columns mean, what feeds/depends on it, and whether tests currently pass — without asking a person.
- Monitoring/alerting table in the design note: run failures, freshness vs SLA, quarantine rate trend, consecutive zero-record runs, volume anomalies, HTTP retry rate, DQ assertion failures, late-arrival rate vs. lookback window, cost per run — each with a named action and the principle that an unowned alert becomes noise.
- Airflow DAG and Databricks Job JSON are committed as **illustrative/not deployed** — honest about what wasn't executed vs. what was.

## 10. AI usage disclosure (be ready to discuss transparently)
Used an LLM as a pair programmer for drafting/profiling/review, but verification was independent of generation: dataset profiled twice by different methods, API probed before writing the client (caught 3 real quirks: amount as JSON string, HTTP 206 on filtered responses, invalid `limit` silently ignored), mart built twice and diffed, two bugs caught by tests, and a subtle fixture-vs-live-API discrepancy found by diffing rather than assuming equivalence.

## 11. Likely interview questions to rehearse answers for
- Why `delete+insert` instead of `merge` for an aggregate table?
- Walk me through the watermark trap and why it's computed from validated data only.
- Why `gte` and not `gt`, and what does that force elsewhere in the design?
- What's wrong with the mixed-currency total, and why didn't you just fix it?
- How did you prove idempotency rather than just claim it?
- What broke when you moved from DuckDB to Databricks, and what's the general lesson?
- Why two implementations of the same mart — wasn't that wasted effort?
- What would you change first with more time? (FX handling, `id`-based cursor watermark, continuous DQ monitoring)

---

## Quick Reference: Key Numbers
- **352** records fetched
- **349** valid → bronze
- **3** quarantined (schema violations: dates, categories, countries)
- **5** duplicate pairs detected (deterministic survivorship on lowest transaction_id)
- **257** rows in `daily_account_summary` mart
- **60–65** unit tests (stdlib unittest)
- **11** data quality assertions
- **34** dbt tests PASS (20+ column tests + schema tests)
- **72** hours lookback window
- **7** currencies max in one row

## Key Files to Reference During Interview
- `dbt_project/models/marts/daily_account_summary.sql` — the centerpiece incremental mart model
- `dbt_project/models/marts/schema.yml` — enforced data contract
- `ingestion/models.py` — validation rules, all violations per record
- `ingestion/dedupe.py` — deterministic duplicate survivorship
- `sql/daily_account_summary.sql` — the independent SQL implementation (for comparison proof)
- `tests/test_validation.py` — every rule, each planted defect by name
- `docs/product_platform_note.md` — design trade-offs, monitoring/alerting strategy
- `outputs/` — committed evidence (run transcripts, sample outputs, idempotency proof)
- `outputs/databricks/idempotency_check.txt` — explicit Task 3 proof (row counts before/after rerun)
