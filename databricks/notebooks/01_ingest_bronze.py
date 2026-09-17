# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — REST API to Delta
# MAGIC
# MAGIC Fetches payment transactions from the source API, validates every
# MAGIC record, routes failures to quarantine, flags duplicates, and writes
# MAGIC Delta tables in Unity Catalog. Runs on serverless compute, so the
# MAGIC compute sits next to the storage.
# MAGIC
# MAGIC **This notebook imports the repository's own validation modules rather
# MAGIC than reimplementing them.** `validate_record`, `natural_key_hash`,
# MAGIC `flag_duplicates`, `apply_lookback` and `max_valid_transaction_date`
# MAGIC come from `ingestion/`. A second copy of the rules would drift from the
# MAGIC first, and the whole argument for a single silver layer is that a rule
# MAGIC should be stated exactly once.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC 1. This repository added as a Databricks Git folder, so `ingestion/` is
# MAGIC    on the filesystem beside this notebook.
# MAGIC 2. A secret scope holding the API credentials:
# MAGIC    `databricks secrets create-scope transactions-api`
# MAGIC    then put `api-base-url`, `api-key` and `auth-token` in it.
# MAGIC
# MAGIC **Downstream:** dbt builds silver (`stg_transactions`) and gold
# MAGIC (`daily_account_summary`) from what this writes.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"],
                         "Ingestion mode")
dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("raw_schema", "raw", "Raw schema (must match the dbt source name)")
dbutils.widgets.text("secret_scope", "transactions-api", "Secret scope")
dbutils.widgets.text("lookback_hours", "72", "Lookback window (hours)")

MODE = dbutils.widgets.get("mode")
CATALOG = dbutils.widgets.get("catalog")
RAW_SCHEMA = dbutils.widgets.get("raw_schema")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
LOOKBACK_HOURS = int(dbutils.widgets.get("lookback_hours"))

print(f"mode={MODE} target={CATALOG}.{RAW_SCHEMA} lookback={LOOKBACK_HOURS}h")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import the repository's validation modules
# MAGIC
# MAGIC The Git folder puts `ingestion/` one level above this notebook.

# COMMAND ----------

import os
import sys

# Walk up from the notebook until the `ingestion` package is found, so this
# works whether the notebook sits in databricks/notebooks/ or at the root.
_here = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook()
    .getContext().notebookPath().get()
)
_candidates = [
    "/Workspace" + _here,
    "/Workspace" + os.path.dirname(_here),
    "/Workspace" + os.path.dirname(os.path.dirname(_here)),
]
for _root in _candidates:
    if os.path.isdir(os.path.join(_root, "ingestion")):
        if _root not in sys.path:
            sys.path.insert(0, _root)
        print(f"repository root: {_root}")
        break
else:
    raise RuntimeError(
        "Could not locate the `ingestion` package.\n"
        "Add this repository as a Databricks Git folder so the notebook sits "
        "inside it, or upload `ingestion/` as workspace files and add its "
        "parent to sys.path.\n"
        f"Searched: {_candidates}"
    )

from ingestion.dedupe import flag_duplicates            # noqa: E402
from ingestion.models import validate_record            # noqa: E402
from ingestion.pipeline import (                        # noqa: E402
    SOURCE_SYSTEM,
    apply_lookback,
    max_valid_transaction_date,
)

print("validation modules imported from the repository — rules stated once")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credentials
# MAGIC
# MAGIC From a secret scope, never from the notebook. `dbutils.secrets.get` is
# MAGIC the Databricks equivalent of the environment variables the local path
# MAGIC uses, and its value is redacted from all notebook output.

# COMMAND ----------

API_BASE_URL = dbutils.secrets.get(SECRET_SCOPE, "api-base-url").rstrip("/")
API_KEY = dbutils.secrets.get(SECRET_SCOPE, "api-key")
try:
    AUTH_TOKEN = dbutils.secrets.get(SECRET_SCOPE, "auth-token")
except Exception:
    # The brief states the bearer token may equal the API key.
    AUTH_TOKEN = API_KEY

print(f"API host: {API_BASE_URL.split('//')[-1].split('/')[0]}")
print(f"API key: set ({len(API_KEY)} chars)")

# COMMAND ----------

# MAGIC %md ## Create the Delta tables
# MAGIC
# MAGIC In the `raw` schema, because that is the source name dbt declares — so
# MAGIC dbt reads these directly with no intermediate step.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")

FQ_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.bronze_transactions"
FQ_QUARANTINE = f"{CATALOG}.{RAW_SCHEMA}.quarantine_transactions"
FQ_WATERMARK = f"{CATALOG}.{RAW_SCHEMA}.pipeline_watermark"
FQ_METRICS = f"{CATALOG}.{RAW_SCHEMA}.ingestion_run_metrics"

# Every transaction field is STRING. Bronze is a faithful record of what the
# API sent; interpreting types is silver's job, done once and explicitly.
# Storing amount as STRING also keeps money out of floating point before
# anyone has decided on a precision.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ_BRONZE} (
    transaction_id      STRING  NOT NULL,
    account_id          STRING  NOT NULL,
    transaction_date    STRING  NOT NULL,
    amount              STRING  NOT NULL,
    currency            STRING  NOT NULL,
    transaction_type    STRING  NOT NULL,
    merchant_name       STRING  NOT NULL,
    merchant_category   STRING  NOT NULL,
    status              STRING  NOT NULL,
    country_code        STRING  NOT NULL,
    source_id           INT,
    natural_key_hash    STRING  NOT NULL,
    is_duplicate        INT     NOT NULL,
    duplicate_of        STRING,
    ingestion_timestamp STRING  NOT NULL,
    ingestion_run_id    STRING  NOT NULL,
    source_system       STRING  NOT NULL
) USING DELTA
COMMENT 'Bronze: every validated record as the API sent it, plus ingestion metadata. Duplicates present and flagged, not removed.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ_QUARANTINE} (
    quarantine_id       STRING  NOT NULL,
    transaction_id      STRING,
    source_id           INT,
    raw_record          STRING  NOT NULL,
    error_reason        STRING  NOT NULL,
    error_count         INT     NOT NULL,
    failed_fields       STRING  NOT NULL,
    ingestion_timestamp STRING  NOT NULL,
    ingestion_run_id    STRING  NOT NULL,
    source_system       STRING  NOT NULL
) USING DELTA
COMMENT 'Quarantine: records failing validation, with every reason. Append-only across runs.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ_WATERMARK} (
    source_system   STRING  NOT NULL,
    watermark_value STRING,
    run_id          STRING  NOT NULL,
    updated_at      STRING  NOT NULL
) USING DELTA
COMMENT 'High-water mark: max transaction_date among VALIDATED records. Advanced only on a fully successful run.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ_METRICS} (
    run_id                STRING  NOT NULL,
    run_mode              STRING  NOT NULL,
    run_status            STRING  NOT NULL,
    source_system         STRING  NOT NULL,
    watermark_before      STRING,
    watermark_after       STRING,
    effective_filter_from STRING,
    records_fetched       INT     NOT NULL,
    records_valid         INT     NOT NULL,
    records_quarantined   INT     NOT NULL,
    records_duplicate     INT     NOT NULL,
    duration_seconds      DOUBLE  NOT NULL,
    started_at            STRING  NOT NULL,
    completed_at          STRING  NOT NULL
) USING DELTA
COMMENT 'One row per run. Makes quarantine rate and watermark movement queryable as history, not only visible in logs.'
""")

print("Delta tables ready")

# COMMAND ----------

# MAGIC %md ## Read the watermark and decide the filter

# COMMAND ----------

import uuid
from datetime import datetime, timezone

run_id = ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
          + "-" + uuid.uuid4().hex[:6])
started = datetime.now(timezone.utc)
started_iso = started.strftime("%Y-%m-%dT%H:%M:%SZ")

_wm = spark.sql(
    f"SELECT watermark_value FROM {FQ_WATERMARK} "
    f"WHERE source_system = '{SOURCE_SYSTEM}' LIMIT 1"
).collect()
watermark_before = _wm[0][0] if _wm and _wm[0][0] else None

if MODE == "full" or not watermark_before:
    since = None
    effective_from = None
    print("mode=full: reading all records, no date filter")
else:
    # transaction_date is when the transaction happened, not when the source
    # learned about it. A record dated Tuesday but written Thursday is
    # invisible once the watermark passes Wednesday, so a trailing window is
    # re-scanned every run. The MERGE below makes the overlap free.
    effective_from = apply_lookback(watermark_before, LOOKBACK_HOURS)
    since = effective_from
    print(f"mode=incremental: watermark={watermark_before} "
          f"lookback={LOOKBACK_HOURS}h filter=gte.{effective_from}")

# COMMAND ----------

# MAGIC %md ## Fetch from the API
# MAGIC
# MAGIC Behaviours below came from probing the endpoint before any client was
# MAGIC written: 206 is a success code, an invalid `limit` is silently ignored
# MAGIC rather than rejected, and ordering must be by the surrogate `id`
# MAGIC because offset paging over a non-unique sort key can drop or duplicate
# MAGIC rows between requests.

# COMMAND ----------

import json
import time
import urllib.error
import urllib.parse
import urllib.request

PAGE_SIZE = 100
MAX_RETRIES = 5
BACKOFF_BASE = 0.5
TIMEOUT = 30
SUCCESS_CODES = {200, 206}
RETRYABLE = {429, 500, 502, 503, 504}


def fetch_page(offset: int, since_value: str | None) -> list[dict]:
    params = {"limit": PAGE_SIZE, "offset": offset, "order": "id.asc"}
    if since_value:
        params["transaction_date"] = f"gte.{since_value}"
    url = f"{API_BASE_URL}/transactions?" + urllib.parse.urlencode(params, safe=".")

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        req = urllib.request.Request(url)
        req.add_header("apikey", API_KEY)
        req.add_header("Authorization", f"Bearer {AUTH_TOKEN}")
        req.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                if resp.status in SUCCESS_CODES:
                    return json.loads(resp.read())
                last_error = f"HTTP {resp.status}"
        except urllib.error.HTTPError as e:
            if e.code not in RETRYABLE:
                # 401, 400, 404 are configuration or contract errors that will
                # never succeed. Retrying wastes time and hides the cause.
                raise RuntimeError(f"HTTP {e.code}: {e.read()[:300]!r}") from e
            last_error = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError) as e:
            last_error = f"transport: {e}"

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"  retry {attempt}/{MAX_RETRIES} after {last_error}, "
                  f"waiting {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"exhausted {MAX_RETRIES} attempts: {last_error}")


raw_records: list[dict] = []
offset = 0
while True:
    page = fetch_page(offset, since)
    print(f"fetched page offset={offset} records={len(page)}")
    raw_records.extend(page)
    # Stop on a short or empty page. Both are needed: short means exhausted,
    # empty covers a total that is an exact multiple of the page size.
    if len(page) < PAGE_SIZE:
        break
    offset += PAGE_SIZE

print(f"\nfetched {len(raw_records)} records")

# COMMAND ----------

# MAGIC %md ## Validate
# MAGIC
# MAGIC Every record is checked against all ten field rules. Failures collect
# MAGIC *every* violation rather than stopping at the first: the planted
# MAGIC defects carry three and four violations each, so short-circuiting would
# MAGIC report one and hide the rest, and a quarantine row saying "invalid"
# MAGIC helps nobody.

# COMMAND ----------

valid = []
invalid = []
for rec in raw_records:
    txn, reasons = validate_record(rec)
    if txn is None:
        invalid.append((rec, reasons))
    else:
        valid.append(txn)

print(f"valid: {len(valid)}   quarantined: {len(invalid)}")
for rec, reasons in invalid:
    print(f"  {rec.get('transaction_id')}: {len(reasons)} violations")
    for r in reasons:
        print(f"      {r}")

# COMMAND ----------

# MAGIC %md ## Detect duplicates
# MAGIC
# MAGIC The natural key is every field except `transaction_id`. Duplicates are
# MAGIC **flagged, not dropped** — bronze stays a faithful record, so the
# MAGIC decision is reversible and the duplicate rate stays measurable. The
# MAGIC survivor is the lowest `transaction_id`: arbitrary, but stable across
# MAGIC reruns, which is what keeps the mart idempotent.

# COMMAND ----------

hashes, duplicate_of, duplicate_count = flag_duplicates(valid)
print(f"duplicate groups found, {duplicate_count} redundant records flagged")

# COMMAND ----------

# MAGIC %md ## Write bronze with MERGE
# MAGIC
# MAGIC Spark SQL has no `INSERT ... ON CONFLICT`; `MERGE INTO` is the Delta
# MAGIC equivalent. It is what makes a rerun safe: the watermark filter uses
# MAGIC `gte`, so the boundary record is deliberately re-read on every
# MAGIC incremental run and a blind insert would duplicate it each time.

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

BRONZE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", StringType(), False),
    StructField("amount", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("status", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("source_id", IntegerType(), True),
    StructField("natural_key_hash", StringType(), False),
    StructField("is_duplicate", IntegerType(), False),
    StructField("duplicate_of", StringType(), True),
    StructField("ingestion_timestamp", StringType(), False),
    StructField("ingestion_run_id", StringType(), False),
    StructField("source_system", StringType(), False),
])

ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

bronze_rows = [
    (
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        # Normalised decimal text, never a float: binary floating point cannot
        # represent decimal currency exactly.
        format(t.amount.normalize(), "f"),
        t.currency,
        t.transaction_type,
        t.merchant_name,
        t.merchant_category,
        t.status,
        t.country_code,
        None,
        hashes[t.transaction_id],
        1 if duplicate_of.get(t.transaction_id) else 0,
        duplicate_of.get(t.transaction_id),
        ingested_at,
        run_id,
        SOURCE_SYSTEM,
    )
    for t in valid
]

if bronze_rows:
    spark.createDataFrame(bronze_rows, BRONZE_SCHEMA) \
         .createOrReplaceTempView("bronze_incoming")

    spark.sql(f"""
        MERGE INTO {FQ_BRONZE} AS target
        USING bronze_incoming AS source
           ON target.transaction_id = source.transaction_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"merged {len(bronze_rows)} records into bronze")
else:
    print("no valid records to merge")

# COMMAND ----------

# MAGIC %md ## Write quarantine
# MAGIC
# MAGIC Idempotent within a run — this run's own rows are cleared first, so
# MAGIC re-running a failed run does not double-count. Append-only *across*
# MAGIC runs, because "was bad yesterday, still bad today" is an audit fact
# MAGIC worth keeping.

# COMMAND ----------

QUARANTINE_SCHEMA = StructType([
    StructField("quarantine_id", StringType(), False),
    StructField("transaction_id", StringType(), True),
    StructField("source_id", IntegerType(), True),
    StructField("raw_record", StringType(), False),
    StructField("error_reason", StringType(), False),
    StructField("error_count", IntegerType(), False),
    StructField("failed_fields", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False),
    StructField("ingestion_run_id", StringType(), False),
    StructField("source_system", StringType(), False),
])

spark.sql(f"DELETE FROM {FQ_QUARANTINE} WHERE ingestion_run_id = '{run_id}'")

quarantine_rows = [
    (
        f"{run_id}-{i:05d}",
        rec.get("transaction_id"),
        None,
        json.dumps(rec, sort_keys=True, default=str),
        "; ".join(reasons),
        len(reasons),
        ",".join(sorted({r.split(":")[0] for r in reasons})),
        ingested_at,
        run_id,
        SOURCE_SYSTEM,
    )
    for i, (rec, reasons) in enumerate(invalid)
]

if quarantine_rows:
    spark.createDataFrame(quarantine_rows, QUARANTINE_SCHEMA) \
         .write.mode("append").saveAsTable(FQ_QUARANTINE)
    print(f"wrote {len(quarantine_rows)} quarantine records")
else:
    print("no records quarantined this run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advance the watermark — from VALIDATED records only
# MAGIC
# MAGIC **The single most important step.** This dataset contains two invalid
# MAGIC records dated April and November 2024, later than every valid record. A
# MAGIC watermark taken over *raw* records lands on the November date; the next
# MAGIC run then asks the API for records newer than that, receives nothing,
# MAGIC and exits successfully — and so does every run after it. No error, no
# MAGIC alert, a green pipeline and no data.
# MAGIC
# MAGIC It is advanced only here, after every record has been written. Moving
# MAGIC it earlier means a mid-run failure skips unprocessed records
# MAGIC permanently.

# COMMAND ----------

# A zero-record incremental run is not a normal outcome: the lookback window
# always contains the record that set the watermark, so an empty result means
# the filter is malformed. Failing loudly is the difference between a caught
# bug and silent data loss.
if MODE == "incremental" and effective_from and len(raw_records) == 0:
    raise RuntimeError(
        f"incremental run fetched 0 records with filter gte.{effective_from}, "
        f"but the window should contain the watermark record "
        f"({watermark_before}). Refusing to report success."
    )

new_watermark = max_valid_transaction_date(valid)
watermark_after = watermark_before
if new_watermark and (not watermark_before or new_watermark > watermark_before):
    watermark_after = new_watermark

spark.sql(
    f"DELETE FROM {FQ_WATERMARK} WHERE source_system = '{SOURCE_SYSTEM}'"
)
spark.sql(f"""
    INSERT INTO {FQ_WATERMARK} (source_system, watermark_value, run_id, updated_at)
    VALUES ('{SOURCE_SYSTEM}',
            {'NULL' if watermark_after is None else repr(watermark_after)},
            '{run_id}', '{ingested_at}')
""")

print(f"watermark: {watermark_before} -> {watermark_after}")

# COMMAND ----------

# MAGIC %md ## Record run metrics

# COMMAND ----------

completed = datetime.now(timezone.utc)
duration = round((completed - started).total_seconds(), 3)


def _lit(v):
    return "NULL" if v is None else repr(str(v))


spark.sql(f"""
    INSERT INTO {FQ_METRICS} VALUES (
        '{run_id}', '{MODE}', 'success', '{SOURCE_SYSTEM}',
        {_lit(watermark_before)}, {_lit(watermark_after)}, {_lit(effective_from)},
        {len(raw_records)}, {len(valid)}, {len(invalid)}, {duplicate_count},
        {duration}, '{started_iso}',
        '{completed.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    )
""")

summary = {
    "run_id": run_id,
    "mode": MODE,
    "target": f"{CATALOG}.{RAW_SCHEMA}",
    "watermark_before": watermark_before,
    "effective_filter_from": effective_from,
    "watermark_after": watermark_after,
    "fetched": len(raw_records),
    "valid": len(valid),
    "quarantined": len(invalid),
    "duplicate": duplicate_count,
    "duration_seconds": duration,
}
print(json.dumps(summary, indent=2))

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {FQ_BRONZE})                        AS bronze_rows,
        (SELECT COUNT(*) FROM {FQ_BRONZE} WHERE is_duplicate = 1) AS flagged_duplicates,
        (SELECT COUNT(*) FROM {FQ_QUARANTINE})                    AS quarantine_rows,
        (SELECT watermark_value FROM {FQ_WATERMARK}
          WHERE source_system = '{SOURCE_SYSTEM}')                AS watermark
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: silver and gold
# MAGIC
# MAGIC dbt reads these tables directly — the `raw` schema is the source name
# MAGIC declared in `dbt_project/models/staging/schema.yml`:
# MAGIC
# MAGIC ```
# MAGIC cd dbt_project && dbt build --target databricks
# MAGIC ```
# MAGIC
# MAGIC - **silver** `transactions_staging.stg_transactions` — resolves
# MAGIC   duplicates, casts types. The single place where "what counts as a
# MAGIC   usable transaction" is answered, so a second mart cannot answer it
# MAGIC   differently.
# MAGIC - **gold** `transactions_marts.daily_account_summary` — one row per
# MAGIC   account per UTC day, completed transactions only, with an enforced
# MAGIC   model contract and 32 tests.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary))
