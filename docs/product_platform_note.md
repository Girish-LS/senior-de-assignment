# Product and Platform Design Note

## Assumptions I would challenge before productionising

**Summing amounts across currencies.** `total_debit_amount` adds together up
to seven currencies, so the number is arithmetically valid and financially
meaningless — USD plus JPY is not a quantity. I implemented the specification
as written rather than silently correcting it, because the fix is a product
decision, not an implementation detail: it needs an FX rate source, an as-of
convention, and a ruling on whether the mart reports native or converted
amounts. This is the first thing I would raise. The two credible designs are a
per-currency grain, or native amounts alongside a reporting-currency column
derived from a dated FX table. The `currencies` column is in the output
precisely so no consumer can mistake the total for a single-currency figure.

**Watermarking on business time.** `transaction_date` records when a
transaction happened, not when the source learned about it. The API exposes no
`created_at`, so business time is the only filter available — and it has a
failure mode this dataset demonstrates. A transaction dated Tuesday but
written to the source on Thursday is invisible once the watermark has passed
Wednesday. Not delayed: permanently invisible, with the pipeline reporting
success. I would ask the source owner to expose an insertion timestamp, which
would make the watermark exact and reduce the lookback to a small safety
margin. Failing that, the undocumented `id` sequence is a usable cursor.

**That the natural key is genuinely the natural key.** Deduplication compares
every field except `transaction_id`. Two legitimately distinct transactions —
same account, same merchant, same amount, same second — would be wrongly
collapsed. At this volume that does not occur, but the definition is an
assumption about the business, not a property of the data, and it should be
confirmed rather than inferred.

**That duplicates are a source bug at all.** I deduplicate downstream, which
treats the symptom. The question worth asking is why the source emits the same
transaction under two identifiers. That is usually a retry or replay defect
upstream, and fixing it there removes the problem for every consumer rather
than for this one.

**That `transaction_id` is unique.** The schema says so. The pipeline asserts
it rather than trusting it, because a silently non-unique key would make the
upsert overwrite real records.

## What I would monitor, and what I would alert on

| Signal | Why | Action |
|---|---|---|
| Run failed, or did not run at all | The missing-run case is the one usually forgotten; a scheduler that stopped firing produces no failures | Page |
| Freshness against the declared SLA | The consumer-visible commitment | Page |
| Quarantine rate, as a proportion with trend | Absolute counts mislead when volume varies | Alert on step change |
| Consecutive zero-record runs | Indistinguishable from health on most dashboards | Alert on unexpected streak |
| Volume anomaly against recent baseline | A large drop is often a silent upstream filter change | Alert |
| HTTP retry rate | Leading indicator of source degradation, visible before failures start | Warn |
| Data quality assertion failures | Quality must gate publication, not decorate it | Alert |
| Late-arrival rate within the lookback | Tells you whether the window is correctly sized; without it the length is a guess that never gets corrected | Warn as it approaches the window |
| Cost per run | Unexamined compute growth is the normal failure | Review on trend |

Every alert needs a named owner and a runbook entry. An alert nobody owns
becomes noise, noisy alerts get muted, and a muted alert is worse than no
alert because it creates false confidence.

The run metrics table exists so these are queryable as history rather than
only visible in logs. A quarantine rate is only meaningful against its own
trend.

## Making this reusable for the next ten APIs

I deliberately did not build a framework. An abstraction derived from a single
example encodes that example's accidents as if they were requirements, and
unwinding it later costs more than the duplication it saved. What I did build
is the seam: the HTTP client owns pagination, retry and auth and knows nothing
about transactions, while validation and modelling are source-specific.

The shape I would grow into, once two or three real sources have shown which
parts actually vary:

- A declarative source definition — endpoint, auth method, pagination style,
  incremental column, schema and rules, target tables, SLA, owner — consumed
  by a shared ingestion runtime.
- Contracts, documentation and quality checks generated from that same
  definition, so schema, docs and tests cannot drift apart.
- Scaffolding so onboarding a source is configuration plus review rather than
  new code.
- dbt packages for the transformation patterns that repeat.

The honest endpoint: at around ten sources, building bespoke tooling stops
being justifiable and the right move is to adopt Azure Data Factory or `dlt`
and keep hand-written code only for sources with genuinely unusual semantics.
Knowing when to stop building your own is part of the answer.

## What makes `daily_account_summary` trustworthy

Correct numbers are necessary and not sufficient. A consumer trusts a dataset
when they can answer five questions without asking a person: who owns it, when
it last updated and when it will next, what each column means, what feeds it
and what depends on it, and whether its quality checks currently pass.

Concretely, in this repository: a data contract declaring owner, SLA and
column semantics; `updated_at` on every row so freshness is visible in the
data itself; assertions that gate publication rather than reporting after the
fact; documented and deterministic handling of the awkward cases —
`top_category` ties broken by name, null for credit-only days, currencies
sorted; and the mixed-currency limitation stated in the model, the contract
and this note rather than left for an analyst to discover.

The last one matters most. Trust is not built by having no limitations; it is
built by the limitations being disclosed by the producer rather than found by
the consumer.

## Exposing lineage, ownership, documentation and quality status

In this repository: a contract file per published dataset, ownership metadata
on every dbt model, a dbt exposure representing the downstream dashboard so
lineage runs from source through to consumption, and `dbt docs` output giving
a column-level lineage graph.

The layers are named for what they are — `bronze`, `silver`, `gold` in Unity
Catalog — which required overriding dbt's default `generate_schema_name`,
because it concatenates the target schema with a model's custom schema rather
than replacing it. A small thing, but a consumer reading a schema name should
learn the layer from it rather than have to ask.

In production on Databricks, Unity Catalog becomes the system of record —
automatic column-level lineage, centralised access control, tagging and audit
— and the contract file remains as the versioned, reviewable statement of what
the producer commits to. The two are complementary: the catalog describes what
*is*, the contract states what is *promised*.

Quality status belongs next to the data, not in a separate tool nobody opens.
Test results published as a queryable table, surfaced in the catalog, is the
minimum.

## Trade-offs made because of the time limit and the environment

**Two execution paths rather than one.** The primary path is Databricks:
ingestion writes bronze, quarantine, the watermark and run metrics as Delta
tables in Unity Catalog (`workspace.bronze`), and dbt builds silver
(`workspace.silver.stg_transactions`) and gold
(`workspace.gold.daily_account_summary`) on a SQL warehouse. Verified from an
empty catalog: 352 fetched, 349 valid, 3 quarantined, 5 flagged, then a rerun
that re-read 17 records inside the lookback and inserted none.

There is also a local path using SQLite and DuckDB that runs with no account,
no credentials and no install step, because `sqlite3` ships with Python. I kept
it deliberately. Reproducibility is a graded criterion and a reviewer being
able to clone the repository and see results without provisioning anything is
worth more than a single canonical path.

**How that came about is worth being honest about.** The machine available
blocks the public package index, so neither `duckdb` nor `dbt-core` could
initially be installed and the pipeline was written against the standard
library alone. The internal artifact repository was configured later. By then
the mart existed twice — hand-written SQL through `sqlite3`, and a dbt model —
and comparing them row by row gave 257 rows each, identical grain, zero value
mismatches. An environment constraint produced the strongest correctness
evidence in the submission, because two independent implementations agreeing do
not share a bug.

**The genuine compromise is that ingestion runs outside Databricks.** The
notebook in `databricks/notebooks/` runs on serverless compute, and I verified
serverless can reach the API — but the path I executed most is an external
Python process writing over the SQL connector, because it runs from the
repository with three environment variables and nothing else. In production
this would be a Databricks Job or Azure Data Factory, so the compute sits
beside the storage. The shape is identical; only the host differs.

**What running on two engines cost, and bought.** It surfaced three
portability defects invisible on either engine alone: a contract declaring
`timestamp with time zone`, which does not exist in Spark SQL; a
`cast(null as varchar)`, which Spark rejects without a length; and `listagg`
ignoring `order_by` on Spark, so `currencies` is sorted on DuckDB and unsorted
on Databricks. The first two are fixed; the third is documented rather than
papered over. The lesson: **a dbt model contract is declared in the
warehouse's own type system, so portable SQL is not portable until it has run
on the second warehouse.**

**Hand-written validation rather than Pydantic.** Same cause. The rules are
identical. The cost is more code; the benefit is that every rule is legible
without knowing a library's coercion semantics.

**Orchestration defined, not deployed.** Two definitions are committed: an
Airflow DAG in `dags/`, because Airflow is the named tool, and a two-task
Databricks Job in `databricks/jobs/`, because on this platform Workflows would
arguably be the better choice for a single pipeline — no separate
infrastructure to run. Airflow earns its overhead when there are cross-system
dependencies. Neither is deployed, and I would not claim otherwise.
The DAG shows the intended task decomposition, retry boundaries and alerting,
which is the part worth reviewing.

**No FX handling.** Discussed above. Out of scope for the time available and a
product decision rather than an implementation one.

**Reuse described rather than built.** Deliberate, per the section above, and
also what the brief asks for.

**Single mart.** A real platform would have dimensional models behind this.
One mart is what the assessment asks for and more would be scope creep.

## AI tool usage

I used Claude as a pair programmer throughout: to profile the dataset, to
probe the API before writing the client, to draft implementation and tests,
and as a reviewer for design decisions.

Being precise about the division of labour, since a vague disclosure is worth
less than an accurate one. Most of the code was drafted by the model. The
direction was mine: I set the scope, decided which trade-offs to accept, and
rejected suggestions I disagreed with — including the recommendation to move
off a corporate-managed machine to avoid its network restrictions, and the
argument for a leaner design record than the one in
`docs/architecture_decision_record.md`. Two artefacts here exist because I
asked for them rather than because they were proposed: the captured run
transcript in `outputs/run_transcript.txt`, and `scripts/verify_submission.py`,
which audits the submission against the assessment's own checklist.

Where I accepted a recommendation, it was after understanding the reasoning,
not because it arrived first. The watermark design is the clearest example: I
questioned why `gte` rather than `gt`, and the answer — that `gt` silently
drops any record sharing the boundary timestamp, which is why the load must
upsert — is the reason those three decisions are documented as one
interlocking design rather than three independent choices.

Verification was independent of generation, which is the part that matters:

- The dataset was profiled twice by different methods — a direct scan against
  the JSON schema, and the validator itself — and the results reconciled
  exactly: 349 valid, 3 invalid, 5 duplicate pairs. Agreement between two
  methods is what makes the number trustworthy.
- The API was probed before the client was written. Three findings changed the
  design and each would otherwise have been a real defect: `amount` arrives as
  a JSON string, filtered responses return HTTP 206 rather than 200, and an
  invalid `limit` returns 200 with the parameter silently ignored.
- The mart is implemented twice — hand-written SQL against SQLite and a dbt
  model against DuckDB — and the outputs compared row by row: 257 rows each,
  identical grain, zero value mismatches. Two independent implementations
  agreeing is stronger evidence than either passing its own tests.
- Two bugs were caught by tests that existed because the expected answer was
  established first. Strict-mode enum validation quarantined all 352 records;
  a semicolon inside a SQL comment broke statement splitting. Neither would
  have been visible from output that merely looked plausible.
- Idempotency is asserted by rebuilding and comparing, not claimed.

One assumption the tooling did not catch, and the process did: the supplied
CSV backup is not an export of the live API. It has the same shape — 352
records, 3 invalid, 5 duplicate pairs — but different content, with entirely
disjoint duplicate pairs. Diffing the two rather than assuming equivalence is
what surfaced it, and it means the committed outputs are generated from the
API rather than from the fixture.

The discipline I would state plainly: generated code is a draft until
something other than the generator has confirmed it. Establishing the expected
answer before writing the code is what made both bugs findable.
