"""Illustrative Airflow DAG. NOT DEPLOYED.

Included to show how this pipeline would be orchestrated in production, and
in particular how it decomposes into independently retryable tasks. There is
no target Airflow environment for this assessment, so the file is documentation
rather than something that has run.

Why tasks are split where they are
----------------------------------
Each boundary is a failure boundary. Ingestion talks to an external API and is
the only step exposed to transient network faults, so it carries retries.
Transformation is deterministic and local: if it fails, retrying it unchanged
will fail again, so it does not. Tests are separated from the build so that a
quality failure is a distinct, visible state rather than a generic pipeline
error - and so the decision about whether failing tests should block
publication is explicit rather than implied.

A note on the platform choice
-----------------------------
For a single pipeline, Databricks Workflows would be the better production
choice: native to the compute, no standing scheduler infrastructure, lower
operational cost. Airflow earns its overhead when there are many pipelines
with cross-system dependencies. Airflow is shown here because it is the
orchestrator named in the role, but the argument above is one I would make in
an architecture review.
"""

from __future__ import annotations

from datetime import datetime, timedelta

# These imports fail outside an Airflow environment, which is expected.
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - file is illustrative
    DAG = None  # type: ignore[assignment]


def alert_on_failure(context) -> None:
    """Failure callback.

    In production this routes by severity to the owning team's channel and
    pager. Every alert needs a named owner and a runbook entry: an alert
    nobody owns becomes noise, noisy alerts get muted, and a muted alert is
    worse than no alert because it creates false confidence.
    """
    task = context["task_instance"]
    message = (
        f"transactions_pipeline failed\n"
        f"task: {task.task_id}\n"
        f"run: {context['run_id']}\n"
        f"log: {task.log_url}\n"
        f"runbook: https://wiki.example.com/runbooks/transactions-pipeline"
    )
    print(message)  # replace with Slack / PagerDuty hook


def check_freshness_sla(**context) -> None:
    """Assert the mart is within its declared freshness SLA.

    Separate from the data quality assertions on purpose: correctness and
    timeliness are different promises and fail for different reasons. A mart
    can be perfectly correct and two days stale.
    """
    raise NotImplementedError("illustrative")


DEFAULT_ARGS = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email_on_failure": False,  # handled by the callback instead
    "on_failure_callback": alert_on_failure,
    "retries": 0,  # overridden per task; most steps should not blind-retry
}

if DAG is not None:
    with DAG(
        dag_id="transactions_pipeline",
        description="Ingest transactions, build the daily account summary",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 1, 1),
        schedule="0 2 * * *",
        # Backfill is disabled deliberately. The watermark is pipeline state
        # rather than a function of the execution date, so scheduler catchup
        # would launch many runs competing over the same watermark. Backfill
        # here means a deliberate full reload, not a replay of past intervals.
        catchup=False,
        max_active_runs=1,  # concurrent runs would race on the watermark
        tags=["ingestion", "payments", "daily"],
        doc_md=__doc__,
    ) as dag:

        ingest = BashOperator(
            task_id="ingest_incremental",
            bash_command="python -m ingestion.incremental_ingest",
            # The only task exposed to an external system, so the only one
            # where a retry has a reasonable chance of succeeding.
            retries=3,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=30),
            execution_timeout=timedelta(minutes=30),
            doc_md="Fetch, validate, quarantine, deduplicate, persist to bronze.",
        )

        build_summary = BashOperator(
            task_id="build_daily_account_summary",
            bash_command="cd dbt_project && dbt run --select daily_account_summary",
            # Deterministic and local: a retry of unchanged input fails again.
            retries=0,
            execution_timeout=timedelta(minutes=15),
        )

        test_summary = BashOperator(
            task_id="test_daily_account_summary",
            bash_command="cd dbt_project && dbt test --select daily_account_summary",
            retries=0,
            execution_timeout=timedelta(minutes=10),
            doc_md=(
                "Separated from the build so a quality failure is its own "
                "visible state, and so blocking publication on failing tests "
                "is an explicit policy decision."
            ),
        )

        freshness = PythonOperator(
            task_id="check_freshness_sla",
            python_callable=check_freshness_sla,
            retries=0,
        )

        publish = BashOperator(
            task_id="publish_quality_status",
            bash_command="python -m ingestion.export_outputs",
            retries=1,
            doc_md=(
                "Publish run metrics and assertion results so consumers can "
                "see quality status without asking an engineer."
            ),
        )

        ingest >> build_summary >> test_summary >> freshness >> publish
