"""CLI entry point for incremental ingestion (Task 3).

    python -m ingestion.incremental_ingest
    python -m ingestion.incremental_ingest --source csv --csv-path data/transactions.csv

Thin wrapper over the same pipeline as full ingestion, differing only in the
filter applied. It additionally writes a watermark snapshot to outputs/ after
each run so the two-run demonstration the assessment asks for is inspectable
without querying the warehouse.

Behaviours, spelled out because the assessment asks for them explicitly:

first run          No successful run exists, so there is no watermark. The
                   pipeline reads everything, exactly as a full load, and
                   sets the watermark from the highest *valid*
                   transaction_date it saw.

no new data        The filtered request returns nothing. The watermark is
                   unchanged and the run exits zero. This is a normal
                   outcome, not an error - but it is counted and logged,
                   because a silently empty pipeline is indistinguishable
                   from a healthy one on a dashboard. A run of consecutive
                   empty runs is itself an alert condition.

late-arriving data Covered by the lookback window, bounded by its length.
                   Anything arriving later than the window is missed, which
                   is why the window length is a deliberate risk decision
                   rather than a default.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from ingestion import storage
from ingestion.config import ConfigError, load_settings
from ingestion.ingest_transactions import build_parser, configure_logging
from ingestion.pipeline import SOURCE_SYSTEM, run_ingestion


def write_watermark_snapshot(settings, result: dict, run_index: int) -> Path:
    """Persist watermark state to outputs/ as a reviewable artefact."""
    conn = storage.connect(settings.warehouse_path)
    try:
        history = storage.watermark_history(conn, SOURCE_SYSTEM)
    finally:
        conn.close()

    snapshot = {
        "source_system": SOURCE_SYSTEM,
        "snapshot_taken_at": storage.utc_now_iso(),
        "lookback_hours": settings.lookback_hours,
        "current_watermark": result.get("watermark_after"),
        "this_run": result,
        "run_history": history,
    }
    path = settings.outputs_dir / f"watermark_run{run_index}.json"
    storage.write_json(path, snapshot)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    parser.add_argument(
        "--snapshot-index",
        type=int,
        default=None,
        help="write outputs/watermark_run<N>.json after the run",
    )
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    log = logging.getLogger("incremental")

    try:
        # The offline CSV path must run without API credentials.
        settings = load_settings(require_api=(args.source == "api"))
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        return 2

    try:
        result = run_ingestion(
            settings,
            mode="incremental",
            source=args.source,
            csv_path=args.csv_path,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("incremental ingestion failed: %s: %s", type(exc).__name__, exc)
        return 1

    if args.snapshot_index is not None:
        path = write_watermark_snapshot(settings, result, args.snapshot_index)
        log.info("watermark snapshot written to %s", path)

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
