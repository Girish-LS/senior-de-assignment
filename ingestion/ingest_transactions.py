"""CLI entry point for raw ingestion (Task 1).

    python -m ingestion.ingest_transactions
    python -m ingestion.ingest_transactions --source csv --csv-path data/transactions.csv
    python -m ingestion.ingest_transactions --mode incremental

Reads all configuration from the environment. No credential is ever passed on
the command line, where it would land in shell history and process listings.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from ingestion.config import ConfigError, load_settings
from ingestion.pipeline import run_ingestion


def configure_logging(verbose: bool = False) -> None:
    """Structured-ish logging to stderr.

    Deliberately excludes request bodies and headers. In a payments context
    a full record dump writes account identifiers into log storage, which
    routinely has weaker access controls than the warehouse itself.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest transactions into the bronze layer.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="full reads everything; incremental filters from the watermark",
    )
    parser.add_argument(
        "--source",
        choices=("api", "csv"),
        default="api",
        help="read the live API, or the offline CSV fixture",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=Path("data/transactions.csv"),
        help="path to the fixture when --source csv",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.verbose)
    log = logging.getLogger("ingest")

    try:
        # The offline CSV path must run without API credentials.
        settings = load_settings(require_api=(args.source == "api"))
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        return 2

    log.info("settings: %s", json.dumps(settings.redacted()))

    try:
        result = run_ingestion(
            settings,
            mode=args.mode,
            source=args.source,
            csv_path=args.csv_path,
        )
    except Exception as exc:  # noqa: BLE001 - top level: report and exit non-zero
        log.error("ingestion failed: %s: %s", type(exc).__name__, exc)
        return 1

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
