# Zero-dependency targets. Everything here runs on a bare Python 3.11+.
PYTHON ?= python3
CSV    ?= data/transactions.csv

.PHONY: help test ingest incremental transform outputs all clean check-secrets \
        verify dbt-export dbt-build dbt-docs dbt-all

help:
	@echo "make test        - run the test suite (65 tests)"
	@echo "make ingest      - Task 1: full ingestion"
	@echo "make incremental - Task 3: incremental run with watermark"
	@echo "make transform   - Task 2: build summary and assert quality"
	@echo "make outputs     - export sample outputs for review"
	@echo "make all         - clean, then the full sequence twice over"
	@echo ""
	@echo "make verify      - audit the submission against the assessment"
	@echo ""
	@echo "dbt path (requires: pip install -r requirements.txt):"
	@echo "make dbt-export  - materialise SQLite tables as CSV for dbt"
	@echo "make dbt-build   - dbt build: models plus schema tests"
	@echo "make dbt-docs    - generate lineage documentation"
	@echo "make dbt-all     - export, build, docs"
	@echo ""
	@echo "Append SOURCE=csv to run offline against the fixture."

test:
	$(PYTHON) -m unittest discover -s tests -v

ingest:
	$(PYTHON) -m ingestion.ingest_transactions $(ARGS)

incremental:
	$(PYTHON) -m ingestion.incremental_ingest $(ARGS)

transform:
	$(PYTHON) -m ingestion.run_transform

outputs:
	$(PYTHON) -m ingestion.export_outputs

# Full demonstration: two runs proving idempotency, then the mart.
all: clean
	$(PYTHON) -m unittest discover -s tests
	$(PYTHON) -m ingestion.ingest_transactions --source csv --csv-path $(CSV)
	$(PYTHON) -m ingestion.incremental_ingest --source csv --csv-path $(CSV)
	$(PYTHON) -m ingestion.run_transform
	$(PYTHON) -m ingestion.run_transform --check-idempotency
	$(PYTHON) -m ingestion.export_outputs

check-secrets:
	@git ls-files | grep -E '(^|/)(\.env|assignment\.env)$$' \
		&& (echo "ERROR: environment file is tracked"; exit 1) \
		|| echo "no environment files tracked"

clean:
	rm -rf warehouse state
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

verify:
	$(PYTHON) scripts/verify_submission.py

# ---- dbt path -------------------------------------------------------------
#
# dbt reads CSV exports of the SQLite tables rather than the database file.
# DuckDB can only attach SQLite through the sqlite_scanner extension, and
# extension downloads are blocked on corporate networks (403 from
# extensions.duckdb.org), which would make the dbt path environment-dependent.
# CSV needs no extension and works everywhere.

dbt-export:
	$(PYTHON) scripts/export_for_dbt.py

dbt-build: dbt-export
	cd dbt_project && dbt build --target duckdb

dbt-docs:
	cd dbt_project && dbt docs generate --target duckdb
	@mkdir -p docs/dbt
	@cp dbt_project/target/index.html docs/dbt/index.html
	@echo "lineage docs written to docs/dbt/index.html"

dbt-all: dbt-export dbt-build dbt-docs
