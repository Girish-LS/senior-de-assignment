# Zero-dependency targets. Everything here runs on a bare Python 3.11+.
PYTHON ?= python3
CSV    ?= data/transactions.csv

.PHONY: help test ingest incremental transform outputs all clean check-secrets

help:
	@echo "make test        - run the test suite (60 tests)"
	@echo "make ingest      - Task 1: full ingestion"
	@echo "make incremental - Task 3: incremental run with watermark"
	@echo "make transform   - Task 2: build summary and assert quality"
	@echo "make outputs     - export sample outputs for review"
	@echo "make all         - clean, then the full sequence twice over"
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
