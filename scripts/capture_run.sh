#!/usr/bin/env bash
# Run the full pipeline and capture console output as evidence.
#
# Companion to capture_run.ps1 so the transcript is reproducible on any
# platform, not only the machine it was first generated on.
#
#   ./scripts/capture_run.sh            # live API, needs credentials
#   ./scripts/capture_run.sh csv        # bundled fixture, needs none
#   REDACT=1 ./scripts/capture_run.sh   # mask the API host for a public repo

set -euo pipefail

SOURCE="${1:-api}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON=".venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

mkdir -p outputs
TRANSCRIPT="outputs/run_transcript.txt"

SOURCE_ARGS=()
if [ "$SOURCE" = "csv" ]; then
    SOURCE_ARGS=(--source csv --csv-path data/transactions.csv)
fi

# Reset generated state so stage 1 is a genuine first run.
rm -f state/*.json
rm -rf warehouse/*

{
    printf '%s\n' "========================================================================"
    printf '%s\n' "PIPELINE RUN TRANSCRIPT"
    printf '%s\n' "========================================================================"
    printf 'Captured    : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'Source      : %s\n' "$SOURCE"
    printf 'Python      : %s\n' "$($PYTHON --version 2>&1)"
    printf 'Platform    : %s\n' "$(uname -sr)"
    echo
    echo "Watermark state and warehouse cleared before this run, so stage 1 is a"
    echo "genuine first run and stage 2 a genuine incremental run."
    echo

    echo "------------------------------------------------------------------------"
    echo "TEST SUITE"
    echo "------------------------------------------------------------------------"
    $PYTHON -m unittest discover -s tests 2>&1
    echo

    for stage in \
        "Task 1  full ingestion|ingestion.ingest_transactions|args" \
        "Task 3  incremental ingestion|ingestion.incremental_ingest|args" \
        "Task 2  transform and assert|ingestion.run_transform|noargs" \
        "Export  sample outputs|ingestion.export_outputs|noargs"
    do
        name="${stage%%|*}"
        rest="${stage#*|}"
        module="${rest%%|*}"
        argmode="${rest##*|}"

        echo "------------------------------------------------------------------------"
        echo "$name"
        echo "------------------------------------------------------------------------"
        if [ "$argmode" = "args" ] && [ ${#SOURCE_ARGS[@]} -gt 0 ]; then
            $PYTHON -m "$module" "${SOURCE_ARGS[@]}" 2>&1
        else
            $PYTHON -m "$module" 2>&1
        fi
        echo
    done

    printf '%s\n' "========================================================================"
    printf '%s\n' "RUN COMPLETE"
    printf '%s\n' "========================================================================"
} 2>&1 | tee "$TRANSCRIPT"

if [ "${REDACT:-0}" = "1" ]; then
    sed -i.bak -E 's#https://[a-z0-9]+\.supabase\.co#https://<project>.supabase.co#g' "$TRANSCRIPT"
    rm -f "${TRANSCRIPT}.bak"
    echo "API host redacted in transcript."
fi

echo
echo "Transcript written to $TRANSCRIPT"
echo "No credentials appear in it: the pipeline logs key length, never the key."
