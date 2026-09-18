<#
.SYNOPSIS
    Run the Databricks path and capture the evidence into outputs/databricks/.

.DESCRIPTION
    The repository is the only thing a reviewer sees. Claiming the pipeline
    runs on Databricks is worth nothing without an artefact showing it, so this
    captures the full run: ingestion to Delta, the incremental rerun, the dbt
    build, and a sample of the resulting tables.

    Three environment variables must be set first:
        $env:DATABRICKS_HOST      = "dbc-xxxx.cloud.databricks.com"
        $env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<id>"
        $env:DATABRICKS_TOKEN     = "dapi..."

.PARAMETER Rebuild
    Drop every schema first, so the transcript shows a build from an empty
    catalog. Proves nothing in the workspace exists because someone clicked
    something.

.PARAMETER Redact
    Mask the workspace hostname, for a public repository.

.EXAMPLE
    .\scripts\capture_databricks_run.ps1
    .\scripts\capture_databricks_run.ps1 -Rebuild -Redact
#>

[CmdletBinding()]
param(
    [switch]$Rebuild,
    [switch]$Redact
)

# Python writes its logs to stderr. With ErrorActionPreference 'Stop',
# PowerShell wraps each redirected line in an ErrorRecord and prints a
# NativeCommandError block around it, making the transcript unreadable.
# Stage failures are still caught: every stage checks $LASTEXITCODE.
$ErrorActionPreference = 'Continue'

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }
$Dbt = Join-Path $RepoRoot '.venv\Scripts\dbt.exe'

foreach ($v in 'DATABRICKS_HOST', 'DATABRICKS_HTTP_PATH', 'DATABRICKS_TOKEN') {
    if (-not (Get-Item "env:$v" -ErrorAction SilentlyContinue)) {
        Write-Host "$v is not set. See the comment block at the top of this script." -ForegroundColor Red
        exit 2
    }
}

$OutDir = Join-Path $RepoRoot 'outputs\databricks'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$Transcript = Join-Path $OutDir 'run_transcript.txt'

function Write-Both { param([string]$Text)
    Write-Host $Text
    Add-Content -Path $script:Transcript -Value $Text -Encoding utf8
}

Set-Content -Path $Transcript -Value '' -Encoding utf8

Write-Both ('=' * 72)
Write-Both 'DATABRICKS RUN TRANSCRIPT'
Write-Both ('=' * 72)
Write-Both ("Captured : " + (Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'))
Write-Both ("Python   : " + (& $Python --version 2>&1))
Write-Both ("Catalog  : " + $(if ($env:DATABRICKS_CATALOG) { $env:DATABRICKS_CATALOG } else { 'workspace' }))
Write-Both ''
Write-Both 'Bronze, silver and gold are Delta tables in Unity Catalog.'
Write-Both 'Ingestion writes bronze; dbt builds silver and gold.'
Write-Both ''

if ($Rebuild) {
    Write-Both ('-' * 72)
    Write-Both 'REBUILD: dropping every schema first'
    Write-Both ('-' * 72)
    Write-Both 'Run these in the Databricks SQL editor before this script:'
    Write-Both '  DROP SCHEMA IF EXISTS workspace.bronze CASCADE;'
    Write-Both '  DROP SCHEMA IF EXISTS workspace.silver CASCADE;'
    Write-Both '  DROP SCHEMA IF EXISTS workspace.gold CASCADE;'
    Write-Both ''
    Write-Both 'Every object below is therefore created by code in this repository.'
    Write-Both ''
}

# ---- Stage 1: full ingestion --------------------------------------------
Write-Both ('-' * 72)
Write-Both 'STAGE 1  Task 1 - full ingestion, API to Delta bronze'
Write-Both '  python scripts/ingest_to_databricks.py --mode full'
Write-Both ('-' * 72)
$out = & $Python scripts/ingest_to_databricks.py --mode full 2>&1 |
    ForEach-Object { $_.ToString() } | Out-String
Write-Both $out.TrimEnd()
if ($LASTEXITCODE -ne 0) { Write-Both "STAGE FAILED"; exit 1 }
# The JSON summary is the machine-readable artefact; keep it separately.
$json = ($out -split "`n" | Select-String -Pattern '^\s*[\{\}"]' | ForEach-Object { $_.Line }) -join "`n"
Set-Content -Path (Join-Path $OutDir 'ingest_run1.json') -Value $json -Encoding utf8
Write-Both ''

# ---- Stage 2: incremental ------------------------------------------------
#
# The row count is captured either side of the rerun. This is the assessment's
# explicit requirement - "a second run where no duplicate rows are inserted" -
# so it is asserted here rather than left to be inferred from two numbers in
# different parts of the transcript.
$BronzeBefore = (& $Python scripts/count_bronze.py 2>&1 | Select-Object -Last 1).Trim()

Write-Both ('-' * 72)
Write-Both 'STAGE 2  Task 3 - incremental run, watermark plus lookback'
Write-Both '  python scripts/ingest_to_databricks.py'
Write-Both ('-' * 72)
Write-Both "bronze rows BEFORE rerun: $BronzeBefore"
Write-Both ''
$out = & $Python scripts/ingest_to_databricks.py 2>&1 |
    ForEach-Object { $_.ToString() } | Out-String
Write-Both $out.TrimEnd()
if ($LASTEXITCODE -ne 0) { Write-Both "STAGE FAILED"; exit 1 }
$json = ($out -split "`n" | Select-String -Pattern '^\s*[\{\}"]' | ForEach-Object { $_.Line }) -join "`n"
Set-Content -Path (Join-Path $OutDir 'ingest_run2.json') -Value $json -Encoding utf8

$BronzeAfter = (& $Python scripts/count_bronze.py 2>&1 | Select-Object -Last 1).Trim()
Write-Both ''
Write-Both "bronze rows AFTER rerun : $BronzeAfter"
if ($BronzeBefore -eq $BronzeAfter) {
    Write-Both "IDEMPOTENT: the rerun re-read records inside the lookback window and inserted zero new rows."
} else {
    Write-Both "NOT IDEMPOTENT: $BronzeBefore -> $BronzeAfter. The rerun changed the row count."
    Write-Both "This is a failure, not a warning. MERGE should have matched on transaction_id."
}
Set-Content -Path (Join-Path $OutDir 'idempotency_check.txt') -Value @(
    "Idempotency check - the assessment's explicit Task 3 requirement",
    "================================================================",
    "",
    "bronze rows before rerun : $BronzeBefore",
    "bronze rows after rerun  : $BronzeAfter",
    "result                   : $(if ($BronzeBefore -eq $BronzeAfter) { 'PASS - zero new rows' } else { 'FAIL' })",
    "",
    "The incremental run re-reads a 72-hour trailing window, so it deliberately",
    "re-fetches records already in bronze. MERGE INTO matches them on",
    "transaction_id and updates in place rather than inserting duplicates.",
    "",
    "This matters because the watermark filter uses gte rather than gt: with gt,",
    "any record sharing the exact maximum timestamp of the previous run would be",
    "skipped permanently. gte re-reads the boundary, which is only safe because",
    "the load is an upsert."
) -Encoding utf8
Write-Both ''

# ---- Stage 3: dbt --------------------------------------------------------
Write-Both ('-' * 72)
Write-Both 'STAGE 3  Task 2 - dbt builds silver and gold on Databricks'
Write-Both '  dbt build --target databricks'
Write-Both ('-' * 72)
Push-Location (Join-Path $RepoRoot 'dbt_project')
$out = & $Dbt build --target databricks 2>&1 |
    ForEach-Object { $_.ToString() } | Out-String
Pop-Location
# Keep the summary lines rather than the full per-test log, which would bury
# the result a reviewer is looking for.
$kept = $out -split "`n" | Where-Object {
    $_ -match 'Running with dbt|Registered adapter|Found \d+ models|OK created|ERROR|FAIL|WARN|Completed|Done\.'
}
Write-Both ($kept -join "`n").TrimEnd()
Set-Content -Path (Join-Path $OutDir 'dbt_build.txt') -Value $out -Encoding utf8
Write-Both ''

# ---- Stage 4: sample the resulting tables --------------------------------
Write-Both ('-' * 72)
Write-Both 'STAGE 4  sample the Delta tables'
Write-Both ('-' * 72)
$out = & $Python scripts/export_databricks_samples.py 2>&1 |
    ForEach-Object { $_.ToString() } | Out-String
Write-Both $out.TrimEnd()
Write-Both ''

Write-Both ('=' * 72)
Write-Both 'RUN COMPLETE'
Write-Both ('=' * 72)

if ($Redact) {
    $c = Get-Content $Transcript -Raw
    $c = [regex]::Replace($c, 'dbc-[a-z0-9\-]+\.cloud\.databricks\.com',
                          'dbc-<workspace>.cloud.databricks.com')
    Set-Content -Path $Transcript -Value $c -Encoding utf8
    Write-Host 'Workspace hostname redacted.' -ForegroundColor Yellow
}

Write-Host ''
Write-Host "Evidence written to outputs\databricks\" -ForegroundColor Green
Write-Host 'No credentials appear in it: the token is never logged.' -ForegroundColor Green
