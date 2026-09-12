<#
.SYNOPSIS
    Run the full pipeline and capture the console output as evidence.

.DESCRIPTION
    Executes the four pipeline stages in order and tees everything to
    outputs/run_transcript.txt, which is committed so a reviewer can see the
    real execution without running it themselves.

    Clears watermark state and the warehouse first, so run 1 behaves as a
    genuine first run and run 2 as a genuine incremental run. Without that,
    the watermark evidence is meaningless.

.PARAMETER Source
    'api' (default) reads the live endpoint and needs credentials.
    'csv' reads the bundled fixture and needs none.

.PARAMETER Redact
    Replace the API host in the transcript with a placeholder. Use when the
    repository is public.

.EXAMPLE
    .\scripts\capture_run.ps1
    .\scripts\capture_run.ps1 -Source csv
    .\scripts\capture_run.ps1 -Redact
#>

[CmdletBinding()]
param(
    [ValidateSet('api', 'csv')]
    [string]$Source = 'api',

    [switch]$Redact
)

# Python writes its logs to stderr. With ErrorActionPreference set to 'Stop',
# PowerShell wraps each redirected stderr line in an ErrorRecord and prints a
# NativeCommandError block around it, which makes the transcript unreadable.
# 'Continue' keeps the lines as plain output. Stage failures are still caught,
# because every stage checks $LASTEXITCODE explicitly rather than relying on
# PowerShell's error handling.
$ErrorActionPreference = 'Continue'

# Resolve paths relative to the repository root, not the caller's location.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

# Prefer the virtual environment interpreter; fall back to whatever is on PATH.
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) {
    $Python = 'python'
    Write-Host "No .venv found; using '$Python' from PATH." -ForegroundColor Yellow
}

$OutputsDir = Join-Path $RepoRoot 'outputs'
New-Item -ItemType Directory -Force -Path $OutputsDir | Out-Null
$Transcript = Join-Path $OutputsDir 'run_transcript.txt'

$SourceArgs = @()
if ($Source -eq 'csv') {
    $SourceArgs = @('--source', 'csv', '--csv-path', 'data/transactions.csv')
}

# Stages in dependency order. Stage 2 must follow stage 1 to demonstrate the
# watermark; stage 3 reads what stages 1 and 2 wrote.
$Stages = @(
    @{ Name = 'Task 1  full ingestion';        Module = 'ingestion.ingest_transactions';  Args = $SourceArgs },
    @{ Name = 'Task 3  incremental ingestion'; Module = 'ingestion.incremental_ingest';   Args = $SourceArgs },
    @{ Name = 'Task 2  transform and assert';  Module = 'ingestion.run_transform';        Args = @() },
    @{ Name = 'Export  sample outputs';        Module = 'ingestion.export_outputs';       Args = @() }
)

function Write-Both {
    param([string]$Text)
    Write-Host $Text
    Add-Content -Path $script:Transcript -Value $Text -Encoding utf8
}

# ---- Reset generated state so the run is reproducible from zero ----
Remove-Item (Join-Path $RepoRoot 'state\*.json')  -ErrorAction SilentlyContinue
Remove-Item (Join-Path $RepoRoot 'warehouse\*') -Recurse -ErrorAction SilentlyContinue

Set-Content -Path $Transcript -Value '' -Encoding utf8

Write-Both ('=' * 72)
Write-Both 'PIPELINE RUN TRANSCRIPT'
Write-Both ('=' * 72)
Write-Both ("Captured    : " + (Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'))
Write-Both ("Source      : $Source")
Write-Both ("Python      : " + (& $Python --version 2>&1))
Write-Both ("Platform    : " + [System.Environment]::OSVersion.VersionString)
Write-Both ''
Write-Both 'Watermark state and warehouse cleared before this run, so stage 1 is a'
Write-Both 'genuine first run and stage 2 a genuine incremental run.'
Write-Both ''

# ---- Test suite first: evidence the code is green before it is run ----
Write-Both ('-' * 72)
Write-Both 'TEST SUITE'
Write-Both ('-' * 72)
$testOutput = & $Python -m unittest discover -s tests 2>&1 |
    ForEach-Object { $_.ToString() } | Out-String
Write-Both $testOutput.TrimEnd()
if ($LASTEXITCODE -ne 0) {
    Write-Both ''
    Write-Both 'TESTS FAILED - stopping. Fix before capturing a transcript.'
    exit 1
}
Write-Both ''

# ---- Pipeline stages ----
foreach ($stage in $Stages) {
    Write-Both ('-' * 72)
    Write-Both $stage.Name
    Write-Both ("  python -m " + $stage.Module + ' ' + ($stage.Args -join ' ')).TrimEnd()
    Write-Both ('-' * 72)

    $stageOutput = & $Python -m $stage.Module @($stage.Args) 2>&1 |
        ForEach-Object { $_.ToString() } | Out-String
    Write-Both $stageOutput.TrimEnd()
    Write-Both ''

    if ($LASTEXITCODE -ne 0) {
        Write-Both ("STAGE FAILED with exit code $LASTEXITCODE - stopping.")
        exit $LASTEXITCODE
    }
}

Write-Both ('=' * 72)
Write-Both 'RUN COMPLETE'
Write-Both ('=' * 72)

# ---- Optional redaction for a public repository ----
if ($Redact) {
    $content = Get-Content $Transcript -Raw
    $content = [regex]::Replace(
        $content,
        'https://[a-z0-9]+\.supabase\.co',
        'https://<project>.supabase.co'
    )
    Set-Content -Path $Transcript -Value $content -Encoding utf8
    Write-Host 'API host redacted in transcript.' -ForegroundColor Yellow
}

Write-Host ''
Write-Host "Transcript written to outputs\run_transcript.txt" -ForegroundColor Green
Write-Host 'No credentials appear in it: the pipeline logs key length, never the key.' -ForegroundColor Green
