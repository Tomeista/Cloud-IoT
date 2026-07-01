<#
.SYNOPSIS
    Startet die IoT Sensor Monitoring Entwicklungsumgebung (ohne Docker/Kafka).
.DESCRIPTION
    Erstellt/aktiviert eine lokale venv, installiert Dependencies,
    startet Backend (FastAPI) und Frontend (HTTP-Server) parallel.
    Beendet beide mit Ctrl+C.
#>

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvDir = Join-Path $ProjectRoot ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"

Write-Host ""
Write-Host "  IoT Sensor Monitoring - Dev Server" -ForegroundColor Cyan
Write-Host "  ===================================" -ForegroundColor Cyan
Write-Host ""

# --- Check Python ---
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "  [ERROR] Python nicht gefunden. Bitte Python 3.12+ installieren." -ForegroundColor Red
    exit 1
}

# --- Create / activate venv ---
if (-not (Test-Path $VenvPython)) {
    Write-Host "  [1/4] Erstelle lokale venv in .venv/ ..." -ForegroundColor Yellow
    python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [ERROR] venv konnte nicht erstellt werden." -ForegroundColor Red
        exit 1
    }
    Write-Host "         .venv erstellt." -ForegroundColor Green
} else {
    Write-Host "  [1/4] venv gefunden (.venv/)" -ForegroundColor Green
}

# --- Install dependencies in venv ---
Write-Host "  [2/3] Pruefe Backend-Abhaengigkeiten in venv..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
& $VenvPip install -q -r "$ProjectRoot\backend\requirements.txt" 2>&1 | Out-Null
$pipExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($pipExit -ne 0) {
    Write-Host "  [WARN] pip install fehlgeschlagen - manuell ausfuehren:" -ForegroundColor Yellow
    Write-Host "         .venv\Scripts\pip install -r backend\requirements.txt" -ForegroundColor Gray
}

# --- Start Backend (serves API + Frontend static files on one port) ---
Write-Host "  [3/3] Starte Server (API + Frontend) auf Port 8000..." -ForegroundColor Yellow
$backend = Start-Process -PassThru -NoNewWindow -FilePath $VenvPython -ArgumentList "-m", "uvicorn", "backend.app.main:app", "--host", "127.0.0.1", "--port", "8000", "--reload", "--reload-exclude", ".venv" -WorkingDirectory $ProjectRoot

Write-Host ""
Write-Host "  Bereit!" -ForegroundColor Green
Write-Host ""
Write-Host "  App:       http://localhost:8000" -ForegroundColor White
Write-Host "  API Docs:  http://localhost:8000/docs" -ForegroundColor White
Write-Host ""
Write-Host "  Stoppen:   Ctrl+C hier, oder:  .\stop-dev.ps1" -ForegroundColor Gray
Write-Host ""

# --- Wait and cleanup on Ctrl+C ---
try {
    while (-not $backend.HasExited) {
        Start-Sleep -Seconds 2
    }
    Write-Host "  [WARN] Server gestoppt (Exit: $($backend.ExitCode))" -ForegroundColor Yellow
}
finally {
    Write-Host ""
    Write-Host "  Stoppe Server..." -ForegroundColor Yellow
    if (-not $backend.HasExited) { Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue }
    # Clean up any orphaned uvicorn processes
    Get-Process -Name python -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*uvicorn*backend*' } |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "  Beendet." -ForegroundColor Green
}
