param(
    [string]$ProjectPath = ".",
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 8000,
    [string]$ApiKey = "",
    [string]$Model = "gpt-4.1-mini",
    [switch]$SkipPlaywrightInstall,
    [switch]$SkipDependencyInstall,
    [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"
function Write-Step { param([string]$Message) Write-Host ""; Write-Host "==> $Message" -ForegroundColor Cyan }
function Get-PythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) { return "py" }
    elseif (Get-Command python -ErrorAction SilentlyContinue) { return "python" }
    else { throw "Python was not found. Install Python 3.10+ and try again." }
}
$ProjectPath = (Resolve-Path $ProjectPath).Path
Set-Location $ProjectPath
$PythonCmd = Get-PythonCommand
$VenvPath = Join-Path $ProjectPath ".venv"
$VenvPython = Join-Path $VenvPath "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) { Write-Step "Creating virtual environment"; & $PythonCmd -m venv .venv }
if (-not $SkipDependencyInstall) { Write-Step "Installing dependencies"; & $VenvPython -m pip install --upgrade pip; & $VenvPython -m pip install -r requirements.txt }
if (-not $SkipPlaywrightInstall) { Write-Step "Installing Playwright browsers"; & $VenvPython -m playwright install }
if ($ApiKey -ne "") { $env:OPENAI_API_KEY = $ApiKey; $env:HOLIDAY_AI_MODEL = $Model; Write-Step "Using OpenAI API extraction in this session" } else { Write-Step "No OpenAI API key set - rule-based extraction only" }
$Url = "http://$HostAddress`:$Port"
if ($OpenBrowser) { Start-Process $Url }
Write-Step "Starting app"
Write-Host "App URL: $Url" -ForegroundColor Green
& $VenvPython -m uvicorn main:app --host $HostAddress --port $Port --reload
