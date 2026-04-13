param(
    [string]$ProjectPath = ".",
    [string]$TaskName = "AI Holiday Hunter Daily",
    [string]$Time = "09:00",
    [string]$Mode = "cheapest",
    [int]$Limit = 18,
    [int]$Queries = 12,
    [int]$Threshold = 3000
)

$ErrorActionPreference = "Stop"
$ProjectPath = (Resolve-Path $ProjectPath).Path
$PythonExe = Join-Path $ProjectPath ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) { throw "Virtual environment Python not found at $PythonExe. Run the app once first." }
$Script = Join-Path $ProjectPath "daily_runner.py"
if (-not (Test-Path $Script)) { throw "daily_runner.py not found." }

$Action = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$Script`" --mode $Mode --limit $Limit --max-flight-queries $Queries --threshold $Threshold"
$Trigger = New-ScheduledTaskTrigger -Daily -At $Time
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Description "Runs AI Holiday Hunter daily autopilot plus alerts" -Force
Write-Host "Scheduled task created: $TaskName at $Time"
