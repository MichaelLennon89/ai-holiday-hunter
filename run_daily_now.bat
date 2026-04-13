@echo off
cd /d %~dp0
if not exist ".venv\Scripts\python.exe" (
  echo Virtual environment missing. Run run_local.bat first.
  pause
  exit /b 1
)
.venv\Scripts\python.exe daily_runner.py --mode cheapest --limit 18 --max-flight-queries 12 --threshold 3000
pause
