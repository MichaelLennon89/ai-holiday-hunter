@echo off
cd /d %~dp0
py -m venv .venv 2>nul
if not exist ".venv\Scripts\python.exe" (
  echo Failed to create virtual environment.
  pause
  exit /b 1
)
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m playwright install
start http://127.0.0.1:8000
.venv\Scripts\python.exe -m uvicorn main:app --reload
pause
