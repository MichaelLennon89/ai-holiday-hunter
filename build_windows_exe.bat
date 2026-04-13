@echo off
setlocal enabledelayedexpansion
cd /d %~dp0

set "PY_CMD="
where py >nul 2>nul
if not errorlevel 1 set "PY_CMD=py"
if not defined PY_CMD (
  where python >nul 2>nul
  if not errorlevel 1 set "PY_CMD=python"
)
if not defined PY_CMD (
  echo Python 3 was not found on this PC.
  pause
  exit /b 1
)

if not exist .venv (
  %PY_CMD% -m venv .venv
)
call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt pyinstaller
pyinstaller holiday_hunter_desktop.spec --noconfirm

echo Built app in dist\AI Holiday Hunter
pause
