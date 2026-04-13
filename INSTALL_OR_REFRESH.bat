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
  echo Install Python 3 from python.org and tick "Add Python to PATH".
  pause
  exit /b 1
)

echo Using %PY_CMD%
%PY_CMD% bootstrap_start.py --force-setup --no-browser
pause
exit /b %errorlevel%
