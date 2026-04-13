#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python -m venv .venv || true
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install
python -m webbrowser "http://127.0.0.1:8000" || true
.venv/bin/python -m uvicorn main:app --reload
