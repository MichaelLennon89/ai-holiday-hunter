from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run_autopilot(mode: str = "cheapest", limit: int = 18, max_flight_queries: int = 12, headed: bool = False):
    cmd = [sys.executable, str(ROOT / "browser_scan.py"), "--mode", mode, "--limit", str(limit), "--max-flight-queries", str(max_flight_queries)]
    if not headed:
        cmd.append("--headless")
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["balanced", "cheapest", "best_value", "low_stress"], default="cheapest")
    p.add_argument("--limit", type=int, default=18)
    p.add_argument("--max-flight-queries", type=int, default=12)
    p.add_argument("--headed", action="store_true")
    a = p.parse_args()
    code, out, err = run_autopilot(a.mode, a.limit, a.max_flight_queries, a.headed)
    print(out)
    if err:
        print(err)
    raise SystemExit(code)
