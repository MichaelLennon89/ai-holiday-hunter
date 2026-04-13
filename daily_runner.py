from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["balanced","cheapest","best_value","low_stress"], default="cheapest")
    parser.add_argument("--limit", type=int, default=18)
    parser.add_argument("--max-flight-queries", type=int, default=12)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--threshold", type=float, default=3000)
    args = parser.parse_args()

    auto_cmd = [sys.executable, str(ROOT / "autopilot.py"), "--mode", args.mode, "--limit", str(args.limit), "--max-flight-queries", str(args.max_flight_queries)]
    if args.headed:
        auto_cmd.append("--headed")
    res1 = run(auto_cmd)
    print(res1.stdout)
    if res1.stderr:
        print(res1.stderr)

    notify_cmd = [sys.executable, str(ROOT / "notify_alerts.py"), "--threshold", str(args.threshold)]
    res2 = run(notify_cmd)
    print(res2.stdout)
    if res2.stderr:
        print(res2.stderr)

    raise SystemExit(0 if (res1.returncode == 0 and res2.returncode == 0) else 1)
