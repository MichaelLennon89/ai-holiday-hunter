from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cloud_queue import QUEUE_KEY, append_job_output, get_job, redis_client, update_job

PROGRESS_RE = re.compile(r"\[(\d+)\/(\d+)\]\s+(.+?)\s*->")


def _elapsed(job):
    started = job.get("started_at") or job.get("created_at")
    if not started:
        return 0.0
    try:
        dt = datetime.fromisoformat(started)
        return max(0.0, (datetime.now() - dt).total_seconds())
    except Exception:
        return 0.0


def _eta(job):
    current = int(job.get("current_step") or 0)
    total = int(job.get("total_steps") or 0)
    progress = float(job.get("progress_pct") or 0.0)
    elapsed = _elapsed(job)
    if total and current > 0:
        return int(max(0, round((elapsed / max(1, current)) * max(0, total - current))))
    if progress > 0:
        return int(max(0, round(elapsed * ((100.0 - progress) / progress))))
    return None


def _update_from_line(job_id: str, line: str, stream: str = "stdout"):
    line = (line or "").strip()
    if not line:
        return
    append_job_output(job_id, stream, line + "\n")
    updates = {}
    if stream == "stdout":
        updates["progress_label"] = line
        m = PROGRESS_RE.search(line)
        if m:
            current = int(m.group(1)); total = int(m.group(2)); site = m.group(3).strip()
            updates.update({
                "current_step": current,
                "total_steps": total,
                "progress_pct": min(97.0, max(float((get_job(job_id) or {}).get("progress_pct") or 0.0), round((current / total) * 84.0, 1))) if total else 5.0,
                "current_site": site,
                "status_message": f"Searching {site} ({current}/{total})",
            })
        elif "extracted search-journey rows" in line.lower():
            updates.update({"progress_pct": 90.0, "status_message": "Checking room and basket options…"})
        elif "extracted provider-detail rows" in line.lower():
            updates.update({"progress_pct": 94.0, "status_message": "Validating provider detail pages…"})
        elif "saved" in line.lower() and "deals" in line.lower():
            updates.update({"progress_pct": 100.0, "status_message": "Finished and saved results."})
        elif "failed:" in line.lower():
            updates["status_message"] = line
    if updates:
        job = get_job(job_id) or {}
        eta = _eta({**job, **updates})
        if eta is not None:
            updates["eta_seconds"] = eta
        update_job(job_id, **updates)


def _run_job(payload: dict):
    job_id = payload["job_id"]
    cmd = payload["cmd"]
    cwd = payload.get("cwd") or str(ROOT)
    update_job(job_id, status="running", started_at=datetime.now().isoformat(timespec="seconds"), progress_pct=2.0, status_message="Launching search engine…", progress_label="Launching search engine…")
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True)
    try:
        while True:
            if proc.stdout is not None:
                line = proc.stdout.readline()
                if line:
                    _update_from_line(job_id, line, "stdout")
            if proc.stderr is not None:
                err = proc.stderr.readline()
                if err:
                    _update_from_line(job_id, err, "stderr")
            if proc.poll() is not None:
                break
            job = get_job(job_id) or {}
            if str(job.get("cancel_requested") or "0") == "1":
                proc.kill()
                update_job(job_id, status="cancelled", finished_at=datetime.now().isoformat(timespec="seconds"), returncode=-2, status_message="Cancelled by user.")
                return
            time.sleep(0.05)
        # flush any remaining
        if proc.stdout is not None:
            for line in proc.stdout.readlines():
                _update_from_line(job_id, line, "stdout")
        if proc.stderr is not None:
            for line in proc.stderr.readlines():
                _update_from_line(job_id, line, "stderr")
        rc = proc.returncode or 0
        update_job(
            job_id,
            status="completed" if rc == 0 else "failed",
            finished_at=datetime.now().isoformat(timespec="seconds"),
            returncode=rc,
            progress_pct=100.0 if rc == 0 else 99.0,
            status_message="Finished and saved results." if rc == 0 else "Run failed.",
        )
    except Exception as exc:
        proc.kill()
        update_job(job_id, status="failed", finished_at=datetime.now().isoformat(timespec="seconds"), returncode=-1, status_message=f"Failed: {exc}")


def main():
    r = redis_client()
    if r is None:
        raise SystemExit("REDIS_URL not configured; worker cannot start")
    print("Worker listening on", QUEUE_KEY)
    while True:
        item = r.brpop(QUEUE_KEY, timeout=5)
        if not item:
            continue
        _, raw = item
        payload = json.loads(raw)
        _run_job(payload)


if __name__ == "__main__":
    main()
