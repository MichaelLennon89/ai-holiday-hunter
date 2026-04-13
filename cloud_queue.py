from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

QUEUE_KEY = os.getenv("JOB_QUEUE_NAME", "aihh:jobs")
JOB_PREFIX = os.getenv("JOB_PREFIX", "aihh:job:")
RECENT_JOBS_KEY = os.getenv("RECENT_JOBS_KEY", "aihh:recent_jobs")
MAX_RECENT_JOBS = int(os.getenv("MAX_RECENT_JOBS", "100"))


def redis_client():
    if redis is None:
        return None
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    return redis.from_url(url, decode_responses=True)


def redis_enabled() -> bool:
    return redis_client() is not None


def _job_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


def enqueue_job(kind: str, cmd: List[str], cwd: str, total_steps: int = 0) -> Dict[str, Any]:
    r = redis_client()
    if r is None:
        raise RuntimeError("REDIS_URL not configured")
    job_id = uuid.uuid4().hex[:12]
    now = datetime.now().isoformat(timespec="seconds")
    job = {
        "id": job_id,
        "kind": kind,
        "status": "queued",
        "created_at": now,
        "started_at": "",
        "finished_at": "",
        "returncode": "",
        "stdout": "",
        "stderr": "",
        "progress_pct": "0",
        "progress_label": "Queued",
        "status_message": "Waiting to start…",
        "current_step": "0",
        "total_steps": str(total_steps or 0),
        "current_site": "",
        "eta_seconds": "",
        "cwd": cwd,
        "cmd": json.dumps(cmd),
        "cancel_requested": "0",
    }
    r.hset(_job_key(job_id), mapping=job)
    payload = {"job_id": job_id, "kind": kind, "cmd": cmd, "cwd": cwd, "total_steps": total_steps}
    r.lpush(QUEUE_KEY, json.dumps(payload))
    r.lpush(RECENT_JOBS_KEY, job_id)
    r.ltrim(RECENT_JOBS_KEY, 0, MAX_RECENT_JOBS - 1)
    return get_job(job_id) or {"id": job_id}


def update_job(job_id: str, **updates: Any) -> None:
    r = redis_client()
    if r is None:
        return
    mapping = {k: (json.dumps(v) if isinstance(v, (dict, list)) else ("" if v is None else str(v))) for k, v in updates.items()}
    if mapping:
        r.hset(_job_key(job_id), mapping=mapping)


def append_job_output(job_id: str, field: str, text: str, max_len: int = 50000) -> None:
    r = redis_client()
    if r is None:
        return
    key = _job_key(job_id)
    existing = r.hget(key, field) or ""
    new_val = (existing + text)[-max_len:]
    r.hset(key, field, new_val)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    r = redis_client()
    if r is None:
        return None
    data = r.hgetall(_job_key(job_id))
    if not data:
        return None
    def _num(name: str, kind=float):
        raw = data.get(name, "")
        if raw in (None, ""):
            return None
        try:
            return kind(raw)
        except Exception:
            return None
    out: Dict[str, Any] = dict(data)
    out["progress_pct"] = _num("progress_pct", float) or 0.0
    out["current_step"] = _num("current_step", int) or 0
    out["total_steps"] = _num("total_steps", int) or 0
    out["eta_seconds"] = _num("eta_seconds", int)
    out["returncode"] = _num("returncode", int)
    return out


def list_jobs(limit: int = 20) -> List[Dict[str, Any]]:
    r = redis_client()
    if r is None:
        return []
    ids = r.lrange(RECENT_JOBS_KEY, 0, max(0, limit - 1))
    jobs = []
    for jid in ids:
        job = get_job(jid)
        if job:
            jobs.append(job)
    return jobs


def cancel_job(job_id: str) -> Optional[Dict[str, Any]]:
    r = redis_client()
    if r is None:
        return None
    r.hset(_job_key(job_id), mapping={"cancel_requested": "1", "status_message": "Cancellation requested…"})
    return get_job(job_id)
