from __future__ import annotations

import subprocess
import sys
from pathlib import Path
import os
import webbrowser
import uuid
import threading
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import (
    build_search_plan,
    load_config,
    save_config,
    load_results,
    generate_urls_preview,
    run_demo_scan,
    query_deals,
    package_profile_text,
)
import json
from cloud_queue import redis_enabled, enqueue_job as enqueue_cloud_job, list_jobs as list_cloud_jobs, get_job as get_cloud_job, cancel_job as cancel_cloud_job

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"

app = FastAPI(title="AI Holiday Hunter Product App v21")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
app.mount("/results", StaticFiles(directory=str(ROOT / "results")), name="results")

JOBS: Dict[str, Dict[str, Any]] = {}
JOB_LOCK = threading.Lock()


PROGRESS_RE = re.compile(r"\[(\d+)\/(\d+)\]\s+(.+?)\s*->")

def _job_elapsed_seconds(job: Dict[str, Any]) -> float:
    started = job.get("started_at") or job.get("created_at")
    if not started:
        return 0.0
    try:
        dt = datetime.fromisoformat(started)
        return max(0.0, (datetime.now() - dt).total_seconds())
    except Exception:
        return 0.0

def _job_eta_seconds(job: Dict[str, Any]) -> Optional[int]:
    current = job.get("current_step") or 0
    total = job.get("total_steps") or 0
    progress = float(job.get("progress_pct") or 0.0)
    elapsed = _job_elapsed_seconds(job)
    if total and current > 0:
        per_step = elapsed / max(1, current)
        return int(max(0, round(per_step * max(0, total - current))))
    if progress > 0:
        remaining_ratio = max(0.0, 100.0 - progress) / progress
        return int(max(0, round(elapsed * remaining_ratio)))
    return None

def _update_job_from_line(job_id: str, line: str, stream: str = "stdout") -> None:
    line = (line or "").strip()
    if not line:
        return
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        buf_key = "stdout" if stream == "stdout" else "stderr"
        existing = job.get(buf_key, "")
        job[buf_key] = (existing + line + "\n")[-50000:]
        if stream == "stdout":
            job["progress_label"] = line
            m = PROGRESS_RE.search(line)
            if m:
                current = int(m.group(1))
                total = int(m.group(2))
                current_site = m.group(3).strip()
                job["current_step"] = current
                job["total_steps"] = total
                base = (current / total) * 84.0 if total else 5.0
                job["progress_pct"] = min(97.0, max(job.get("progress_pct", 0.0), round(base, 1)))
                job["current_site"] = current_site
                job["status_message"] = f"Searching {current_site} ({current}/{total})"
            elif "extracted search-journey rows" in line.lower():
                job["progress_pct"] = min(98.0, max(job.get("progress_pct", 0.0), 90.0))
                job["status_message"] = "Checking room and basket options…"
            elif "extracted provider-detail rows" in line.lower():
                job["progress_pct"] = min(98.0, max(job.get("progress_pct", 0.0), 94.0))
                job["status_message"] = "Validating provider detail pages…"
            elif "saved" in line.lower() and "deals" in line.lower():
                job["progress_pct"] = 100.0
                job["status_message"] = "Finished and saved results."
            elif "failed:" in line.lower():
                job["status_message"] = line
        job["eta_seconds"] = _job_eta_seconds(job)


def _job_summary(job: Dict[str, Any]) -> Dict[str, Any]:
    data = {k: v for k, v in job.items() if k not in {"process"}}
    data["elapsed_seconds"] = int(round(_job_elapsed_seconds(job)))
    data["eta_seconds"] = _job_eta_seconds(job)
    return data

def _set_job(job_id: str, **updates: Any) -> None:
    with JOB_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(updates)

def _start_background_job(kind: str, cmd: list[str], cwd: Path) -> Dict[str, Any]:
    job_id = uuid.uuid4().hex[:10]
    total_steps = 0
    if "--limit" in cmd:
        try:
            total_steps = int(cmd[cmd.index("--limit") + 1])
        except Exception:
            total_steps = 0
    job = {
        "id": job_id,
        "kind": kind,
        "status": "queued",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "started_at": None,
        "finished_at": None,
        "returncode": None,
        "stdout": "",
        "stderr": "",
        "cmd": cmd,
        "progress_pct": 0.0,
        "progress_label": "Queued",
        "status_message": "Waiting to start…",
        "current_step": 0,
        "total_steps": total_steps,
        "current_site": None,
        "eta_seconds": None,
        "process": None,
    }
    with JOB_LOCK:
        JOBS[job_id] = job

    def runner():
        proc = None
        try:
            _set_job(job_id, status="running", started_at=datetime.now().isoformat(timespec="seconds"), progress_pct=2.0, status_message="Launching search engine…", progress_label="Launching search engine…")
            proc = subprocess.Popen(
                cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1, universal_newlines=True
            )
            _set_job(job_id, process=proc)

            def _pump(stream_name: str, pipe):
                try:
                    for line in iter(pipe.readline, ""):
                        _update_job_from_line(job_id, line, stream_name)
                finally:
                    try:
                        pipe.close()
                    except Exception:
                        pass

            t_out = threading.Thread(target=_pump, args=("stdout", proc.stdout), daemon=True)
            t_err = threading.Thread(target=_pump, args=("stderr", proc.stderr), daemon=True)
            t_out.start(); t_err.start()
            proc.wait(timeout=2400)
            t_out.join(timeout=2)
            t_err.join(timeout=2)
            _set_job(
                job_id,
                status="completed" if proc.returncode == 0 else "failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                returncode=proc.returncode,
                progress_pct=100.0 if proc.returncode == 0 else min(99.0, float(JOBS.get(job_id, {}).get("progress_pct") or 0.0)),
                status_message="Finished and saved results." if proc.returncode == 0 else (JOBS.get(job_id, {}).get("status_message") or "Run failed."),
                progress_label=JOBS.get(job_id, {}).get("progress_label") or ("Finished" if proc.returncode == 0 else "Failed"),
            )
        except subprocess.TimeoutExpired:
            if proc:
                proc.kill()
            _set_job(job_id, status="timeout", finished_at=datetime.now().isoformat(timespec="seconds"), returncode=-9, status_message="Timed out after 40 minutes.")
        except Exception as exc:
            _set_job(job_id, status="failed", finished_at=datetime.now().isoformat(timespec="seconds"), returncode=-1, stderr=str(exc), status_message=f"Failed: {exc}")

    threading.Thread(target=runner, daemon=True).start()
    return _job_summary(job)


class ScanRequest(BaseModel):
    mode: str = "cheapest"
    limit: int = 120
    max_flight_queries: int = 40
    headed: bool = False


class NotifyRequest(BaseModel):
    threshold: float = 3000




class ManualTakeoverRequest(BaseModel):
    source_url: str = ""
    evidence_html: str = ""
    screenshot_file: str = ""
    session_id: str = ""
    hotel_name: str = ""


class QueryRequest(BaseModel):
    query: str = ""
    max_price: Optional[float] = None
    min_temp: Optional[float] = None
    beach_max_minutes: Optional[int] = None
    require_pool: bool = False
    breakfast_or_better: bool = False
    alerts_only: bool = False
    free_child_only: bool = False
    family_room_only: bool = False
    source_site: str = ""
    sort_by: str = "best"


@app.get("/")
def index():
    return FileResponse(str(STATIC / "index.html"))


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/config")
def get_config():
    cfg = load_config()
    cfg["package_profile_text"] = package_profile_text(cfg)
    cfg["search_plan"] = build_search_plan(cfg)
    return cfg


@app.post("/api/config")
def post_config(payload: Dict[str, Any]):
    save_config(payload)
    cfg = load_config()
    return {
        "saved": True,
        "package_profile_text": package_profile_text(cfg),
        "search_plan": build_search_plan(cfg),
        "config": cfg,
    }


@app.get("/api/search-plan")
def get_search_plan():
    cfg = load_config()
    return build_search_plan(cfg)


@app.get("/api/url-preview")
def url_preview(max_flight_queries: int = 12):
    cfg = load_config()
    return {
        "rows": generate_urls_preview(max_flight_queries=max_flight_queries),
        "package_profile": package_profile_text(cfg),
        "search_plan": build_search_plan(cfg),
    }


@app.get("/api/results")
def get_results():
    return {"rows": [d.to_dict() for d in load_results()]}


@app.post("/api/query-deals")
def query_results(req: QueryRequest):
    cfg = load_config()
    rows = query_deals(load_results(), cfg=cfg, **req.model_dump())
    return {"rows": [d.to_dict() for d in rows], "count": len(rows)}


@app.post("/api/demo-scan")
def demo_scan(req: ScanRequest):
    deals = run_demo_scan(mode=req.mode)
    return {"ok": True, "count": len(deals), "rows": [d.to_dict() for d in deals]}


@app.get("/api/best-today")
def best_today():
    path = ROOT / "results" / "best_today.json"
    if not path.exists():
        return {"ok": False, "best": None}
    return {"ok": True, **json.loads(path.read_text(encoding="utf-8"))}



@app.get("/api/recommendations")
def recommendations():
    cfg = load_config()
    prefs = cfg.get("search_preferences", {})
    strategy = cfg.get("strategy", {})
    rows = query_deals(
        load_results(),
        cfg=cfg,
        max_price=float(strategy.get("buy_line_gbp", 3000) or 3000),
        min_temp=float(prefs.get("min_temp_c", 26) or 26),
        beach_max_minutes=int(prefs.get("target_beach_minutes", 10) or 10),
        sort_by="best",
    )
    best_now = [d.to_dict() for d in rows if d.recommendation_bucket in {"best-now", "exact-fit"}][:6]
    worth = [d.to_dict() for d in rows if d.recommendation_bucket in {"worth-a-look", "near-miss", "stretch"}][:6]
    return {"best_now": best_now, "worth_looking_at": worth}

@app.get("/api/destinations")
def destinations():
    cfg = load_config()
    return {"rows": cfg.get("destinations", []), "search_plan": build_search_plan(cfg)}





@app.get("/api/evidence-index")
def evidence_index():
    path = ROOT / "results" / "evidence_index.json"
    if not path.exists():
        return {"generated_at": None, "count": 0, "items": []}
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/api/provider-scorecard")
def provider_scorecard():
    path = ROOT / "results" / "provider_scorecard.json"
    if not path.exists():
        return {"rows": []}
    return {"rows": json.loads(path.read_text(encoding="utf-8"))}



@app.get("/api/assisted-resume-manifest")
def assisted_resume_manifest():
    path = ROOT / "results" / "assisted_resume_manifest.json"
    if not path.exists():
        return {"rows": []}
    try:
        return {"rows": json.loads(path.read_text(encoding="utf-8"))}
    except Exception:
        return {"rows": []}


@app.post("/api/manual-takeover/open")
def manual_takeover_open(req: ManualTakeoverRequest):
    opened = []
    if req.source_url:
        try:
            webbrowser.open(req.source_url)
            opened.append(req.source_url)
        except Exception:
            pass
    for name in [req.evidence_html, req.screenshot_file]:
        if not name:
            continue
        path = ROOT / "results" / "evidence" / name
        if path.exists():
            try:
                webbrowser.open(path.resolve().as_uri())
                opened.append(path.name)
            except Exception:
                pass
    log_path = ROOT / "results" / "manual_takeover_log.jsonl"
    row = {
        "timestamp": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
        "session_id": req.session_id,
        "hotel_name": req.hotel_name,
        "source_url": req.source_url,
        "evidence_html": req.evidence_html,
        "screenshot_file": req.screenshot_file,
        "opened": opened,
    }
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return {"ok": True, "opened": opened, "count": len(opened)}

@app.get("/api/dashboard")
def dashboard():
    cfg = load_config()
    rows = load_results()
    ranked = query_deals(rows, cfg=cfg, sort_by="best")
    best_now = [d.to_dict() for d in ranked[:6]]
    worth = [d.to_dict() for d in ranked if (d.recommendation_bucket or '') in {"worth-a-look", "near-miss", "stretch"}][:6]
    stats = {
        "total_results": len(rows),
        "exact_fit": sum(1 for d in rows if (d.fit_label or '') == 'Exact fit'),
        "near_miss": sum(1 for d in rows if 'Near' in (d.fit_label or '')),
        "prepayment": sum(1 for d in rows if (d.pricing_truth_label or '') == 'Pre-payment price'),
        "basket_like": sum(1 for d in rows if (d.pricing_truth_label or '') == 'Basket-like price'),
        "drop_alerts": sum(1 for d in rows if (d.deal_signal or '') == 'DROP ALERT'),
    }
    outputs = {}
    for name in [
        'elite_search_summary.md', 'provider_tuning_report.md', 'truth_ranked_shortlist.md',
        'assisted_review_queue.md', 'best_today.md', 'worth_looking_at.md', 'basket_audit.md',
        'provider_scorecard.md', 'market_overview.md', 'historical_pricing_report.md',
        'hotel_clusters.md', 'operator_briefing_pack.md', 'assisted_resume_manifest.md'
    ]:
        path = ROOT / 'results' / name
        if path.exists():
            outputs[name] = path.read_text(encoding='utf-8', errors='ignore')[:12000]
    return {
        'stats': stats,
        'best_now': best_now,
        'worth_looking_at': worth,
        'provider_plan': build_search_plan(cfg).get('providers', []),
        'outputs': outputs,
        'evidence_count': len(json.loads((ROOT / 'results' / 'evidence_index.json').read_text(encoding='utf-8')).get('items', [])) if (ROOT / 'results' / 'evidence_index.json').exists() else 0,
        'jobs': list_jobs().get('rows', [])[:8],
    }


@app.get("/api/jobs")
def list_jobs():
    if redis_enabled():
        rows = list_cloud_jobs(limit=20)
        rows.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return {"rows": rows}
    with JOB_LOCK:
        rows = [_job_summary(v) for v in JOBS.values()]
    rows.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return {"rows": rows[:20]}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    if redis_enabled():
        job = get_cloud_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_summary(job)


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    if redis_enabled():
        job = cancel_cloud_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"ok": True, "job": job}
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    proc = job.get("process")
    if proc and job.get("status") == "running":
        proc.kill()
        _set_job(job_id, status="cancelled", finished_at=datetime.now().isoformat(timespec="seconds"), returncode=-2)
    return {"ok": True, "job": _job_summary(JOBS[job_id])}


@app.post("/api/live-scan-background")
def live_scan_background(req: ScanRequest):
    cmd = [sys.executable, str(ROOT / "browser_scan.py"), "--mode", req.mode, "--limit", str(req.limit), "--max-flight-queries", str(req.max_flight_queries)]
    if not req.headed:
        cmd.append("--headless")
    total_steps = int(req.limit)
    if redis_enabled():
        return {"ok": True, "job": enqueue_cloud_job("live-scan", cmd, str(ROOT), total_steps=total_steps)}
    return {"ok": True, "job": _start_background_job("live-scan", cmd, ROOT)}


@app.post("/api/autopilot-background")
def autopilot_background(req: ScanRequest):
    cmd = [sys.executable, str(ROOT / "autopilot.py"), "--mode", req.mode, "--limit", str(req.limit), "--max-flight-queries", str(req.max_flight_queries)]
    if req.headed:
        cmd.append("--headed")
    total_steps = int(req.limit)
    if redis_enabled():
        return {"ok": True, "job": enqueue_cloud_job("autopilot", cmd, str(ROOT), total_steps=total_steps)}
    return {"ok": True, "job": _start_background_job("autopilot", cmd, ROOT)}


@app.post("/api/notify-alerts-background")
def notify_alerts_background(req: NotifyRequest):
    cmd = [sys.executable, str(ROOT / "notify_alerts.py"), "--threshold", str(req.threshold)]
    if redis_enabled():
        return {"ok": True, "job": enqueue_cloud_job("notify-alerts", cmd, str(ROOT), total_steps=1)}
    return {"ok": True, "job": _start_background_job("notify-alerts", cmd, ROOT)}


def _run_script(script_name: str, req: ScanRequest):
    cmd = [
        sys.executable, str(ROOT / script_name),
        "--mode", req.mode,
        "--limit", str(req.limit),
        "--max-flight-queries", str(req.max_flight_queries),
    ]
    if script_name == "browser_scan.py" and not req.headed:
        cmd.append("--headless")
    if script_name == "autopilot.py" and req.headed:
        cmd.append("--headed")
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=2400)
    return {
        "ok": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
        "rows": [d.to_dict() for d in load_results()],
    }


@app.post("/api/live-scan")
def live_scan(req: ScanRequest):
    return JSONResponse(_run_script("browser_scan.py", req))


@app.post("/api/autopilot")
def autopilot(req: ScanRequest):
    return JSONResponse(_run_script("autopilot.py", req))


@app.post("/api/notify-alerts")
def notify_alerts(req: NotifyRequest):
    cmd = [sys.executable, str(ROOT / "notify_alerts.py"), "--threshold", str(req.threshold)]
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=300)
    return JSONResponse({
        "ok": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    })
