from __future__ import annotations

import argparse
import csv
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"

def build_summary(threshold: float) -> str:
    alerts_csv = RESULTS / "price_drop_alerts.csv"
    rec_md = RESULTS / "autopilot_recommendations.md"
    best_today = RESULTS / "best_today.json"
    lines = ["AI Holiday Hunter alert summary", "", f"Booking threshold used: £{threshold:,.0f}", ""]
    if best_today.exists():
        import json
        payload = json.loads(best_today.read_text(encoding="utf-8"))
        best = payload.get("best") or {}
        if best:
            lines.append("Best deal today")
            lines.append(f"- {best.get('hotel_name') or best.get('destination') or 'Unnamed option'}")
            lines.append(f"- Action: {best.get('action_now') or 'Watch'}")
            lines.append(f"- Scenario price: £{best.get('scenario_price_gbp') if best.get('scenario_price_gbp') is not None else 'Unknown'}")
            lines.append(f"- Signal: {best.get('deal_signal') or 'None'}")
            lines.append("")
    if alerts_csv.exists():
        with alerts_csv.open("r", encoding="utf-8", newline="") as f:
            reader = list(csv.DictReader(f))
        shortlisted = [r for r in reader if r.get("price_total_gbp") and float(r["price_total_gbp"]) <= threshold]
        lines.append(f"DROP ALERT rows: {len(reader)}")
        lines.append(f"Below threshold: {len(shortlisted)}")
        lines.append("")
        for i, row in enumerate(shortlisted[:10], 1):
            lines.append(f"{i}. {row.get('hotel_name','Unknown')} | {row.get('destination','')} | £{row.get('price_total_gbp','')} | Δ £{row.get('price_delta_gbp','')}")
    else:
        lines.append("No price_drop_alerts.csv found yet.")
    lines.append("")
    lines.append("autopilot_recommendations.md is available in the results folder." if rec_md.exists() else "No autopilot_recommendations.md found yet.")
    if best_today.exists():
        lines.append("best_today.md is available in the results folder.")
    return "\n".join(lines)

def write_local_summary(summary: str) -> Path:
    out = RESULTS / "last_alert_summary.txt"
    out.write_text(summary, encoding="utf-8")
    return out

def send_email(summary: str) -> tuple[bool, str]:
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    sender = os.getenv("ALERT_FROM", username).strip()
    recipient = os.getenv("ALERT_TO", "").strip()
    use_tls = os.getenv("SMTP_STARTTLS", "true").strip().lower() in {"1","true","yes","y"}
    if not (host and username and password and sender and recipient):
        return False, "SMTP settings not configured. Wrote local summary only."
    msg = EmailMessage()
    msg["Subject"] = "AI Holiday Hunter alerts"
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(summary)
    with smtplib.SMTP(host, port, timeout=30) as server:
        if use_tls:
            server.starttls()
        server.login(username, password)
        server.send_message(msg)
    return True, f"Email sent to {recipient}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=3000)
    args = parser.parse_args()
    summary = build_summary(args.threshold)
    out = write_local_summary(summary)
    ok, msg = send_email(summary)
    print(summary)
    print("")
    print(f"Saved local summary to: {out}")
    print(msg)
