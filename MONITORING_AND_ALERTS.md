# Monitoring and alerts

## Built-in app signals
- `/api/health` for uptime checks
- `/api/jobs` for recent background jobs
- SMTP alerts via `notify_alerts.py`
- Runtime logs in `logs/`

## Recommended production checks
- uptime monitor against `/api/health`
- daily scan automation hitting `/api/live-scan-background`
- email alerts from SMTP settings in `.env`

## Suggested external services
- UptimeRobot or Better Stack for uptime
- Sentry for exceptions
- Railway / Render logs for worker failures
