# Deploy guide

## Fastest local cloud-like run
```bash
docker compose up --build
```

Then open `http://localhost:8000`.

## Railway
1. Push this repo to GitHub.
2. Create a Railway project from the repo.
3. Add a Redis service.
4. Set `REDIS_URL`, `OPENAI_API_KEY`, and any SMTP vars you want.
5. Deploy the web service with the included `Dockerfile`.
6. Add a second service using the same repo and set the start command to `python workers/worker.py`.

## Render
Use the included `render.yaml` blueprint. Add your env vars after import.
