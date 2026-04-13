FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends     curl ca-certificates gnupg unzip wget     libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2     libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2     libgbm1 libasound2 libpangocairo-1.0-0 libgtk-3-0     libpango-1.0-0 libx11-6 libxcb1 libxext6 libxshmfence1     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN python -m playwright install chromium

COPY . .
RUN mkdir -p results/history results/evidence results/sessions results/tuning results/baskets logs

EXPOSE 8000
CMD ["bash", "-lc", "gunicorn -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2} -b 0.0.0.0:${PORT:-8000} main:app"]
