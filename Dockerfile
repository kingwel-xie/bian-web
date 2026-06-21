FROM python:3.12-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    LEADERBOARD_DISCOVERY=node

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        fonts-noto-cjk \
        fonts-noto-color-emoji \
        nodejs \
        npm \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt package.json ./

RUN python -m pip install --no-cache-dir -r requirements.txt \
    && npm install --omit=dev \
    && npx playwright install --with-deps chromium \
    && npm cache clean --force

COPY auto_leaderboard.py fit_market_volume.py workflow.py sum_har_leaderboard.py app.py export_leaderboards_xlsx.py send_exports_email.py docker-entrypoint.sh ./
COPY web ./web

RUN chmod +x /app/docker-entrypoint.sh \
    && mkdir -p /data

VOLUME ["/data"]
EXPOSE 48234

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["serve"]
