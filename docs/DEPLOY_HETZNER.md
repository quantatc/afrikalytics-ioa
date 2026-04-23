# Deploy In(Sights) Tracker on Hetzner with Docker

This deployment keeps everything portable: one app container and one Postgres container with pgvector. When the app moves to another company server, copy the repo, `.env`, and a database dump.

## 1. Provision the server

Use an Ubuntu LTS Hetzner Cloud server. A small instance is fine for testing; increase CPU/RAM if enrichment jobs and Streamlit will run concurrently.

Open only:

- `22/tcp` for SSH
- `80/tcp` and `443/tcp` if you put Caddy/Nginx in front
- `8501/tcp` only temporarily for direct testing

## 2. Install Docker

```bash
sudo apt update
sudo apt install -y ca-certificates curl git ufw
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker
docker compose version
```

## 3. Copy the project

```bash
sudo mkdir -p /opt/insights-tracker
sudo chown "$USER":"$USER" /opt/insights-tracker
cd /opt/insights-tracker
git clone <your-repo-url> .
```

Create `.env`:

```bash
cp .env.example .env
nano .env
```

Set at least:

```bash
POSTGRES_PASSWORD=<long-random-password>
OPENAI_API_KEY=<your-key>
```

The Docker Compose file sets `DATABASE_URL` internally, so you do not need to set it for the app container unless you use an external Postgres.

## 4. Start the stack

```bash
docker compose up -d --build
docker compose logs -f app
```

Open:

```text
http://<server-ip>:8501
```

For team use, put the app behind Caddy or Nginx with HTTPS rather than leaving port `8501` public.

## 5. Run pipeline jobs

Collection:

```bash
docker compose run --rm app python layer1/collect.py --mode prod
```

Enrichment:

```bash
docker compose run --rm app python layer2/enrich.py --mode prod --batch-size 100
```

Brief generation:

```bash
docker compose run --rm app python layer3/synthesise.py --mode prod --period-days 30 --min-relevance 3
```

## 6. Schedule jobs with host cron

```bash
crontab -e
```

Example:

```cron
0 6,18 * * * cd /opt/insights-tracker && docker compose run --rm app python layer1/collect.py --mode prod >> /var/log/insights-tracker-collect.log 2>&1
30 6,18 * * * cd /opt/insights-tracker && docker compose run --rm app python layer2/enrich.py --mode prod --batch-size 100 >> /var/log/insights-tracker-enrich.log 2>&1
```

## 7. Back up and move later

Create a database dump:

```bash
docker compose exec db pg_dump -U insights_tracker -d insights_tracker -Fc > insights_tracker.dump
```

Restore on another server:

```bash
docker compose up -d db
cat insights_tracker.dump | docker compose exec -T db pg_restore -U insights_tracker -d insights_tracker --clean --if-exists
docker compose up -d --build app
```

Copy these directories if you want generated artifacts too:

- `layer1/logs/`
- `layer2/logs/`
- `layer3/logs/`
- `layer3/reports/`

## 8. Useful maintenance commands

```bash
docker compose ps
docker compose logs -f app
docker compose logs -f db
docker compose pull
docker compose up -d --build
docker system prune
```
