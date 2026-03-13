# n8n Setup (Cloud Trial -> Self-Hosted)

This project supports two orchestration modes:

1. **n8n Cloud trial now**: n8n calls a secure local webhook runner over a tunnel.
2. **Self-hosted n8n later**: n8n runs local shell commands directly (`Execute Command` nodes).

Import templates:

- `orchestration/n8n_workflow_cloud_trial.json`
- `orchestration/n8n_workflow_self_hosted.json`

---

## 1) Cloud Trial Setup (recommended now)

### A. Configure `.env` in repo root

Set these:

- `SUPABASE_URL`
- `SUPABASE_KEY` (service_role key)
- `OPENAI_API_KEY`
- `ORCH_RUN_TOKEN` (long random secret)

### B. Start local webhook runner API

From repo root:

```bash
cd orchestration
uv sync
uv run uvicorn runner_api:app --host 0.0.0.0 --port 8000
```

Health check:

```bash
curl http://localhost:8000/health
```

### C. Expose API publicly (temporary tunnel)

Example with ngrok:

```bash
ngrok http 8000
```

Copy your HTTPS URL, e.g.:
`https://abc123.ngrok.app`

### D. Build n8n Cloud workflow

Use nodes in this order:

1. `Schedule Trigger` (every 6h or daily)
2. `HTTP Request` (POST `https://abc123.ngrok.app/run`) for Layer 1
3. `HTTP Request` (POST `https://abc123.ngrok.app/run`) for Layer 2
4. `HTTP Request` (POST `https://abc123.ngrok.app/run`) for Layer 3
5. `IF` node checks each response `.ok == true`
6. `Slack` node posts success/failure

For each HTTP node:

- Method: `POST`
- Header: `X-Run-Token: <ORCH_RUN_TOKEN>`
- JSON body examples:

Layer 1:

```json
{
  "job": "layer1",
  "mode": "prod"
}
```

Layer 2:

```json
{
  "job": "layer2",
  "mode": "prod",
  "batch_size": 100,
  "drain": true
}
```

Layer 3:

```json
{
  "job": "layer3",
  "mode": "prod",
  "period_days": 7,
  "max_articles": 120,
  "min_relevance": 3
}
```

---

## 2) Self-Hosted n8n Setup (later)

When n8n runs on the same VM/server as this repo:

1. Use `Execute Command` nodes directly.
2. Remove tunnel + webhook runner dependency if preferred.

Commands:

```bash
uv run python layer1/collect.py --mode prod
uv run python layer2/enrich.py --mode prod --batch-size 100 --drain
uv run python layer3/synthesise.py --mode prod --period-days 7 --max-articles 120 --min-relevance 3
```

This is simpler and lower latency than the cloud+tunnel bridge.

---

## Slack Suggestions

Post these fields to Slack from Layer 3 response/log:

- report period
- rows analyzed
- top 3 themes
- `slack_digest`
- local/Drive report link
