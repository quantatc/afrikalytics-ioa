"""
Secure orchestration webhook runner for n8n Cloud trial.

Use this when n8n Cloud cannot execute local shell commands directly.
Expose this API with a tunnel (e.g., ngrok/cloudflared) and call from n8n.
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "fastapi>=0.115.0",
#   "uvicorn>=0.30.0",
# ]
# ///

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel


def load_repo_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key:
            os.environ.setdefault(key, value)


load_repo_env()

REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_TOKEN = os.getenv("ORCH_RUN_TOKEN", "")

app = FastAPI(title="IOA Orchestration Runner", version="0.1.0")


class RunRequest(BaseModel):
    job: str
    mode: str = "prod"
    batch_size: int = 100
    drain: bool = True
    period_days: int = 7
    max_articles: int = 120
    min_relevance: int = 3


def _require_token(token: str | None) -> None:
    if not RUN_TOKEN:
        raise HTTPException(status_code=500, detail="ORCH_RUN_TOKEN is not configured on server")
    if token != RUN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid run token")


def _build_command(req: RunRequest) -> list[str]:
    py = sys.executable

    if req.job == "layer1":
        return [py, str(REPO_ROOT / "layer1" / "collect.py"), "--mode", req.mode]

    if req.job == "layer2":
        cmd = [
            py,
            str(REPO_ROOT / "layer2" / "enrich.py"),
            "--mode",
            req.mode,
            "--batch-size",
            str(req.batch_size),
        ]
        if req.drain:
            cmd.append("--drain")
        return cmd

    if req.job == "layer3":
        cmd = [
            py,
            str(REPO_ROOT / "layer3" / "synthesise.py"),
            "--mode",
            req.mode,
            "--period-days",
            str(req.period_days),
            "--max-articles",
            str(req.max_articles),
            "--min-relevance",
            str(req.min_relevance),
        ]
        return cmd

    raise HTTPException(status_code=400, detail=f"Unsupported job '{req.job}'")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/run")
def run_job(req: RunRequest, x_run_token: str | None = Header(default=None)):
    _require_token(x_run_token)
    cmd = _build_command(req)
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60 * 60,
    )

    stdout_tail = (proc.stdout or "")[-8000:]
    stderr_tail = (proc.stderr or "")[-8000:]
    return {
        "ok": proc.returncode == 0,
        "job": req.job,
        "returncode": proc.returncode,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "command": cmd,
    }
