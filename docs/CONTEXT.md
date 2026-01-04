# tg-agent — CONTEXT

## What this project is
Telegram bot for collecting messages from a group chat about POS/terminal incidents.
Goal: silently ingest all messages into DB (raw), then parse/classify later.

## Current state (as of YYYY-MM-DD)
- Bot runs on VPS (Ubuntu 24.04) under systemd service `tg-agent`.
- Deployment: pull from GitHub + install deps + restart via `/usr/local/bin/tg-agent-deploy`.
- Config:
  - `config.yaml` in repo (non-secret settings)
  - `.env` on VPS only (secrets like BOT_TOKEN)
- Storage:
  - SQLite DB: `/opt/tg-agent/data/agent.db`
  - Table: `messages` (raw ingestion only)

## Repo layout
- `app/bot.py` — aiogram bot entrypoint
- `app/storage.py` — sqlite helpers (init_db, save_message)
- `config.yaml` — settings (no secrets)
- `requirements.txt`

## Non-goals (for now)
- No parsing / LLM / classification in ingestion step
- No replying in groups (silent mode)

## Next milestones
1) Silent mode hardening (commands only in private or admin-only)
2) Admin commands: /stats, /last, /export
3) Parsing pipeline: messages -> parsed_events (separate table)
4) Threading / SLA / “was there a response?” logic
