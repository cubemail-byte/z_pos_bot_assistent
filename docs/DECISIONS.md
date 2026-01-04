# Decisions

## D001 — Raw storage first, parsing later
We store all incoming messages as-is into `messages` (SQLite).
Parsing/classification will write to separate tables (e.g. `parsed_events`) linked by message_id.

Rationale:
- raw data is source of truth
- parsing logic evolves and must be replayable
- avoid mixing inference with ingestion

## D002 — Secrets are not stored in git
Secrets live in `.env` on VPS.
Repo contains `.env.example` for documentation only.
