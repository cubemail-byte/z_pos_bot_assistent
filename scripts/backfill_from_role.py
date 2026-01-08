from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

import yaml


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def build_user_role_index(cfg: dict[str, Any]) -> dict[int, str]:
    roles: dict[int, str] = {}
    for item in cfg.get("users", []) or []:
        if not isinstance(item, dict):
            continue
        user_id = item.get("user_id")
        role = item.get("role")
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            continue
        if role:
            roles[uid] = str(role)
    return roles


def resolve_db_path(cfg: dict[str, Any], project_root: Path) -> Path:
    sqlite_path_cfg = cfg.get("storage", {}).get("sqlite_path", "data/agent.db")
    path = Path(sqlite_path_cfg)
    return path if path.is_absolute() else project_root / path


def ensure_message_column(con: sqlite3.Connection, column: str, column_type: str) -> None:
    cur = con.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in cur.fetchall()}
    if cols and column not in cols:
        con.execute(f"ALTER TABLE messages ADD COLUMN {column} {column_type}")


def backfill_from_role(db_path: Path, roles: dict[int, str], dry_run: bool) -> int:
    if not roles:
        print("No roles found in config.yaml; nothing to backfill.")
        return 0

    updated = 0
    with sqlite3.connect(str(db_path)) as con:
        ensure_message_column(con, "from_role", "TEXT")
        for user_id, role in roles.items():
            if dry_run:
                cur = con.execute(
                    "SELECT COUNT(*) FROM messages WHERE from_id = ? AND (from_role IS NULL OR from_role = '')",
                    (user_id,),
                )
                count = int(cur.fetchone()[0])
                if count:
                    print(f"[dry-run] user_id={user_id} role={role} rows={count}")
            else:
                cur = con.execute(
                    """
                    UPDATE messages
                    SET from_role = ?
                    WHERE from_id = ? AND (from_role IS NULL OR from_role = '')
                    """,
                    (role, user_id),
                )
                updated += cur.rowcount
        if not dry_run:
            con.commit()
    if not dry_run:
        print(f"Updated rows: {updated}")
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill messages.from_role from config.yaml users")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--db", default=None, help="Override sqlite DB path")
    parser.add_argument("--dry-run", action="store_true", help="Only show counts, do not update")
    args = parser.parse_args()

    config_path = Path(args.config)
    project_root = config_path.resolve().parent
    cfg = load_config(config_path)
    roles = build_user_role_index(cfg)

    db_path = Path(args.db) if args.db else resolve_db_path(cfg, project_root)
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 2

    backfill_from_role(db_path, roles, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
