import sqlite3
from typing import Any, Mapping

from pathlib import Path
from typing import Optional
from entities_engine import extract_entities

from functools import lru_cache
import yaml


DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    chat_type TEXT,
    from_id INTEGER,
    username TEXT,
    text TEXT
);
"""

DDL_MESSAGE_ENTITIES = """
CREATE TABLE IF NOT EXISTS message_entities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL,
  entity_type TEXT NOT NULL,
  entity_value TEXT NOT NULL,
  entity_raw TEXT,
  confidence REAL NOT NULL DEFAULT 0.5,
  extractor TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  UNIQUE(message_id, entity_type, entity_value),
  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_message_entities_message_id ON message_entities(message_id);
CREATE INDEX IF NOT EXISTS idx_message_entities_type_value ON message_entities(entity_type, entity_value);
"""

BASE_DIR = Path(__file__).resolve().parent
ENRICHMENT_CFG_PATH = BASE_DIR / "config" / "enrichment.yaml"

@lru_cache(maxsize=1)
def get_enrichment_cfg() -> dict:
    if not ENRICHMENT_CFG_PATH.exists():
        # безопасный дефолт: enrichment выключен
        return {"terminal_directory": {"enabled": False}}
    with open(ENRICHMENT_CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _ensure_message_column(con: sqlite3.Connection, column: str, column_type: str) -> None:
    cur = con.execute("PRAGMA table_info(messages)")
    cols = {row[1] for row in cur.fetchall()}
    if not cols:
        return
    if column not in cols:
        con.execute(f"ALTER TABLE messages ADD COLUMN {column} {column_type}")


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.executescript(DDL_MESSAGE_ENTITIES)
        _ensure_message_column(con, "from_role", "TEXT")
        _ensure_message_column(con, "reply_kind", "TEXT")
        con.commit()


def save_message(
    db_path: str,
    ts_utc: str,
    chat_id: int,
    chat_type: Optional[str],
    from_id: Optional[int],
    username: Optional[str],
    text: Optional[str],
    from_role: Optional[str] = None,
    reply_kind: Optional[str] = None,
) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO messages(ts_utc, chat_id, chat_type, from_id, username, text, from_role, reply_kind) VALUES(?,?,?,?,?,?,?,?)",
            (ts_utc, chat_id, chat_type, from_id, username, text, from_role, reply_kind),
        )
        con.commit()

def save_message_raw(db_path: str, m: Mapping[str, Any]) -> None:
    """
    Сохраняет raw-сообщение в messages, заполняя расширенные колонки.
    Все поля опциональны (кроме ts_utc и chat_id) — при отсутствии пишем NULL.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            INSERT INTO messages(
              ts_utc, chat_id, chat_type, from_id, username, text,
              chat_alias,
              tg_message_id, reply_to_tg_message_id,
              reply_to_from_id, reply_to_username,
              from_display, from_role,
              reply_kind,
              forward_from_id, forward_from_name,
              content_type, has_media, service_action,
              edited_ts_utc,
              raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?,
                    ?,
                    ?, ?,
                    ?, ?,
                    ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?,
                    ?)
            """,
            (
                m.get("ts_utc"),
                m.get("chat_id"),
                m.get("chat_type"),
                m.get("from_id"),
                m.get("username"),
                m.get("text"),

                m.get("chat_alias"),

                m.get("tg_message_id"),
                m.get("reply_to_tg_message_id"),

                m.get("reply_to_from_id"),
                m.get("reply_to_username"),

                m.get("from_display"),
                m.get("from_role"),
                m.get("reply_kind"),

                m.get("forward_from_id"),
                m.get("forward_from_name"),

                m.get("content_type"),
                m.get("has_media"),
                m.get("service_action"),

                m.get("edited_ts_utc"),

                m.get("raw_json"),
            ),
        )
        con.commit()

def lookup_terminal_directory(con: sqlite3.Connection, azs: str, plnum: str):
    return con.execute(
        """
        SELECT tid, ip, arm
        FROM terminal_directory
        WHERE azs = ? AND plnum = ?
        """,
        (azs, plnum),
    ).fetchall()


def ingest_raw_and_classify(
    db_path: str,
    m: Mapping[str, Any],
    match: Optional[dict],
    ruleset_version: str,
) -> int:
    """
    Сохраняет raw-сообщение и сразу пытается его классифицировать.
    Если классификация не удалась — сообщение остаётся UNCLASSIFIED.
    """
    with sqlite3.connect(db_path) as con:
        cur = con.execute(
            """
            INSERT INTO messages(
              ts_utc, chat_id, chat_type, from_id, username, text,
              chat_alias,
              tg_message_id, reply_to_tg_message_id,
              reply_to_from_id, reply_to_username,
              from_display, from_role,
              reply_kind,
              forward_from_id, forward_from_name,
              content_type, has_media, service_action,
              edited_ts_utc,
              raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?,
                    ?,
                    ?, ?,
                    ?, ?,
                    ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?,
                    ?)
            """,
            (
                m.get("ts_utc"),
                m.get("chat_id"),
                m.get("chat_type"),
                m.get("from_id"),
                m.get("username"),
                m.get("text"),
                m.get("chat_alias"),
                m.get("tg_message_id"),
                m.get("reply_to_tg_message_id"),
                m.get("reply_to_from_id"),
                m.get("reply_to_username"),
                m.get("from_display"),
                m.get("from_role"),
                m.get("reply_kind"),
                m.get("forward_from_id"),
                m.get("forward_from_name"),
                m.get("content_type"),
                m.get("has_media"),
                m.get("service_action"),
                m.get("edited_ts_utc"),
                m.get("raw_json"),
            ),
        )
        message_id = cur.lastrowid

        # 1) гарантируем строку классификации
        con.execute(
            """
            INSERT INTO message_classification (message_id, chat_id, tg_message_id)
            VALUES (?, ?, ?)
            ON CONFLICT(message_id) DO NOTHING
            """,
            (message_id, m.get("chat_id"), m.get("tg_message_id")),
        )

        # 2) если есть результат классификации — обновляем
        if match:
            con.execute(
                """
                UPDATE message_classification
                SET
                  problem_domain = 'PROBLEM',
                  problem_symptom = ?,
                  rule_id = ?,
                  confidence = ?,
                  ruleset_version = ?,
                  is_unclassified = 0,
                  classified_at_utc = ?,
                  updated_at_utc = ?
                WHERE message_id = ?
                """,
                (
                    match.get("code"),
                    match.get("rule_id"),
                    float(match.get("weight", 0.0)),
                    ruleset_version,
                    m.get("ts_utc"),
                    m.get("ts_utc"),
                    message_id,
                ),
            )

        # 3) извлекаем КЕ / реквизиты (best-effort) и пишем в message_entities
        text = (m.get("text") or "").strip()
        if text:
            entities = extract_entities(text)
            for e in entities:
                con.execute(
                    """
                    INSERT OR IGNORE INTO message_entities(
                      message_id, entity_type, entity_value, entity_raw, confidence, extractor, created_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_id,
                        e.entity_type,
                        e.entity_value,
                        e.entity_raw,
                        float(e.confidence),
                        e.extractor,
                        m.get("ts_utc"),
                    ),
                )

        # 4) enrichment из terminal_directory (best-effort):
        # если есть azs+workplace -> ищем tid/ip в справочнике и пишем как сущности
        cfg = get_enrichment_cfg().get("terminal_directory", {}) or {}
        if cfg.get("enabled", True):
            require_unique = bool(cfg.get("require_unique_match", True))
            write_tid = bool(cfg.get("write_tid", True))
            write_ip = bool(cfg.get("write_ip", True))

            conf = cfg.get("confidence", {}) or {}
            tid_conf = float(conf.get("tid", 0.95))
            ip_conf = float(conf.get("ip", 0.8))

            ent = con.execute(
                """
                SELECT entity_type, entity_value
                FROM message_entities
                WHERE message_id = ?
                """,
                (message_id,),
            ).fetchall()

            azs_val = next((v for (t, v) in ent if t == "azs"), None)
            wp_val = next((v for (t, v) in ent if t == "workplace"), None)

            if azs_val and wp_val:
                rows = lookup_terminal_directory(con, azs_val, wp_val)

                if (not require_unique) or (len(rows) == 1):
                    # если require_unique=false и строк несколько — берём первую (не рекомендуется)
                    if rows:
                        tid, ip, arm = rows[0]

                        if write_tid and tid:
                            con.execute(
                                """
                                INSERT OR IGNORE INTO message_entities(
                                  message_id, entity_type, entity_value, entity_raw, confidence, extractor, created_at_utc
                                )
                                VALUES (?, 'tid', ?, NULL, ?, 'directory:v1', ?)
                                """,
                                (message_id, str(tid), tid_conf, m.get("ts_utc")),
                            )

                        if write_ip and ip:
                            con.execute(
                                """
                                INSERT OR IGNORE INTO message_entities(
                                  message_id, entity_type, entity_value, entity_raw, confidence, extractor, created_at_utc
                                )
                                VALUES (?, 'ip', ?, NULL, ?, 'directory:v1', ?)
                                """,
                                (message_id, str(ip), ip_conf, m.get("ts_utc")),
                            )

        con.commit()
        return message_id
