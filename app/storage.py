import sqlite3
from typing import Any, Mapping

from pathlib import Path
from typing import Optional
from entities_engine import extract_entities


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

        con.commit()
        return message_id
