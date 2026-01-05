import sqlite3
from typing import Any, Mapping

from pathlib import Path
from typing import Optional


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


def init_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(DDL)
        con.commit()


def save_message(
    db_path: str,
    ts_utc: str,
    chat_id: int,
    chat_type: Optional[str],
    from_id: Optional[int],
    username: Optional[str],
    text: Optional[str],
) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO messages(ts_utc, chat_id, chat_type, from_id, username, text) VALUES(?,?,?,?,?,?)",
            (ts_utc, chat_id, chat_type, from_id, username, text),
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
              from_display,
              forward_from_id, forward_from_name,
              content_type, has_media, service_action,
              edited_ts_utc,
              raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?,
                    ?,
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

                m.get("from_display"),

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
