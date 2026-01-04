import sqlite3
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
