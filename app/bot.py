import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
import json

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from storage import init_db, save_message, save_message_raw  # <- ВАЖНО: локальный импорт из app/storage.py

def load_config(project_root: Path) -> dict:
    config_path = project_root / "config.yaml"
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

def chat_alias_for(chat_id: int, cfg: dict) -> str | None:
    for c in cfg.get("chats", []):
        try:
            if int(c.get("chat_id")) == int(chat_id):
                return c.get("alias")
        except Exception:
            continue
    return None

def message_to_raw_json(message: Message) -> str:
    try:
        data = message.model_dump()
    except Exception:
        try:
            data = message.to_python()
        except Exception:
            data = {"repr": repr(message)}
    return json.dumps(data, ensure_ascii=False)


async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set (put it into /opt/tg-agent/.env)")

    project_root = Path(__file__).resolve().parent.parent  # .../app/bot.py -> .../
    cfg = load_config(project_root)

    sqlite_path_cfg = cfg.get("storage", {}).get("sqlite_path", "data/agent.db")
    sqlite_path = Path(sqlite_path_cfg)
    if not sqlite_path.is_absolute():
        sqlite_path = project_root / sqlite_path  # всегда относительно корня проекта
    sqlite_path = str(sqlite_path)

    reply_in_groups = bool(cfg.get("bot", {}).get("reply_in_groups", False))

    init_db(sqlite_path)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("tg-agent")

    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("Привет! Я жив. Команда: /ping")

    @dp.message(Command("ping"))
    async def ping(message: Message):
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("pong")

    @dp.message()
    async def on_message(message: Message):
        ts_utc = datetime.now(timezone.utc).isoformat()

        chat_id = message.chat.id
        alias = chat_alias_for(chat_id, cfg)

        from_id = message.from_user.id if message.from_user else None
        username = message.from_user.username if message.from_user else None

        from_display = None
        if message.from_user:
            first = (message.from_user.first_name or "").strip()
            last = (message.from_user.last_name or "").strip()
            from_display = (first + " " + last).strip() or None

        reply_to_tg_message_id = (
            message.reply_to_message.message_id
            if message.reply_to_message
            else None
        )

        # --- NEW: text может быть в caption (фото/док с подписью) ---
        text = message.text or message.caption or ""

        # --- NEW: content_type + has_media без сохранения контента ---
        content_type = getattr(message, "content_type", None) or "other"
        has_media = 1 if content_type in {
            "photo", "video", "document", "audio", "voice", "video_note", "animation", "sticker"
        } else 0

        # --- NEW: service events ---
        service_action = None
        if getattr(message, "new_chat_members", None):
            content_type = "service"
            has_media = 0
            service_action = "new_chat_members"
        elif getattr(message, "left_chat_member", None):
            content_type = "service"
            has_media = 0
            service_action = "left_chat_member"
        elif getattr(message, "pinned_message", None):
            content_type = "service"
            has_media = 0
            service_action = "pinned_message"

        save_message_raw(
            db_path=sqlite_path,
            m={
                "ts_utc": ts_utc,
                "chat_id": chat_id,
                "chat_type": message.chat.type,
                "chat_alias": alias,

                "from_id": from_id,
                "username": username,
                "from_display": from_display,

                "text": text,

                "tg_message_id": message.message_id,
                "reply_to_tg_message_id": reply_to_tg_message_id,

                "content_type": content_type,
                "has_media": has_media,
                "service_action": service_action,

                "edited_ts_utc": (
                    message.edit_date.astimezone(timezone.utc).isoformat()
                    if message.edit_date
                    else None
                ),

                "raw_json": message_to_raw_json(message),
            },
        )

        log.info(
            "saved raw message chat_id=%s tg_message_id=%s alias=%s content_type=%s has_media=%s",
            chat_id,
            message.message_id,
            alias,
            content_type,
            has_media,
        )

        # тихий режим в группах
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return

        # В личке эхо оставляем. В группе до этой строки не дойдём из-за return выше.
        await message.answer(text)


    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
