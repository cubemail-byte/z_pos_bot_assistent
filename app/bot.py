import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from app.storage import init_db, save_message


def load_config() -> dict:
    config_path = Path(__file__).resolve().parent.parent / "config.yaml"
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set (put it into /opt/tg-agent/.env)")

    cfg = load_config()
    sqlite_path = cfg.get("storage", {}).get("sqlite_path", "data/agent.db")
    reply_in_groups = bool(cfg.get("bot", {}).get("reply_in_groups", False))

    init_db(sqlite_path)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("tg-agent")

    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        # В личке можно отвечать, в группах — по настройке
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("Привет! Я жив. Команда: /ping")

    @dp.message(Command("ping"))
    async def ping(message: Message):
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("pong")

    @dp.message(F.text)
    async def on_text(message: Message):
        ts_utc = datetime.now(timezone.utc).isoformat()

        save_message(
            db_path=sqlite_path,
            ts_utc=ts_utc,
            chat_id=message.chat.id,
            chat_type=message.chat.type,
            from_id=message.from_user.id if message.from_user else None,
            username=message.from_user.username if message.from_user else None,
            text=message.text,
        )

        log.info("saved message chat_id=%s msg_len=%s", message.chat.id, len(message.text or ""))

        # В группах молчим, если не разрешено
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return

        # В личке (и если разрешено в группах) — эхо как раньше
        await message.answer(message.text)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
