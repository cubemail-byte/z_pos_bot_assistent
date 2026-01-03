import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv


async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set (put it into /opt/tg-agent/.env)")

    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        await message.answer("Привет! Я жив. Напиши что-нибудь — я повторю 🙂")

    @dp.message(F.text)
    async def echo(message: Message):
        await message.answer(message.text)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
