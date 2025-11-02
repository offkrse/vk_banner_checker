#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandStart
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer

# === Настройки ===
load_dotenv("/opt/vk_checker/.env")

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
SECRET_KEY = os.getenv("VK_APP_SECRET", "very_secret_key")
DOMAIN = "https://own-zone.ru"  # твой домен, где крутится FastAPI

if not BOT_TOKEN:
    raise RuntimeError("❌ В .env отсутствует TG_BOT_TOKEN")

serializer = URLSafeTimedSerializer(SECRET_KEY)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# === Генерация ссылки на Mini App ===
def get_webapp_link(telegram_id: int) -> str:
    token = serializer.dumps({"telegram_id": telegram_id})
    return f"{DOMAIN}/auth?token={token}"


# === Команда /start ===
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    user_id = msg.from_user.id
    user_name = msg.from_user.first_name or "пользователь"
    link = get_webapp_link(user_id)

    text = (
        f"👋 Привет, {user_name}!\n\n"
        f"Это твой личный кабинет VK Checker.\n"
        f"Нажми кнопку ниже, чтобы открыть панель:"
    )

    kb = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="📊 Открыть VK Checker", url=link)]
        ]
    )

    await msg.answer(text, reply_markup=kb)


# === Команда /help ===
@dp.message(Command("help"))
async def help_cmd(msg: types.Message):
    await msg.answer(
        "🧭 Команды:\n"
        "/start — получить ссылку на VK Checker\n"
        "/help — помощь\n\n"
        "Открой WebApp, чтобы управлять кабинетами."
    )


# === Точка входа ===
async def main():
    print("🚀 Telegram бот VK Checker запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Бот остановлен.")
