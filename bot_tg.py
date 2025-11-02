#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from dotenv import load_dotenv

# === Настройки ===
load_dotenv("/opt/vk_checker/.env")

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
DOMAIN = "https://own-zone.ru"  # твой домен

if not BOT_TOKEN:
    raise RuntimeError("❌ В .env отсутствует TG_BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# === Проверка токена при запуске ===
async def check_bot_connection():
    try:
        me = await bot.get_me()
        print(f"✅ Подключено к Telegram как @{me.username} (ID: {me.id})")
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к Telegram API: {e}")
        return False


# === Команда /start ===
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    user_name = msg.from_user.first_name or "пользователь"
    link = f"{DOMAIN}/dashboard"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть VK Checker", web_app=WebAppInfo(url=link))]
        ]
    )

    await msg.answer(
        f"👋 Привет, {user_name}!\n\n"
        f"Это твой личный кабинет VK Checker.\n"
        f"Нажми кнопку ниже, чтобы открыть панель управления 👇",
        reply_markup=kb
    )


# === Команда /help ===
@dp.message(Command("help"))
async def help_cmd(msg: types.Message):
    await msg.answer(
        "🧭 Команды:\n"
        "/start — открыть VK Checker\n"
        "/help — помощь\n\n"
        "Открой WebApp прямо в Telegram для управления кабинетами."
    )


# === Точка входа ===
async def main():
    ok = await check_bot_connection()
    if not ok:
        print("🛑 Проверка подключения не пройдена. Проверь TG_BOT_TOKEN и интернет.")
        return

    print("🚀 VK Checker бот запущен. Ожидаем команды...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Бот остановлен.")
