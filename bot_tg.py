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
DOMAIN = "https://own-zone.ru"  # домен, где крутится твой FastAPI

if not BOT_TOKEN:
    raise RuntimeError("❌ В .env отсутствует TG_BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()  # создаём до использования декораторов


# === Команда /start ===
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    """
    Отправляет кнопку для открытия мини-приложения VK Checker.
    """
    link = f"{DOMAIN}/dashboard"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Открыть VK Checker",
                    web_app=WebAppInfo(url=link)
                )
            ]
        ]
    )


# === Команда /help ===
@dp.message(Command("help"))
async def help_cmd(msg: types.Message):
    await msg.answer(
        "🧭 Команды:\n"
        "/start — открыть VK Checker\n"
        "/help — справка\n\n"
        "Открой мини-приложение прямо в Telegram для управления кабинетами."
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
