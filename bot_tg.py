from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import CommandStart

@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    link = "https://own-zone.ru/"  # теперь напрямую на mini app
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть VK Checker", web_app=WebAppInfo(url=link))]
        ]
    )
