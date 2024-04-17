import asyncio
import json
import logging
import os
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from algorithm import aggregator
from helper import is_valid_message

load_dotenv()
TOKEN = os.environ.get('TOKEN')

dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """This handler receives messages with `/start` command."""
    await message.answer(f"Hello, {message.from_user.username}!")


@dp.message(Command('help'))
async def command_help_handler(message: Message) -> None:
    """This handler receives messages with `/help` command."""
    await message.answer(
        'Алгоритм принимает на вход сообщение в JSON-формате со следующими данными:\n'
        '1) Дату и время старта агрегации в ISO формате (dt_from).\n'
        '2) Дату и время окончания агрегации в ISO формате (dt_upto).\n'
        '3) Тип агрегации (group_type). Типы агрегации '
        'могут быть следующие: hour, day, month.\n'
    )


@dp.message()
async def message_all_handler(message: Message) -> None:
    """Message handler will handle all message."""
    check = is_valid_message(message.text)

    if not check:
        return await message.answer('Ошибка')

    result = aggregator.aggregate_data(*check)
    await message.answer(json.dumps(result))


async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
