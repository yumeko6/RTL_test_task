import asyncio

from json import loads
from os import getenv

from aiogram import Bot, Dispatcher
from aiogram.types import Message

from main import aggregate_salaries

# Initialize bot and dispatcher
bot = Bot(getenv("BOT_TOKEN"))
dp = Dispatcher()


@dp.message()
async def message_handler(message: Message) -> None:
    """
    Функция принимает сообщение в формате JSON от пользователя в телеграм боте,
    преобразует формат JSON в словарь и выполняет функцию аггрегирования данных
    в MongoDB.
    :param message: Сообщение пользователя в формате JSON
    :return: Ответ бота с аггрегированными данными.
    """
    request: dict = loads(message.text)
    answer = await aggregate_salaries(**request)
    await message.answer(str(answer))


async def start_bot() -> None:
    """
    Функция запуска телеграм бота.
    :return: None
    """
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(start_bot())
