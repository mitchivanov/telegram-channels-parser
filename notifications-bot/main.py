import asyncio
import os
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.enums import ParseMode
from aiogram.types import Message
from redis.asyncio import Redis
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ["CODE_BOT_TOKEN"]
ADMIN_IDS = [int(x) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()]
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

# Redis ключи
REQUEST_CHANNEL = "tg:code:request"
RESPONSE_KEY = "tg:code:response"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
redis = Redis.from_url(REDIS_URL, decode_responses=True)

# Храним текущий ожидаемый тип кода (sms/2fa)
current_request_type = None

async def redis_listener():
    global current_request_type
    pubsub = redis.pubsub()
    await pubsub.subscribe(REQUEST_CHANNEL)
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = msg["data"]
            if isinstance(data, bytes):
                data = data.decode()
            if data not in ("sms", "2fa"):
                continue
            current_request_type = data
            text = "Пожалуйста, отправьте <b>код подтверждения</b> из SMS." if data == "sms" else "Пожалуйста, отправьте <b>пароль двухфакторной аутентификации</b>."
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, text)
                except Exception as e:
                    logging.warning(f"Не удалось отправить сообщение админу {admin_id}: {e}")
        except Exception as e:
            logging.error(f"Ошибка в redis_listener: {e}")

@dp.message(CommandStart())
async def start(message: Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Я помогу передать код подтверждения или 2FA для парсера.")
    else:
        await message.answer("Нет доступа.")

@dp.message(F.text)
async def handle_code(message: Message):
    global current_request_type
    if message.from_user.id not in ADMIN_IDS:
        return
    if not current_request_type:
        await message.answer("Сейчас не требуется код подтверждения или 2FA.")
        return
    code = message.text.strip()
    if current_request_type == "sms" and not code.isdigit():
        await message.answer("Пожалуйста, отправьте только <b>цифровой код</b> из SMS.")
        return
    # Сохраняем код в Redis и сбрасываем ожидание
    await redis.set(RESPONSE_KEY, code, ex=300)  # TTL 5 минут
    await message.answer("Код получен! Парсер продолжит работу.")
    current_request_type = None

async def main():
    await asyncio.gather(
        redis_listener(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    asyncio.run(main()) 