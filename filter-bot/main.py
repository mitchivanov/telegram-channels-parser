import os
import asyncio
import json
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from loguru import logger
import redis.asyncio as aioredis
from dotenv import load_dotenv
from aiogram.client.default import DefaultBotProperties

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
MODERATION_CHAT_ID = int(os.getenv("MODERATION_CHAT_ID", "0"))
MODERATION_QUEUE = "moderation_queue"
APPROVED_QUEUE = "approved_queue"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

redis = None  # глобальный объект redis

class EditPost(StatesGroup):
    waiting_for_text = State()

async def send_post_for_moderation(post: dict, message_id: str):
    text = post.get("text", "<i>Без текста</i>")
    media = post.get("media", [])
    channel = post.get("target_channel") or post.get("channel")
    channel_info = f"\n<b>Канал:</b> <code>{channel}</code>" if channel else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{message_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{message_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{message_id}")
        ]
    ])
    if media:
        msg = await bot.send_message(
            MODERATION_CHAT_ID,
            f"<b>Пост на модерацию:</b>{channel_info}\n{text}",
            reply_markup=kb
        )
    else:
        msg = await bot.send_message(
            MODERATION_CHAT_ID,
            f"<b>Пост на модерацию:</b>{channel_info}\n{text}",
            reply_markup=kb
        )
    await redis.set(f"mod_msg:{msg.message_id}", message_id, ex=3600)
    await redis.set(f"mod_post:{message_id}", json.dumps(post, ensure_ascii=False), ex=3600)
    await redis.set(f"moderation_status:{message_id}", "pending", ex=3600)

@dp.callback_query(F.data.startswith("approve:"))
async def approve_post(callback: CallbackQuery):
    message_id = callback.data.split(":", 1)[1]
    post_json = await redis.get(f"mod_post:{message_id}")
    if not post_json:
        await callback.answer("Пост не найден.", show_alert=True)
        return
    await redis.rpush(APPROVED_QUEUE, post_json)
    await redis.set(f"moderation_status:{message_id}", "approved", ex=3600)
    await callback.answer("Пост одобрен!")
    await callback.message.delete()

@dp.callback_query(F.data.startswith("reject:"))
async def reject_post(callback: CallbackQuery):
    message_id = callback.data.split(":", 1)[1]
    await redis.set(f"moderation_status:{message_id}", "rejected", ex=3600)
    await callback.answer("Пост отклонён!")
    await callback.message.delete()

@dp.callback_query(F.data.startswith("edit:"))
async def edit_post(callback: CallbackQuery, state: FSMContext):
    message_id = callback.data.split(":", 1)[1]
    await state.set_state(EditPost.waiting_for_text)
    await state.update_data(message_id=message_id, mod_msg_id=callback.message.message_id)
    await callback.message.reply("✏️ Введите новый текст для поста:")
    await callback.answer()

@dp.message(EditPost.waiting_for_text)
async def process_new_text(message: Message, state: FSMContext):
    data = await state.get_data()
    message_id = data["message_id"]
    mod_msg_id = data["mod_msg_id"]
    post_json = await redis.get(f"mod_post:{message_id}")
    if not post_json:
        await message.reply("Пост не найден.")
        await state.clear()
        return
    post = json.loads(post_json)
    post["text"] = message.text
    await redis.set(f"mod_post:{message_id}", json.dumps(post, ensure_ascii=False), ex=3600)
    await redis.set(f"moderation_status:{message_id}", "edited", ex=3600)
    # Обновляем сообщение с кнопками
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{message_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{message_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{message_id}")
        ]
    ])
    try:
        await bot.edit_message_text(f"<b>Пост на модерацию (отредактирован):</b>\n{message.text}", MODERATION_CHAT_ID, mod_msg_id, reply_markup=kb)
    except Exception as e:
        logger.error(f"[EDIT] Не удалось обновить сообщение: {e}")
    await message.reply("Текст обновлён. Теперь вы можете одобрить или отклонить пост.")
    await state.clear()

async def moderation_worker():
    global redis
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info("[MODERATION] Стартую воркер очереди модерации...")
    while True:
        post_json = await redis.lpop(MODERATION_QUEUE)
        if post_json:
            try:
                post = json.loads(post_json)
                message_id = str(post.get("id") or hash(post_json))
                logger.info(f"[MODERATION] Получен пост на модерацию: {message_id}")
                await send_post_for_moderation(post, message_id)
            except Exception as e:
                logger.error(f"[MODERATION] Ошибка обработки поста: {e}")
        else:
            await asyncio.sleep(1)

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(moderation_worker())
    logger.info("[BOT] Бот запущен и готов к модерации!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main()) 