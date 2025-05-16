import os
import asyncio
import json
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, FSInputFile
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

def get_channel_name(channel_id):
    return os.getenv(f"CHANNEL_{channel_id}_NAME") or channel_id

async def send_post_for_moderation(post: dict, message_id: str):
    logger.info(f"[MODERATION_DETAILS] Получен пост: {json.dumps(post, ensure_ascii=False)}")
    text = post.get("text", "<i>Без текста</i>")
    media = post.get("media", [])
    logger.info(f"[MODERATION_DETAILS] Медиа файлы ({len(media)}): {json.dumps(media, ensure_ascii=False)}")
    channel = post.get("target_channel") or post.get("source_channel")
    channel_name = get_channel_name(channel)
    channel_info = f"\n<b>Канал:</b> <code>{channel_name}</code>" if channel else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{message_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{message_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{message_id}")
        ]
    ])
    sent_msg = None
    if media:
        # Одиночное медиа
        if len(media) == 1:
            m = media[0]
            file_id = m.get("file_id")
            local_path = m.get("local_path")
            media_type = m.get("type")
            input_file = None
            if file_id:
                logger.info(f"[MODERATION_DETAILS] Использую file_id для {media_type}: {file_id}")
                try:
                    if media_type == "photo":
                        sent_msg = await bot.send_photo(MODERATION_CHAT_ID, photo=file_id, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                    elif media_type == "video":
                        sent_msg = await bot.send_video(MODERATION_CHAT_ID, video=file_id, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                    elif media_type == "document":
                        sent_msg = await bot.send_document(MODERATION_CHAT_ID, document=file_id, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                    logger.info(f"[MODERATION_DETAILS] Медиа успешно отправлено по file_id (одиночное)")
                except Exception as e:
                    logger.error(f"[MODERATION_DETAILS] Ошибка при отправке по file_id: {e}")
            elif local_path:
                logger.info(f"[MODERATION_DETAILS] Проверяю наличие файла: {local_path}")
                if not os.path.exists(local_path):
                    logger.error(f"[MODERATION_DETAILS] Файл не найден: {local_path}")
                else:
                    input_file = FSInputFile(local_path)
                    logger.info(f"[MODERATION_DETAILS] input_file создан: {input_file}")
                    try:
                        if media_type == "photo":
                            sent_msg = await bot.send_photo(MODERATION_CHAT_ID, photo=input_file, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                        elif media_type == "video":
                            sent_msg = await bot.send_video(MODERATION_CHAT_ID, video=input_file, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                        elif media_type == "document":
                            sent_msg = await bot.send_document(MODERATION_CHAT_ID, document=input_file, caption=f"<b>Пост на модерацию:</b>{channel_info}\n{text}", parse_mode=ParseMode.HTML, reply_markup=kb)
                        logger.info(f"[MODERATION_DETAILS] Медиа успешно отправлено (одиночное)")
                    except Exception as e:
                        logger.error(f"[MODERATION_DETAILS] Ошибка при отправке одиночного медиа: {e}")
            else:
                logger.error(f"[MODERATION_DETAILS] Нет file_id и нет валидного local_path для одиночного медиа: {m}")
        # Медиагруппа
        else:
            group = []
            local_paths = []
            for i, m in enumerate(media):
                file_id = m.get("file_id")
                local_path = m.get("local_path")
                media_type = m.get("type")
                caption_ = text if i == 0 else None
                input_file = None
                if file_id:
                    logger.info(f"[MODERATION_DETAILS] Использую file_id для медиагруппы {media_type}: {file_id}")
                    if media_type == "photo":
                        group.append(types.InputMediaPhoto(media=file_id, caption=caption_))
                    elif media_type == "video":
                        group.append(types.InputMediaVideo(media=file_id, caption=caption_))
                    elif media_type == "document":
                        group.append(types.InputMediaDocument(media=file_id, caption=caption_))
                elif local_path:
                    logger.info(f"[MODERATION_DETAILS] Проверяю наличие файла: {local_path}")
                    if not os.path.exists(local_path):
                        logger.error(f"[MODERATION_DETAILS] Файл не найден: {local_path}")
                        continue
                    local_paths.append(local_path)
                    input_file = FSInputFile(local_path)
                    logger.info(f"[MODERATION_DETAILS] input_file создан: {input_file}")
                    if media_type == "photo":
                        group.append(types.InputMediaPhoto(media=input_file, caption=caption_))
                    elif media_type == "video":
                        group.append(types.InputMediaVideo(media=input_file, caption=caption_))
                    elif media_type == "document":
                        group.append(types.InputMediaDocument(media=input_file, caption=caption_))
            if group:
                try:
                    sent_msgs = await bot.send_media_group(MODERATION_CHAT_ID, group)
                    logger.info(f"[MODERATION_DETAILS] Медиагруппа успешно отправлена")
                    # Сразу после медиагруппы отправляем отдельное сообщение с кнопками
                    try:
                        sent_msg = await bot.send_message(
                            MODERATION_CHAT_ID,
                            "<b>Пост на модерацию</b>",
                            reply_markup=kb
                        )
                        logger.info(f"[MODERATION_DETAILS] Сообщение с кнопками успешно отправлено после медиагруппы")
                    except Exception as e:
                        logger.error(f"[MODERATION_DETAILS] Ошибка при отправке сообщения с кнопками после медиагруппы: {e}")
                except Exception as e:
                    logger.error(f"[MODERATION_DETAILS] Ошибка при отправке медиагруппы: {e}")
            else:
                logger.error(f"[MODERATION_DETAILS] Нет валидных медиа для отправки медиагруппы")
    else:
        try:
            sent_msg = await bot.send_message(
                MODERATION_CHAT_ID,
                f"<b>Пост на модерацию:</b>{channel_info}\n{text}",
                reply_markup=kb
            )
            logger.info(f"[MODERATION_DETAILS] Текстовое сообщение успешно отправлено")
        except Exception as e:
            logger.error(f"[MODERATION_DETAILS] Ошибка при отправке текстового сообщения: {e}")
    if sent_msg:
        await redis.set(f"mod_msg:{sent_msg.message_id}", message_id, ex=3600)
    await redis.set(f"mod_post:{message_id}", json.dumps(post, ensure_ascii=False), ex=3600)
    await redis.set(f"moderation_status:{message_id}", "pending", ex=3600)
    logger.info(f"[MODERATION_DETAILS] Обработка поста завершена, статус: pending")

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
                logger.info(f"[MODERATION_RAW] Получены данные из очереди: {post_json}")
                post = json.loads(post_json)
                message_id = str(post.get("id") or hash(post_json))
                logger.info(f"[MODERATION] Получен пост на модерацию: {message_id}")
                await send_post_for_moderation(post, message_id)
            except json.JSONDecodeError as e:
                logger.error(f"[MODERATION] Ошибка декодирования JSON: {e}, данные: {post_json}")
            except Exception as e:
                logger.error(f"[MODERATION] Ошибка обработки поста: {e}")
                import traceback
                logger.error(f"[MODERATION] Трейс: {traceback.format_exc()}")
        else:
            await asyncio.sleep(1)

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(moderation_worker())
    logger.info("[BOT] Бот запущен и готов к модерации!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main()) 