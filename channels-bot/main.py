import os
import asyncio
import json
from aiogram import Bot
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputFile
import logging

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = os.environ.get("KAFKA_TOPICS", "channel1_posts,channel2_posts,channel3_posts").split(",")
CHANNEL1_ID = os.environ["CHANNEL1_ID"]  # Например, -1001234567890
CHANNEL2_ID = os.environ["CHANNEL2_ID"]
CHANNEL3_ID = os.environ["CHANNEL3_ID"]

TOPIC_TO_CHANNEL = {
    "channel1_posts": CHANNEL1_ID,
    "channel2_posts": CHANNEL2_ID,
    "channel3_posts": CHANNEL3_ID,
}

bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def send_post(bot, channel_id, post):
    logging.info(f"[BOT] Отправка поста в канал {channel_id}. post={json.dumps(post, ensure_ascii=False)[:500]}")
    text = post.get("text")
    media = post.get("media", [])
    if media:
        logging.info(f"[BOT] Найдено медиа: {len(media)} файлов. Типы: {[m.get('type') for m in media]}")
        for idx, m in enumerate(media):
            logging.info(f"[BOT] Медиа #{idx+1}: type={m.get('type')}, url={m.get('url')}")
        if len(media) == 1:
            m = media[0]
            try:
                local_path = m["local_path"]
                with open(local_path, 'rb') as f:
                    if m["type"] == "photo":
                        await bot.send_photo(chat_id=channel_id, photo=f, caption=text)
                    elif m["type"] == "video":
                        await bot.send_video(chat_id=channel_id, video=f, caption=text)
                    elif m["type"] == "document":
                        await bot.send_document(chat_id=channel_id, document=f, caption=text)
                logging.info(f"[BOT] Медиа успешно отправлено (одиночное)")
                try:
                    os.remove(local_path)
                    logging.info(f"[BOT] Файл удалён: {local_path}")
                except Exception as e:
                    logging.warning(f"[BOT] Не удалось удалить файл {local_path}: {e}")
            except Exception as e:
                logging.error(f"[BOT][ERR] Ошибка отправки одиночного медиа: {e}")
        else:
            group = []
            local_paths = []
            try:
                for i, m in enumerate(media):
                    caption_ = text if i == 0 else None
                    local_path = m["local_path"]
                    local_paths.append(local_path)
                    with open(local_path, 'rb') as f:
                        if m["type"] == "photo":
                            group.append(InputMediaPhoto(media=f, caption=caption_))
                        elif m["type"] == "video":
                            group.append(InputMediaVideo(media=f, caption=caption_))
                        elif m["type"] == "document":
                            group.append(InputMediaDocument(media=f, caption=caption_))
                await bot.send_media_group(chat_id=channel_id, media=group)
                logging.info(f"[BOT] Медиа-группа успешно отправлена")
                for local_path in local_paths:
                    try:
                        os.remove(local_path)
                        logging.info(f"[BOT] Файл удалён: {local_path}")
                    except Exception as e:
                        logging.warning(f"[BOT] Не удалось удалить файл {local_path}: {e}")
            except Exception as e:
                logging.error(f"[BOT][ERR] Ошибка отправки медиа-группы: {e}")
    else:
        logging.info(f"[BOT] Медиа не найдено, отправляю только текст")
        try:
            await bot.send_message(chat_id=channel_id, text=text)
            logging.info(f"[BOT] Текст успешно отправлен")
        except Exception as e:
            logging.error(f"[BOT][ERR] Ошибка отправки текста: {e}")

async def wait_for_kafka(bootstrap_servers, retries=12, delay=5):
    for attempt in range(retries):
        try:
            consumer = AIOKafkaConsumer(
                "__consumer_offsets",
                bootstrap_servers=bootstrap_servers
            )
            await consumer.start()
            await consumer.stop()
            print("[INFO] Kafka is available.")
            return
        except Exception as e:
            logging.warning(f"Kafka not ready, retrying in {delay}s... ({attempt+1}/{retries})")
            await asyncio.sleep(delay)
    raise RuntimeError("Kafka is not available after several retries")

async def kafka_worker():
    while True:
        try:
            await wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)
            consumer = AIOKafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            await consumer.start()
            try:
                async for msg in consumer:
                    post = msg.value
                    topic = msg.topic
                    logging.info(f"[KAFKA] Получено сообщение из топика {topic}: {json.dumps(post, ensure_ascii=False)[:500]}")
                    channel_id = TOPIC_TO_CHANNEL.get(topic)
                    if channel_id:
                        try:
                            await send_post(bot, channel_id, post)
                            logging.info(f"[OK] Sent to {channel_id}: {str(post)[:40]}")
                        except Exception as e:
                            logging.error(f"[ERR] Failed to send to {channel_id}: {e}")
                    else:
                        logging.warning(f"[WARN] No channel for topic {topic}")
            finally:
                await consumer.stop()
        except Exception as e:
            logging.error(f"Kafka worker error: {e}. Retrying in 10s...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(kafka_worker())
