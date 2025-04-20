import os
import asyncio
import json
from aiogram import Bot
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
from aiogram.types.input_file import FSInputFile
import logging
from redis.asyncio import Redis
import traceback

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

# Глобальный словарь для трекинга времени последней отправки по каждому каналу
last_sent = {}
MIN_INTERVAL = 3  # секунд между сообщениями в один канал

# Настройка максимально подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s() - %(message)s',
)

async def send_post(bot, channel_id, post, redis=None):
    logging.debug(f"[FUNC] Вход в send_post | channel_id={channel_id}, post={post}, redis={redis}")
    global last_sent
    now = asyncio.get_event_loop().time()
    last = last_sent.get(channel_id, 0)
    wait = last + MIN_INTERVAL - now
    logging.debug(f"[SEND_POST] now={now}, last={last}, wait={wait}")
    if wait > 0:
        logging.debug(f"[SEND_POST] Жду {wait} секунд перед отправкой в канал {channel_id}")
        await asyncio.sleep(wait)
    logging.info(f"[BOT] Отправка поста в канал {channel_id}. post={json.dumps(post, ensure_ascii=False)[:500]}")
    text = post.get("text")
    media = post.get("media", [])
    post_id = post.get("id")
    redis_key = None
    if redis and post_id:
        redis_key = f"sent:{channel_id}:{post_id}"
        logging.debug(f"[SEND_POST] Проверяю Redis на ключ {redis_key}")
        if await redis.exists(redis_key):
            logging.info(f"[BOT] Сообщение {post_id} для канала {channel_id} уже отправлено, пропуск")
            return False
    try:
        if media:
            logging.info(f"[BOT] Найдено медиа: {len(media)} файлов. Типы: {[m.get('type') for m in media]}")
            for idx, m in enumerate(media):
                logging.info(f"[BOT] Медиа #{idx+1}: type={m.get('type')}, url={m.get('url')}")
            if len(media) == 1:
                m = media[0]
                try:
                    local_path = m["local_path"]
                    logging.debug(f"[SEND_POST] Проверяю наличие файла: {local_path}")
                    if not os.path.exists(local_path):
                        logging.error(f"[BOT] Файл не найден: {local_path}")
                        return False
                    input_file = FSInputFile(local_path)
                    logging.debug(f"[SEND_POST] input_file создан: {input_file}")
                    if m["type"] == "photo":
                        await bot.send_photo(chat_id=channel_id, photo=input_file, caption=text)
                    elif m["type"] == "video":
                        await bot.send_video(chat_id=channel_id, video=input_file, caption=text)
                    elif m["type"] == "document":
                        await bot.send_document(chat_id=channel_id, document=input_file, caption=text)
                    logging.info(f"[BOT] Медиа успешно отправлено (одиночное)")
                    # Reference counting через Redis
                    if redis:
                        try:
                            count = await redis.decr(f"file:{local_path}")
                            logging.debug(f"[REDIS] DECR file:{local_path} -> {count}")
                            if count == 0:
                                await redis.set(f'delete_after:{local_path}', 1, ex=3600)  # 1 час
                                await redis.delete(f"file:{local_path}")
                                logging.info(f"[BOT] Файл {local_path} помечен на удаление через 1 час (refcount=0)")
                            else:
                                logging.info(f"[BOT] Файл {local_path} ещё нужен {count} каналам")
                        except Exception as e:
                            logging.warning(f"[BOT] Ошибка reference counting для {local_path}: {e}\n{traceback.format_exc()}")
                except Exception as e:
                    logging.error(f"[BOT][ERR] Ошибка отправки одиночного медиа: {e}\n{traceback.format_exc()}")
                    return False
            else:
                group = []
                local_paths = []
                try:
                    for i, m in enumerate(media):
                        caption_ = text if i == 0 else None
                        local_path = m["local_path"]
                        logging.debug(f"[SEND_POST] Проверяю наличие файла: {local_path}")
                        if not os.path.exists(local_path):
                            logging.error(f"[BOT] Файл не найден: {local_path}")
                            continue
                        local_paths.append(local_path)
                        input_file = FSInputFile(local_path)
                        logging.debug(f"[SEND_POST] input_file создан: {input_file}")
                        if m["type"] == "photo":
                            group.append(InputMediaPhoto(media=input_file, caption=caption_))
                        elif m["type"] == "video":
                            group.append(InputMediaVideo(media=input_file, caption=caption_))
                        elif m["type"] == "document":
                            group.append(InputMediaDocument(media=input_file, caption=caption_))
                    logging.debug(f"[SEND_POST] Отправляю медиа-группу: {group}")
                    await bot.send_media_group(chat_id=channel_id, media=group)
                    logging.info(f"[BOT] Медиа-группа успешно отправлена")
                    # Reference counting для всех файлов группы
                    if redis:
                        for local_path in local_paths:
                            try:
                                count = await redis.decr(f"file:{local_path}")
                                logging.debug(f"[REDIS] DECR file:{local_path} -> {count}")
                                if count == 0:
                                    await redis.set(f'delete_after:{local_path}', 1, ex=3600)  # 1 час
                                    await redis.delete(f"file:{local_path}")
                                    logging.info(f"[BOT] Файл {local_path} помечен на удаление через 1 час (refcount=0)")
                                else:
                                    logging.info(f"[BOT] Файл {local_path} ещё нужен {count} каналам")
                            except Exception as e:
                                logging.warning(f"[BOT] Ошибка reference counting для {local_path}: {e}\n{traceback.format_exc()}")
                except Exception as e:
                    logging.error(f"[BOT][ERR] Ошибка отправки медиа-группы: {e}\n{traceback.format_exc()}")
                    return False
        else:
            logging.info(f"[BOT] Медиа не найдено, отправляю только текст")
            try:
                await bot.send_message(chat_id=channel_id, text=text)
                logging.info(f"[BOT] Текст успешно отправлен")
            except Exception as e:
                logging.error(f"[BOT][ERR] Ошибка отправки текста: {e}\n{traceback.format_exc()}")
                return False
        if redis_key:
            await redis.set(redis_key, 1, expire=86400)
            logging.debug(f"[SEND_POST] Установлен ключ в Redis: {redis_key}")
        last_sent[channel_id] = asyncio.get_event_loop().time()
        logging.debug(f"[SEND_POST] last_sent обновлён: {last_sent}")
        logging.debug(f"[FUNC] Выход из send_post | channel_id={channel_id}, post_id={post_id}")
        return True
    except Exception as e:
        # Flood control: Too Many Requests
        logging.error(f"[BOT][ERR] Exception: {e}\n{traceback.format_exc()}")
        if 'retry after' in str(e):
            import re
            m = re.search(r'retry after (\\d+)', str(e))
            if m:
                retry = int(m.group(1))
                logging.warning(f"[BOT] Flood control, жду {retry} сек...")
                await asyncio.sleep(retry)
                return await send_post(bot, channel_id, post, redis=redis)
        raise

async def wait_for_kafka(bootstrap_servers, retries=12, delay=5):
    logging.debug(f"[FUNC] Вход в wait_for_kafka | bootstrap_servers={bootstrap_servers}, retries={retries}, delay={delay}")
    for attempt in range(retries):
        try:
            logging.debug(f"[KAFKA] Попытка подключения к Kafka (попытка {attempt+1})")
            consumer = AIOKafkaConsumer(
                "__consumer_offsets",
                bootstrap_servers=bootstrap_servers
            )
            await consumer.start()
            await consumer.stop()
            print("[INFO] Kafka is available.")
            logging.info("[KAFKA] Kafka доступна.")
            logging.debug(f"[FUNC] Выход из wait_for_kafka | успех")
            return
        except Exception as e:
            logging.warning(f"Kafka not ready, retrying in {delay}s... ({attempt+1}/{retries})\n{traceback.format_exc()}")
            await asyncio.sleep(delay)
    logging.error("Kafka is not available after several retries")
    raise RuntimeError("Kafka is not available after several retries")

async def kafka_channel_worker(topic, channel_id, bot, redis):
    logging.info(f"[KAFKA_WORKER] Запуск воркера для топика {topic} (канал {channel_id})")
    while True:
        try:
            await wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=f"channels-bot-{topic}"
            )
            await consumer.start()
            try:
                async for msg in consumer:
                    logging.debug(f"[KAFKA_WORKER][{topic}] Получено сообщение: {msg}")
                    post = msg.value
                    logging.info(f"[KAFKA][{topic}] Получено сообщение: {json.dumps(post, ensure_ascii=False)[:500]}")
                    try:
                        sent = await send_post(bot, channel_id, post, redis=redis)
                        logging.debug(f"[KAFKA_WORKER][{topic}] send_post вернул: {sent}")
                        if sent:
                            await consumer.commit()
                            logging.info(f"[OK][{topic}] Sent to {channel_id}: {str(post)[:40]}")
                        else:
                            logging.info(f"[SKIP][{topic}] Not sent to {channel_id}: {str(post)[:40]}")
                    except Exception as e:
                        logging.error(f"[ERR][{topic}] Failed to send to {channel_id}: {e}\n{traceback.format_exc()}")
            finally:
                await consumer.stop()
                logging.debug(f"[KAFKA_WORKER][{topic}] consumer.stop() выполнен")
        except Exception as e:
            logging.error(f"Kafka worker error for topic {topic}: {e}. Retrying in 10s...\n{traceback.format_exc()}")
            await asyncio.sleep(10)

async def main():
    redis = Redis.from_url("redis://redis:6379/0")
    tasks = []
    for topic, channel_id in TOPIC_TO_CHANNEL.items():
        tasks.append(asyncio.create_task(kafka_channel_worker(topic, channel_id, bot, redis)))
    logging.info(f"[MAIN] Запущено {len(tasks)} воркеров для Kafka-топиков: {list(TOPIC_TO_CHANNEL.keys())}")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    logging.info("[MAIN] Запуск main() через asyncio.run()")
    asyncio.run(main())
