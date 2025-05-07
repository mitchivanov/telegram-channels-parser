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
from collections import defaultdict
import time

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
MIN_INTERVAL = 5  # секунд между сообщениями в один канал

# --- Floodwait control ---
channel_floodwait = defaultdict(lambda: 0)
channel_locks = defaultdict(asyncio.Lock)

# --- Контроль живости воркера ---
KAFKA_IDLE_WARNING_SEC = 30

# Настройка максимально подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s() - %(message)s',
)

# Отключение спама логов от Kafka и связанных модулей
logging.getLogger('aiokafka').setLevel(logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('conn').setLevel(logging.WARNING)
logging.getLogger('fetcher').setLevel(logging.WARNING)
logging.getLogger('group_coordinator').setLevel(logging.WARNING)

async def send_post(bot, channel_id, post, redis=None, max_retries=5, _attempt=1):
    logging.debug(f"[FUNC] Вход в send_post | channel_id={channel_id}, post={post}, redis={redis}, attempt={_attempt}")
    global last_sent
    now = asyncio.get_event_loop().time()
    last = last_sent.get(channel_id, 0)
    wait = last + MIN_INTERVAL - now
    floodwait_until = channel_floodwait[channel_id]
    if now < floodwait_until:
        logging.warning(f"[FLOODWAIT] Floodwait активен для канала {channel_id}, жду {floodwait_until - now:.1f} сек (попытка {_attempt})")
        await asyncio.sleep(floodwait_until - now)
        logging.info(f"[FLOODWAIT] Выход из FLOODWAIT для канала {channel_id} (попытка {_attempt})")
    async with channel_locks[channel_id]:
        now = asyncio.get_event_loop().time()
        floodwait_until = channel_floodwait[channel_id]
        if now < floodwait_until:
            logging.warning(f"[FLOODWAIT] Floodwait активен (lock) для канала {channel_id}, жду {floodwait_until - now:.1f} сек (попытка {_attempt})")
            await asyncio.sleep(floodwait_until - now)
            logging.info(f"[FLOODWAIT] Выход из FLOODWAIT (lock) для канала {channel_id} (попытка {_attempt})")
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
            try:
                logging.debug(f"[SEND_POST] Проверяю Redis на ключ {redis_key}")
                if await redis.exists(redis_key):
                    logging.info(f"[BOT] Сообщение {post_id} для канала {channel_id} уже отправлено, пропуск")
                    return False
            except Exception as e:
                logging.error(f"[REDIS][ERR] Ошибка при exists({redis_key}): {e}\n{traceback.format_exc()}")
        try:
            if media:
                logging.info(f"[BOT] Найдено медиа: {len(media)} файлов. Типы: {[m.get('type') for m in media]}")
                for idx, m in enumerate(media):
                    logging.info(f"[BOT] Медиа #{idx+1}: type={m.get('type')}, url={m.get('url')}, file_id={m.get('file_id')}")
                if len(media) == 1:
                    m = media[0]
                    try:
                        # Приоритет 1: file_id (новый формат)
                        if m.get("file_id"):
                            file_id = m["file_id"]
                            logging.debug(f"[SEND_POST] Использую file_id: {file_id}")
                            if m["type"] == "photo":
                                await bot.send_photo(chat_id=channel_id, photo=file_id, caption=text)
                            elif m["type"] == "video":
                                await bot.send_video(chat_id=channel_id, video=file_id, caption=text)
                            elif m["type"] == "document":
                                await bot.send_document(chat_id=channel_id, document=file_id, caption=text)
                            logging.info(f"[BOT] Медиа успешно отправлено по file_id (одиночное)")
                        # Приоритет 2: local_path (старый формат)
                        elif m.get("local_path"):
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
                            if redis:
                                try:
                                    count = await redis.decr(f"file:{local_path}")
                                    logging.debug(f"[REDIS] DECR file:{local_path} -> {count}")
                                    if count == 0:
                                        await redis.set(f'delete_after:{local_path}', 1, ex=3600)
                                        await redis.delete(f"file:{local_path}")
                                        logging.info(f"[BOT] Файл {local_path} помечен на удаление через 1 час (refcount=0)")
                                    else:
                                        logging.info(f"[BOT] Файл {local_path} ещё нужен {count} каналам")
                                except Exception as e:
                                    logging.warning(f"[BOT] Ошибка reference counting для {local_path}: {e}\n{traceback.format_exc()}")
                    except Exception as e:
                        if hasattr(e, 'retry_after'):
                            logging.warning(f"[FLOODWAIT][DETAILS] Exception: {e} | retry_after={getattr(e, 'retry_after', None)} | type={type(e)} | args={e.args}")
                            retry = getattr(e, 'retry_after', 5)
                            channel_floodwait[channel_id] = asyncio.get_event_loop().time() + retry
                            logging.warning(f"[FLOODWAIT] Flood control (одиночное) для {channel_id}, жду {retry} сек... (попытка {_attempt})")
                            if _attempt >= max_retries:
                                logging.error(f"[ALERT][FLOODWAIT] Превышено число попыток отправки ({max_retries}) для канала {channel_id}. Пропускаю сообщение.")
                                return False
                            await asyncio.sleep(retry)
                            logging.info(f"[FLOODWAIT] Выход из FLOODWAIT (одиночное) для {channel_id} (попытка {_attempt})")
                            return await send_post(bot, channel_id, post, redis=redis, max_retries=max_retries, _attempt=_attempt+1)
                        logging.error(f"[BOT][ERR] Ошибка отправки одиночного медиа: {e}\n{traceback.format_exc()}")
                        return False
                else:
                    group = []
                    local_paths = []
                    try:
                        for i, m in enumerate(media):
                            caption_ = text if i == 0 else None
                            # Приоритет 1: file_id (новый формат)
                            if m.get("file_id"):
                                file_id = m["file_id"]
                                logging.debug(f"[SEND_POST] Использую file_id для медиагруппы: {file_id}")
                                if m["type"] == "photo":
                                    group.append(InputMediaPhoto(media=file_id, caption=caption_))
                                elif m["type"] == "video":
                                    group.append(InputMediaVideo(media=file_id, caption=caption_))
                                elif m["type"] == "document":
                                    group.append(InputMediaDocument(media=file_id, caption=caption_))
                            # Приоритет 2: local_path (старый формат)
                            elif m.get("local_path"):
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
                        if redis and local_paths:
                            for local_path in local_paths:
                                try:
                                    count = await redis.decr(f"file:{local_path}")
                                    logging.debug(f"[REDIS] DECR file:{local_path} -> {count}")
                                    if count == 0:
                                        await redis.set(f'delete_after:{local_path}', 1, ex=3600)
                                        await redis.delete(f"file:{local_path}")
                                        logging.info(f"[BOT] Файл {local_path} помечен на удаление через 1 час (refcount=0)")
                                    else:
                                        logging.info(f"[BOT] Файл {local_path} ещё нужен {count} каналам")
                                except Exception as e:
                                    logging.warning(f"[BOT] Ошибка reference counting для {local_path}: {e}\n{traceback.format_exc()}")
                    except Exception as e:
                        if hasattr(e, 'retry_after'):
                            logging.warning(f"[FLOODWAIT][DETAILS] Exception: {e} | retry_after={getattr(e, 'retry_after', None)} | type={type(e)} | args={e.args}")
                            retry = getattr(e, 'retry_after', 10)
                            channel_floodwait[channel_id] = asyncio.get_event_loop().time() + retry
                            logging.warning(f"[FLOODWAIT] Flood control (альбом) для {channel_id}, жду {retry} сек... (попытка {_attempt})")
                            if _attempt >= max_retries:
                                logging.error(f"[ALERT][FLOODWAIT] Превышено число попыток отправки ({max_retries}) для канала {channel_id}. Пропускаю сообщение.")
                                return False
                            await asyncio.sleep(retry)
                            logging.info(f"[FLOODWAIT] Выход из FLOODWAIT (альбом) для {channel_id} (попытка {_attempt})")
                            return await send_post(bot, channel_id, post, redis=redis, max_retries=max_retries, _attempt=_attempt+1)
                        logging.error(f"[BOT][ERR] Ошибка отправки медиа-группы: {e}\n{traceback.format_exc()}")
                        return False
            else:
                logging.info(f"[BOT] Медиа не найдено, отправляю только текст")
                try:
                    await bot.send_message(chat_id=channel_id, text=text)
                    logging.info(f"[BOT] Текст успешно отправлен")
                except Exception as e:
                    if hasattr(e, 'retry_after'):
                        logging.warning(f"[FLOODWAIT][DETAILS] Exception: {e} | retry_after={getattr(e, 'retry_after', None)} | type={type(e)} | args={e.args}")
                        retry = getattr(e, 'retry_after', 3)
                        channel_floodwait[channel_id] = asyncio.get_event_loop().time() + retry
                        logging.warning(f"[FLOODWAIT] Flood control (текст) для {channel_id}, жду {retry} сек... (попытка {_attempt})")
                        if _attempt >= max_retries:
                            logging.error(f"[ALERT][FLOODWAIT] Превышено число попыток отправки ({max_retries}) для канала {channel_id}. Пропускаю сообщение.")
                            return False
                        await asyncio.sleep(retry)
                        logging.info(f"[FLOODWAIT] Выход из FLOODWAIT (текст) для {channel_id} (попытка {_attempt})")
                        return await send_post(bot, channel_id, post, redis=redis, max_retries=max_retries, _attempt=_attempt+1)
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
            logging.error(f"[BOT][ERR] Exception: {e}\n{traceback.format_exc()}")
            if hasattr(e, 'retry_after'):
                logging.warning(f"[FLOODWAIT][DETAILS] Exception: {e} | retry_after={getattr(e, 'retry_after', None)} | type={type(e)} | args={e.args}")
                retry = getattr(e, 'retry_after', 5)
                channel_floodwait[channel_id] = asyncio.get_event_loop().time() + retry
                logging.warning(f"[FLOODWAIT] Flood control (generic) для {channel_id}, жду {retry} сек... (попытка {_attempt})")
                if _attempt >= max_retries:
                    logging.error(f"[ALERT][FLOODWAIT] Превышено число попыток отправки ({max_retries}) для канала {channel_id}. Пропускаю сообщение.")
                    return False
                await asyncio.sleep(retry)
                logging.info(f"[FLOODWAIT] Выход из FLOODWAIT (generic) для {channel_id} (попытка {_attempt})")
                return await send_post(bot, channel_id, post, redis=redis, max_retries=max_retries, _attempt=_attempt+1)
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
    last_msg_time = time.time()
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
                    now = time.time()
                    if now - last_msg_time > KAFKA_IDLE_WARNING_SEC:
                        logging.warning(f"[KAFKA_WORKER][{topic}] Нет новых сообщений {int(now - last_msg_time)} сек!")
                    last_msg_time = now
                    logging.debug(f"[KAFKA_WORKER][{topic}] Получено сообщение: {msg}")
                    post = msg.value
                    logging.info(f"[KAFKA][{topic}] Получено сообщение: {json.dumps(post, ensure_ascii=False)[:500]}")
                    try:
                        t0 = time.time()
                        # Ограничение по времени на send_post (например, 120 секунд)
                        sent = await asyncio.wait_for(send_post(bot, channel_id, post, redis=redis), timeout=120)
                        t1 = time.time()
                        logging.debug(f"[KAFKA_WORKER][{topic}] send_post вернул: {sent}, обработка заняла {t1-t0:.2f} сек")
                        if sent:
                            await consumer.commit()
                            logging.info(f"[OK][{topic}] Sent to {channel_id}: {str(post)[:40]}")
                        else:
                            logging.info(f"[SKIP][{topic}] Not sent to {channel_id}: {str(post)[:40]}")
                    except asyncio.TimeoutError:
                        logging.critical(f"[CRITICAL][{topic}] send_post завис (timeout)! Пропускаю сообщение.")
                    except Exception as e:
                        logging.error(f"[ERR][{topic}] Failed to send to {channel_id}: {e}\n{traceback.format_exc()}")
                # Если consumer зависает, логируем это
                if time.time() - last_msg_time > KAFKA_IDLE_WARNING_SEC:
                    logging.warning(f"[KAFKA_WORKER][{topic}] Нет новых сообщений уже {int(time.time() - last_msg_time)} сек!")
            finally:
                await consumer.stop()
                logging.debug(f"[KAFKA_WORKER][{topic}] consumer.stop() выполнен")
                logging.critical(f"[KAFKA_WORKER][{topic}] Воркера завершился (finally)")
        except Exception as e:
            logging.critical(f"[CRITICAL][{topic}] Kafka worker error: {e}. Retrying in 10s...\n{traceback.format_exc()}")
            await asyncio.sleep(10)

async def main():
    redis = Redis.from_url("redis://redis:6379/0")
    while True:
        tasks = []
        for topic, channel_id in TOPIC_TO_CHANNEL.items():
            logging.info(f"[MAIN] Стартую воркер для топика {topic} (канал {channel_id})")
            tasks.append(asyncio.create_task(kafka_channel_worker(topic, channel_id, bot, redis)))
        logging.info(f"[MAIN] Запущено {len(tasks)} воркеров для Kafka-топиков: {list(TOPIC_TO_CHANNEL.keys())}")
        done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        logging.critical(f"[MAIN] Все воркеры завершились! Перезапускаю main() через 10 секунд...")
        for t in done:
            logging.critical(f"[MAIN] Воркер завершился с результатом: {t.result() if not t.cancelled() else 'cancelled'}")
        await asyncio.sleep(10)

if __name__ == "__main__":
    logging.info("[MAIN] Запуск main() через asyncio.run()")
    asyncio.run(main())
