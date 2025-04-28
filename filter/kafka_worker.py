import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from db import SessionLocal
from models import Filter
import logging
import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s() - %(message)s',
)
logger = logging.getLogger("kafka_worker")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_posts")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MODERATION_QUEUE = "moderation_queue"
APPROVED_QUEUE = "approved_queue"

def build_channel_topics():
    topics = {}
    for k, v in os.environ.items():
        if k.startswith('CHANNEL') and k.endswith('_ID') and v.strip():
            num = k[len('CHANNEL'):-len('_ID')]
            topic_env = f'KAFKA_CHANNEL{num}_TOPIC'
            topic = os.environ.get(topic_env, f'channel{num}_posts')
            topics[v] = topic
    return topics

CHANNEL_TOPICS = build_channel_topics()

async def get_filters(db: AsyncSession):
    result = await db.execute(select(Filter).options(selectinload(Filter.channel_obj)))
    filters = result.scalars().all()
    return {f.channel_obj.name: f for f in filters if f.channel_obj}

async def add_to_moderation_queue_redis(redis, post, target_channel):
    logger.info(f"[MODERATION] Кладу пост в Redis moderation_queue для канала {target_channel}")
    post_copy = dict(post)
    post_copy["target_channel"] = target_channel
    await redis.rpush(MODERATION_QUEUE, json.dumps(post_copy, ensure_ascii=False))
    logger.info(f"[MODERATION] Пост добавлен в Redis moderation_queue для канала {target_channel}")

async def send_to_channel_topic(producer, topic, post_filtered):
    logger.info(f"[KAFKA] Отправляю пост в топик {topic}")
    media = post_filtered.get("media", [])
    media_out = []
    for m in media:
        if m.get("local_path"):
            local_path = m["local_path"]
            if not os.path.exists(local_path):
                logger.error(f"[FILTER] Файл не найден: {local_path}")
                continue
            media_out.append({"type": m.get("type", "photo"), "local_path": local_path})
        else:
            url = m.get("url")
            media_out.append({"type": m.get("type", "photo"), "url": url})
    post_out = {
        "text": post_filtered.get("text", ""),
        "media": media_out
    }
    try:
        await producer.send_and_wait(topic, post_out)
        logger.info(f"[KAFKA] Пост успешно отправлен в топик {topic}")
        return True
    except Exception as e:
        logger.error(f"[KAFKA] Ошибка при отправке в топик {topic}: {e}")
        return False

async def approved_queue_worker(redis, producer):
    logger.info("[APPROVED_QUEUE] Запуск воркера очереди одобренных постов...")
    while True:
        post_json = await redis.lpop(APPROVED_QUEUE)
        if post_json:
            try:
                post = json.loads(post_json)
                channel = post.get("target_channel") or post.get("channel")
                topic = CHANNEL_TOPICS.get(channel)
                if not topic:
                    logger.error(f"[APPROVED_QUEUE] Не найден топик для канала {channel}")
                    continue
                logger.info(f"[APPROVED_QUEUE] Отправляю одобренный пост в топик {topic}")
                await send_to_channel_topic(producer, topic, post)
            except Exception as e:
                logger.error(f"[APPROVED_QUEUE] Ошибка обработки одобренного поста: {e}")
        else:
            await asyncio.sleep(1)

async def kafka_filter_worker():
    logger.info("[KAFKA] Запуск consumer и producer...")
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )
    await consumer.start()
    logger.info("[KAFKA] Consumer запущен")
    await producer.start()
    logger.info("[KAFKA] Producer запущен")
    redis = aioredis.from_url(REDIS_URL)
    # Запускаем воркер очереди одобренных постов
    asyncio.create_task(approved_queue_worker(redis, producer))
    try:
        while True:
            async for msg in consumer:
                logger.info(f"[KAFKA] Получено сообщение из топика {msg.topic}: {str(msg.value)[:100]}")
                post = msg.value
                # Сохраняем исходный канал для истории, если есть
                if "channel" in post:
                    post["source_channel"] = post["channel"]
                    del post["channel"]
                media_files = []
                for m in post.get("media", []):
                    if m.get("local_path") and os.path.exists(m["local_path"]):
                        media_files.append(m["local_path"])
                sent_to_any_channel = False
                send_tasks = []
                send_channels = []
                media_files_to_count = set()
                for m in post.get("media", []):
                    if m.get("local_path") and os.path.exists(m["local_path"]):
                        media_files_to_count.add(m["local_path"])
                num_channels = 0
                async with SessionLocal() as db:
                    filters = await get_filters(db)
                    logger.info(f"[FILTER] Загружено {len(filters)} фильтров. Начинаю обработку поста.")
                    text = post.get("text", "").lower()
                    available_channels = []
                    for env_key, env_value in os.environ.items():
                        if env_key.startswith('CHANNEL') and env_key.endswith('_ID') and env_value.strip():
                            available_channels.append(env_value)
                    logger.info(f"[FILTER] Доступные каналы: {available_channels}")
                    for target_channel in available_channels:
                        post_filtered = dict(post)
                        skip = False
                        if target_channel in filters:
                            logger.info(f"[FILTER] Найден фильтр для канала {target_channel}, применяю фильтрацию")
                            rule = filters[target_channel]
                            if rule.keywords and not any(kw.lower() in text for kw in rule.keywords):
                                logger.info(f"[FILTER] Пост не прошёл по ключевым словам для {target_channel}")
                                skip = True
                            if not skip and rule.stopwords and any(sw.lower() in text for sw in rule.stopwords):
                                logger.info(f"[FILTER] Пост не прошёл по стоп-словам для {target_channel}")
                                skip = True
                            if not skip and rule.remove_channel_links:
                                import re
                                post_filtered["text"] = re.sub(r'https?://t\.me/\S+', '[ссылка удалена]', post_filtered["text"])
                                post_filtered["text"] = re.sub(r'@\w+', '[канал удален]', post_filtered["text"])
                                logger.info(f"[FILTER] Ссылки на каналы удалены для {target_channel}")
                            if not skip and rule.moderation_required:
                                logger.info(f"[MODERATION] Требуется модерация для {target_channel}, отправляю в Redis moderation_queue")
                                await add_to_moderation_queue_redis(redis, post_filtered, target_channel)
                                skip = True
                                continue
                        else:
                            logger.info(f"[FILTER] Фильтр для канала {target_channel} не найден, пропускаю сообщение без фильтрации")
                        topic = CHANNEL_TOPICS.get(target_channel)
                        if not skip and topic:
                            logger.info(f"[KAFKA] Отправляю пост в топик {topic} для канала {target_channel}")
                            send_tasks.append(send_to_channel_topic(producer, topic, post_filtered))
                            send_channels.append(target_channel)
                        elif not skip:
                            logger.warning(f"[KAFKA] Не найден топик для канала {target_channel}")
                    num_channels = len(send_channels)
                    if num_channels > 0:
                        for f in media_files_to_count:
                            await redis.incrby(f"file:{f}", num_channels)
                    results = []
                    if send_tasks:
                        results = await asyncio.gather(*send_tasks, return_exceptions=True)
                        for ch, res in zip(send_channels, results):
                            logger.info(f"[KAFKA] Результат отправки в канал {ch}: {res}")
                        sent_to_any_channel = any(res is True for res in results)
                if not sent_to_any_channel and media_files:
                    for f in media_files:
                        try:
                            os.remove(f)
                            logger.info(f"[CLEANUP] Временный файл удалён: {f}")
                        except Exception as e:
                            logger.warning(f"[CLEANUP] Не удалось удалить файл {f}: {e}")
    finally:
        logger.info("[KAFKA] Остановка consumer и producer...")
        await consumer.stop()
        await producer.stop()
        logger.info("[KAFKA] Consumer и producer остановлены")

if __name__ == "__main__":
    asyncio.run(kafka_filter_worker()) 