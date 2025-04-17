import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from db import SessionLocal
from models import Filter, ModerationQueue
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_worker")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_posts")

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
    # Возвращаем словарь {channel.name: filter}
    return {f.channel_obj.name: f for f in filters if f.channel_obj}

async def add_to_moderation_queue(db: AsyncSession, post, target_channel):
    logger.info(f"[MODERATION] Добавляю пост в очередь модерации для канала {target_channel}")
    db_post = ModerationQueue(
        channel=target_channel,
        post_json=json.dumps(post, ensure_ascii=False),
        status="pending"
    )
    db.add(db_post)
    await db.commit()
    logger.info(f"[MODERATION] Пост добавлен в очередь модерации для канала {target_channel}")

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
    try:
        while True:
            async for msg in consumer:
                logger.info(f"[KAFKA] Получено сообщение из топика {msg.topic}: {str(msg.value)[:100]}")
                post = msg.value
                # Сохраняем исходный канал для истории, если есть
                if "channel" in post:
                    post["source_channel"] = post["channel"]
                    del post["channel"]
                async with SessionLocal() as db:
                    filters = await get_filters(db)
                    logger.info(f"[FILTER] Загружено {len(filters)} фильтров. Начинаю обработку поста.")
                    text = post.get("text", "").lower()
                    for target_channel, rule in filters.items():
                        logger.info(f"[FILTER] Проверяю фильтр для канала {target_channel}")
                        # Ключевые слова
                        if rule.keywords and not any(kw.lower() in text for kw in rule.keywords):
                            logger.info(f"[FILTER] Пост не прошёл по ключевым словам для {target_channel}")
                            continue
                        # Стоп-слова
                        if any(sw.lower() in text for sw in rule.stopwords):
                            logger.info(f"[FILTER] Пост не прошёл по стоп-словам для {target_channel}")
                            continue
                        # Вырезание ссылок на каналы
                        post_filtered = dict(post)  # копия для каждого канала
                        if rule.remove_channel_links:
                            import re
                            post_filtered["text"] = re.sub(r'https?://t\.me/\S+', '[ссылка удалена]', post_filtered["text"])
                            post_filtered["text"] = re.sub(r'@\w+', '[канал удален]', post_filtered["text"])
                            logger.info(f"[FILTER] Ссылки на каналы удалены для {target_channel}")
                        # Модерация
                        if rule.moderation_required:
                            logger.info(f"[MODERATION] Требуется модерация для {target_channel}, отправляю в очередь модерации")
                            await add_to_moderation_queue(db, post_filtered, target_channel)
                            continue
                        # Маршрутизация в топик канала
                        topic = CHANNEL_TOPICS.get(target_channel)
                        if topic:
                            logger.info(f"[KAFKA] Отправляю пост в топик {topic} для канала {target_channel}")
                            media = post_filtered.get("media", [])
                            media_out = []
                            for m in media:
                                if m.get("local_path"):
                                    local_path = m["local_path"]
                                    with open(local_path, 'rb') as f:
                                        media_out.append({"type": m.get("type", "photo"), "url": f.read()})
                                else:
                                    url = m.get("url")
                                    media_out.append({"type": m.get("type", "photo"), "url": url})
                            post_out = {
                                "text": post_filtered.get("text", ""),
                                "media": media_out
                            }
                            try:
                                await producer.send_and_wait(topic, post_out)
                                logger.info(f"[KAFKA] Пост успешно отправлен в топик {topic} для канала {target_channel}")
                            except Exception as e:
                                logger.error(f"[KAFKA] Ошибка при отправке в топик {topic} для канала {target_channel}: {e}")
                        else:
                            logger.warning(f"[KAFKA] Не найден топик для канала {target_channel}")
    finally:
        logger.info("[KAFKA] Остановка consumer и producer...")
        await consumer.stop()
        await producer.stop()
        logger.info("[KAFKA] Consumer и producer остановлены")

if __name__ == "__main__":
    asyncio.run(kafka_filter_worker()) 