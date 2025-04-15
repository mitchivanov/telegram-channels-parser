import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db import SessionLocal
from models import Filter, ModerationQueue

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_posts")
CHANNEL_TOPICS = {
    "channel1": os.environ.get("KAFKA_CHANNEL1_TOPIC", "channel1_posts"),
    "channel2": os.environ.get("KAFKA_CHANNEL2_TOPIC", "channel2_posts"),
    "channel3": os.environ.get("KAFKA_CHANNEL3_TOPIC", "channel3_posts"),
}

async def get_filters(db: AsyncSession):
    result = await db.execute(select(Filter))
    return {f.channel: f for f in result.scalars().all()}

async def add_to_moderation_queue(db: AsyncSession, post):
    db_post = ModerationQueue(
        channel=post["channel"],
        post_json=json.dumps(post, ensure_ascii=False),
        status="pending"
    )
    db.add(db_post)
    await db.commit()

async def kafka_filter_worker():
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
    await producer.start()
    try:
        while True:
            async for msg in consumer:
                post = msg.value
                async with SessionLocal() as db:
                    filters = await get_filters(db)
                    channel = post.get("channel")
                    rule = filters.get(channel)
                    if not rule:
                        continue
                    text = post.get("text", "").lower()
                    if rule.keywords and not any(kw.lower() in text for kw in rule.keywords):
                        continue
                    if any(sw.lower() in text for sw in rule.stopwords):
                        continue
                    # Вырезание ссылок на каналы
                    if rule.remove_channel_links:
                        import re
                        post["text"] = re.sub(r'https?://t\.me/\S+', '[ссылка удалена]', post["text"])
                        post["text"] = re.sub(r'@\w+', '[канал удален]', post["text"])
                    # Модерация
                    if rule.moderation_required:
                        await add_to_moderation_queue(db, post)
                        continue
                    # Маршрутизация в топик канала
                    topic = CHANNEL_TOPICS.get(channel)
                    if topic:
                        await producer.send_and_wait(topic, post)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(kafka_filter_worker()) 