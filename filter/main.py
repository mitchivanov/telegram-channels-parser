from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
from routes.filters import router as filters_router
from routes.health import router as health_router
from routes.moderation import router as moderation_router
from routes.media import router as media_router

app = FastAPI(title="Filter Service", version="1.0.0")

app.include_router(filters_router)
app.include_router(moderation_router)
app.include_router(media_router)
app.include_router(health_router)

class FilterRule(BaseModel):
    channel: str
    keywords: List[str] = []
    stopwords: List[str] = []
    remove_channel_links: bool = True
    moderation_required: bool = False

# In-memory хранилище фильтров (заменить на БД позже)
filters = {}

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_posts")
CHANNEL_TOPICS = {
    "channel1": os.environ.get("KAFKA_CHANNEL1_TOPIC", "channel1_posts"),
    "channel2": os.environ.get("KAFKA_CHANNEL2_TOPIC", "channel2_posts"),
    "channel3": os.environ.get("KAFKA_CHANNEL3_TOPIC", "channel3_posts"),
}

# Очередь для постов на модерацию (in-memory, заменить на БД/Redis при необходимости)
moderation_queue = []

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/filters", response_model=List[FilterRule])
def get_filters():
    return list(filters.values())

@app.post("/filters", response_model=FilterRule)
def set_filter(rule: FilterRule):
    filters[rule.channel] = rule
    return rule

@app.get("/filters/{channel}", response_model=Optional[FilterRule])
def get_filter(channel: str):
    return filters.get(channel)

@app.get("/moderation-queue")
def get_moderation_queue():
    return moderation_queue

# --- Kafka background task ---
async def filter_worker():
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
        async for msg in consumer:
            post = msg.value
            channel = post.get("channel")
            rule = filters.get(channel)
            if not rule:
                continue  # Нет фильтра — пропускаем
            # Фильтрация по ключевым словам и стоп-словам
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
                moderation_queue.append(post)
                continue
            # Маршрутизация в топик канала
            topic = CHANNEL_TOPICS.get(channel)
            if topic:
                await producer.send_and_wait(topic, post)
    finally:
        await consumer.stop()
        await producer.stop()

@app.on_event("startup")
def start_kafka_worker():
    loop = asyncio.get_event_loop()
    loop.create_task(filter_worker()) 