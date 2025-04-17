from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import os
from dotenv import load_dotenv
from db import engine, SessionLocal
from models import Base, Channel
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy.future import select
from contextlib import asynccontextmanager
import logging

logging.basicConfig(level=logging.INFO)

class FilterRule(BaseModel):
    channel: str
    keywords: List[str] = []
    stopwords: List[str] = []
    remove_channel_links: bool = True
    moderation_required: bool = False

# In-memory хранилище фильтров (заменить на БД позже)
# filters = {}

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("LIFESPAN ЗАПУЩЕН")
    await create_tables()
    await init_channels()
    yield

app = FastAPI(title="Filter Service", version="1.0.0", lifespan=lifespan, root_path="/api")

from routes.filters import router as filters_router
from routes.health import router as health_router
from routes.moderation import router as moderation_router
from routes.media import router as media_router

logging.info("Подключаю filters_router")
app.include_router(filters_router)
logging.info("Подключаю moderation_router")
app.include_router(moderation_router)
logging.info("Подключаю media_router")
app.include_router(media_router)
logging.info("Подключаю health_router")
app.include_router(health_router)

@app.get("/health")
def health():
    return {"status": "ok"}

async def init_channels():
    logging.info("INIT_CHANNELS ЗАПУЩЕН")
    load_dotenv()
    # Логируем все переменные окружения для диагностики
    logging.info(f"ENV: {dict(os.environ)}")
    # Собираем все переменные окружения вида CHANNEL*_ID
    channel_ids = [v for k, v in os.environ.items() if k.startswith('CHANNEL') and k.endswith('_ID') and v.strip()]
    logging.info(f"Каналы, которые должны быть добавлены: {channel_ids}")
    if channel_ids:
        async with SessionLocal() as session:
            result = await session.execute(select(Channel))
            existing = {c.name for c in result.scalars().all()}
            for cid in channel_ids:
                if cid not in existing:
                    session.add(Channel(name=cid))
            await session.commit()
            # Логируем, что реально есть в базе после инициализации
            result = await session.execute(select(Channel))
            all_channels = result.scalars().all()
            logging.info(f"Каналы в базе после инициализации: {[c.name for c in all_channels]}")

# --- Удалён запуск filter_worker из FastAPI --- 