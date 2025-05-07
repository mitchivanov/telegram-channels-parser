from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db import SessionLocal
from models import Filter, Channel
from schemas import FilterCreate, FilterOut, ChannelOut
from typing import List
import logging
import redis.asyncio as aioredis
import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
PAUSE_KEY_PREFIX = "filtering:paused:"

router = APIRouter(prefix="/filters", tags=["Filters"])

async def get_db():
    async with SessionLocal() as session:
        yield session

async def get_redis():
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield redis
    finally:
        await redis.close()

@router.get("/", response_model=List[FilterOut], summary="Получить все фильтры")
async def get_filters(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Filter))
    return result.scalars().all()

@router.get("/channels", response_model=List[ChannelOut], summary="Получить все каналы с фильтрами")
async def get_channels(db: AsyncSession = Depends(get_db)):
    try:
        # Получаем все каналы
        result = await db.execute(select(Channel))
        channels = result.scalars().all()
        
        # Готовим словарь для хранения результатов
        channel_map = {channel.id: {"id": channel.id, "name": channel.name, "filters": []} for channel in channels}
        
        # Если есть каналы, загружаем их фильтры
        if channels:
            channel_ids = [channel.id for channel in channels]
            filter_result = await db.execute(
                select(Filter).where(Filter.channel_id.in_(channel_ids))
            )
            filters = filter_result.scalars().all()
            
            # Распределяем фильтры по соответствующим каналам
            for filter_item in filters:
                if filter_item.channel_id in channel_map:
                    filter_dict = {
                        "id": filter_item.id,
                        "channel_id": filter_item.channel_id,
                        "channel": channel_map[filter_item.channel_id]["name"],
                        "keywords": filter_item.keywords or [],
                        "stopwords": filter_item.stopwords or [],
                        "remove_channel_links": filter_item.remove_channel_links,
                        "moderation_required": filter_item.moderation_required
                    }
                    channel_map[filter_item.channel_id]["filters"].append(filter_dict)
        
        # Преобразуем словарь в список для возврата
        return list(channel_map.values())
    except Exception as e:
        # Логируем ошибку и возвращаем понятное сообщение
        logging.error(f"Ошибка при получении каналов: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@router.post("/", response_model=FilterOut, summary="Создать или обновить фильтр")
async def set_filter(rule: FilterCreate, db: AsyncSession = Depends(get_db)):
    try:
        logging.info(f"Получен запрос на создание/обновление фильтра для канала: {rule.channel}")
        logging.info(f"Параметры фильтра: keywords={rule.keywords}, stopwords={rule.stopwords}, remove_links={rule.remove_channel_links}, moderation={rule.moderation_required}")
        
        # Найти канал по имени
        channel_obj = (await db.execute(select(Channel).where(Channel.name == rule.channel))).scalar_one_or_none()
        if not channel_obj:
            logging.warning(f"Канал {rule.channel} не найден")
            raise HTTPException(status_code=404, detail="Channel not found")
        
        logging.info(f"Найден канал: {channel_obj.name} (id: {channel_obj.id})")
        
        # Найти фильтр по channel_id
        result = await db.execute(select(Filter).where(Filter.channel_id == channel_obj.id))
        db_filter = result.scalar_one_or_none()
        
        if db_filter:
            logging.info(f"Найден существующий фильтр (id: {db_filter.id}), обновляем")
            db_filter.keywords = rule.keywords
            db_filter.stopwords = rule.stopwords
            db_filter.remove_channel_links = rule.remove_channel_links
            db_filter.moderation_required = rule.moderation_required
        else:
            logging.info(f"Фильтр не найден, создаем новый")
            db_filter = Filter(channel_id=channel_obj.id, keywords=rule.keywords, stopwords=rule.stopwords, remove_channel_links=rule.remove_channel_links, moderation_required=rule.moderation_required)
            db.add(db_filter)
        
        await db.commit()
        await db.refresh(db_filter)
        logging.info(f"Фильтр успешно сохранен с id: {db_filter.id}")
        
        return db_filter
    except Exception as e:
        logging.error(f"Ошибка при создании/обновлении фильтра: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@router.get("/pause_status", summary="Получить статусы паузы по всем каналам")
async def get_pause_status(db: AsyncSession = Depends(get_db), redis=Depends(get_redis)):
    result = await db.execute(select(Channel))
    channels = result.scalars().all()
    statuses = {}
    for channel in channels:
        key = f"{PAUSE_KEY_PREFIX}{channel.name}"
        paused = await redis.get(key)
        statuses[channel.id] = {"name": channel.name, "paused": paused == "1"}
    return statuses

@router.post("/pause/{channel_name}", summary="Поставить канал на паузу")
async def pause_channel(channel_name: str, redis=Depends(get_redis)):
    await redis.set(f"{PAUSE_KEY_PREFIX}{channel_name}", "1")
    return {"channel": channel_name, "paused": True}

@router.post("/resume/{channel_name}", summary="Снять канал с паузы")
async def resume_channel(channel_name: str, redis=Depends(get_redis)):
    await redis.set(f"{PAUSE_KEY_PREFIX}{channel_name}", "0")
    return {"channel": channel_name, "paused": False}

@router.get("/filter_by_id/{channel_id}", response_model=FilterOut, summary="Получить фильтр по id канала")
async def get_filter_by_id(channel_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Filter).where(Filter.channel_id == channel_id))
    db_filter = result.scalar_one_or_none()
    if not db_filter:
        raise HTTPException(status_code=404, detail="Filter not found")
    return db_filter

@router.get("/{channel}", response_model=FilterOut, summary="Получить фильтр по каналу")
async def get_filter(channel: str, db: AsyncSession = Depends(get_db)):
    try:
        logging.info(f"Получен запрос на получение фильтра для канала: {channel}")
        channel_obj = (await db.execute(select(Channel).where(Channel.name == channel))).scalar_one_or_none()
        if not channel_obj:
            logging.warning(f"Канал {channel} не найден")
            raise HTTPException(status_code=404, detail="Channel not found")
        logging.info(f"Найден канал: {channel_obj.name} (id: {channel_obj.id})")
        result = await db.execute(select(Filter).where(Filter.channel_id == channel_obj.id))
        db_filter = result.scalar_one_or_none()
        if not db_filter:
            logging.warning(f"Фильтр для канала {channel} не найден")
            raise HTTPException(status_code=404, detail="Filter not found")
        logging.info(f"Найден фильтр (id: {db_filter.id}) для канала {channel}")
        return db_filter
    except Exception as e:
        logging.error(f"Ошибка при получении фильтра для канала {channel}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}") 