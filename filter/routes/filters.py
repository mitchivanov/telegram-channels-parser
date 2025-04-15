from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from ..db import SessionLocal
from ..models import Filter
from ..schemas import FilterCreate, FilterOut
from typing import List

router = APIRouter(prefix="/filters", tags=["Filters"])

async def get_db():
    async with SessionLocal() as session:
        yield session

@router.get("/", response_model=List[FilterOut], summary="Получить все фильтры")
async def get_filters(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Filter))
    return result.scalars().all()

@router.post("/", response_model=FilterOut, summary="Создать или обновить фильтр")
async def set_filter(rule: FilterCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Filter).where(Filter.channel == rule.channel))
    db_filter = result.scalar_one_or_none()
    if db_filter:
        db_filter.keywords = rule.keywords
        db_filter.stopwords = rule.stopwords
        db_filter.remove_channel_links = rule.remove_channel_links
        db_filter.moderation_required = rule.moderation_required
    else:
        db_filter = Filter(**rule.dict())
        db.add(db_filter)
    await db.commit()
    await db.refresh(db_filter)
    return db_filter

@router.get("/{channel}", response_model=FilterOut, summary="Получить фильтр по каналу")
async def get_filter(channel: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Filter).where(Filter.channel == channel))
    db_filter = result.scalar_one_or_none()
    if not db_filter:
        raise HTTPException(status_code=404, detail="Filter not found")
    return db_filter 