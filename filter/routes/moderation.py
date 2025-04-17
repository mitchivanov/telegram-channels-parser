from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db import SessionLocal
from models import ModerationQueue
from schemas import ModerationPost, PostIn
from typing import List
import json as pyjson

router = APIRouter(prefix="/moderation", tags=["Moderation"])

async def get_db():
    async with SessionLocal() as session:
        yield session

@router.get("/queue", response_model=List[ModerationPost], summary="Получить все посты на модерацию")
async def get_moderation_queue(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ModerationQueue).where(ModerationQueue.status == "pending"))
    return result.scalars().all()

@router.post("/queue", response_model=ModerationPost, summary="Добавить пост в очередь модерации")
async def add_to_moderation_queue(post: PostIn, db: AsyncSession = Depends(get_db)):
    db_post = ModerationQueue(
        channel=post.channel,
        post_json=pyjson.dumps(post.dict(), ensure_ascii=False),
        status="pending"
    )
    db.add(db_post)
    await db.commit()
    await db.refresh(db_post)
    return db_post

@router.patch("/queue/{post_id}", response_model=ModerationPost, summary="Обновить статус поста в модерации")
async def update_moderation_status(post_id: int, status: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ModerationQueue).where(ModerationQueue.id == post_id))
    db_post = result.scalar_one_or_none()
    if not db_post:
        raise HTTPException(status_code=404, detail="Post not found")
    db_post.status = status
    await db.commit()
    await db.refresh(db_post)
    return db_post 