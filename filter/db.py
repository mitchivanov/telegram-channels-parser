from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import os

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql+asyncpg://postgres:postgres@db:5432/filter")

engine = create_async_engine(POSTGRES_DSN, echo=True, future=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base() 