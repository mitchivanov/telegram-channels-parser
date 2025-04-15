from sqlalchemy import Column, Integer, String, Boolean, Text
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.ext.asyncio import AsyncAttrs
from filter.db import Base

class Filter(Base, AsyncAttrs):
    __tablename__ = "filters"
    id = Column(Integer, primary_key=True, index=True)
    channel = Column(String, unique=True, index=True, nullable=False)
    keywords = Column(PG_ARRAY(String), default=[])
    stopwords = Column(PG_ARRAY(String), default=[])
    remove_channel_links = Column(Boolean, default=True)
    moderation_required = Column(Boolean, default=False)

class ModerationQueue(Base, AsyncAttrs):
    __tablename__ = "moderation_queue"
    id = Column(Integer, primary_key=True, index=True)
    channel = Column(String, index=True, nullable=False)
    post_json = Column(Text, nullable=False)
    status = Column(String, default="pending")  # pending, approved, rejected, edited 