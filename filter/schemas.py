from pydantic import BaseModel, Field
from typing import List, Optional, Literal

class FilterBase(BaseModel):
    channel: str = Field(..., example="channel1")
    keywords: List[str] = Field(default_factory=list, example=["python", "aiogram"])
    stopwords: List[str] = Field(default_factory=list, example=["spam", "casino"])
    remove_channel_links: bool = Field(default=True, example=True)
    moderation_required: bool = Field(default=False, example=False)

class FilterCreate(FilterBase):
    pass

class FilterOut(FilterBase):
    id: int

class ModerationPost(BaseModel):
    id: int
    channel: str
    post_json: str
    status: Literal["pending", "approved", "rejected", "edited"]

class PostIn(BaseModel):
    channel: str
    id: int
    date: str
    text: str
    media: Optional[list] = None 