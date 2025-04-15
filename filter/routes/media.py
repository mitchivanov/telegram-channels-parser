from fastapi import APIRouter, HTTPException
from minio import Minio
import os
from datetime import timedelta

router = APIRouter(prefix="/media", tags=["Media"])

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "parser-media")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@router.get("/{object_name}", summary="Получить временную ссылку на медиафайл в Minio")
def get_media_url(object_name: str):
    try:
        url = minio_client.presigned_get_object(
            MINIO_BUCKET,
            object_name,
            expires=timedelta(minutes=10)
        )
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Media not found: {e}") 