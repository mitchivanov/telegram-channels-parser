from fastapi import APIRouter

router = APIRouter(tags=["Health"])

@router.get("/health", summary="Проверка работоспособности сервиса")
async def health():
    return {"status": "ok"}