import asyncio
import os
import logging
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", 600))  # по умолчанию 10 минут

logging.basicConfig(level=logging.INFO, format="[CLEANUP] %(asctime)s %(levelname)s: %(message)s")

async def cleanup_temp_files(redis, interval=CLEANUP_INTERVAL):
    while True:
        keys = await redis.keys('delete_after:*')
        for key in keys:
            local_path = key.decode().replace('delete_after:', '')
            if not os.path.exists(local_path):
                await redis.delete(key)
                continue
            ttl = await redis.ttl(key)
            if ttl == -2:  # ключ уже истёк
                try:
                    os.remove(local_path)
                    logging.info(f'Файл удалён: {local_path}')
                except Exception as e:
                    logging.warning(f'Не удалось удалить файл {local_path}: {e}')
                await redis.delete(key)
        await asyncio.sleep(interval)

async def main():
    redis = aioredis.from_url(REDIS_URL, decode_responses=False)
    try:
        await cleanup_temp_files(redis)
    finally:
        await redis.close()

if __name__ == "__main__":
    asyncio.run(main()) 