import asyncio
import os
import logging
import redis.asyncio as aioredis
from dotenv import load_dotenv
import time

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", 1))  # секунда для быстрой очистки
TEMP_DIR = os.getenv("TEMP_DIR", "/app/temp")
MAX_FILE_AGE_HOURS = 24

logging.basicConfig(level=logging.INFO, format="[CLEANUP_WORKER] %(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

async def cleanup_old_temp_files(temp_dir=TEMP_DIR, max_age_hours=MAX_FILE_AGE_HOURS):
    now = time.time()
    deleted = 0
    try:
        if not os.path.exists(temp_dir):
            logger.info(f"[OLD_CLEANUP] TEMP_DIR не существует: {temp_dir}")
            return
        for file_path in os.listdir(temp_dir):
            full_path = os.path.join(temp_dir, file_path)
            if not os.path.isfile(full_path):
                continue
            file_age = now - os.path.getmtime(full_path)
            if file_age > max_age_hours * 3600:
                try:
                    os.remove(full_path)
                    logger.info(f"[OLD_CLEANUP] Удалён старый файл: {full_path} (возраст: {file_age/3600:.2f} часов)")
                    deleted += 1
                except Exception as e:
                    logger.warning(f"[OLD_CLEANUP] Не удалось удалить файл {full_path}: {e}")
        if deleted > 0:
            logger.info(f"[OLD_CLEANUP] Всего удалено старых файлов: {deleted}")
    except Exception as e:
        logger.error(f"[OLD_CLEANUP] Ошибка при очистке старых файлов: {e}")

async def cleanup_temp_files(redis, interval=CLEANUP_INTERVAL):
    logger.info("Starting cleanup_temp_files loop...")
    while True:
        logger.debug("Attempting to get keys from Redis with pattern 'delete_after:*'")
        try:
            keys = await redis.keys('delete_after:*')
            if keys:
                logger.info(f"Found {len(keys)} keys with pattern 'delete_after:*'")
            else:
                logger.debug("No keys found with pattern 'delete_after:*'")
        except Exception as e:
            logger.error(f"Error getting keys from Redis: {e}")
            await asyncio.sleep(interval)
            continue
        
        processed_count = 0
        deleted_files_count = 0
        deleted_keys_count = 0
        errors_count = 0
        
        for key in keys:
            local_path = key.decode().replace('delete_after:', '')
            logger.debug(f"Processing key: {key.decode()}, path: {local_path}")
            processed_count += 1
            
            # Проверка наличия файла
            file_exists = os.path.exists(local_path)
            if not file_exists:
                logger.info(f"File {local_path} does not exist. Deleting key from Redis.")
                try:
                    await redis.delete(key)
                    deleted_keys_count += 1
                except Exception as e:
                    logger.error(f"Error deleting key {key.decode()} from Redis: {e}")
                    errors_count += 1
                continue
            
            # ИЗМЕНЕНО: Удаляем файл независимо от TTL
            logger.info(f"Attempting to remove file {local_path}")
            try:
                os.remove(local_path)
                logger.info(f'File удалён: {local_path}')
                deleted_files_count += 1
            except Exception as e:
                logger.warning(f'Не удалось удалить файл {local_path}: {e}')
                errors_count += 1
            
            # Удаляем ключ из Redis в любом случае
            try:
                await redis.delete(key)
                logger.info(f"Key {key.decode()} deleted from Redis after processing.")
                deleted_keys_count += 1
            except Exception as e:
                logger.error(f"Error deleting key {key.decode()} from Redis after processing: {e}")
                errors_count += 1
        
        # Сводная статистика за итерацию
        if processed_count > 0:
            logger.info(f"Cleanup stats: processed={processed_count}, deleted_files={deleted_files_count}, deleted_keys={deleted_keys_count}, errors={errors_count}")

        # Новый блок: удаление старых файлов
        await cleanup_old_temp_files()

        logger.debug(f"Sleeping for {interval} seconds...")
        await asyncio.sleep(interval)

async def main():
    logger.info("Cleanup worker starting...")
    try:
        redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        await redis.ping()
        logger.info("Successfully connected to Redis.")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis: {e}. Worker will not start.")
        return

    try:
        await cleanup_temp_files(redis)
    except Exception as e:
        logger.critical(f"Critical error in cleanup_temp_files: {e}", exc_info=True)
    finally:
        logger.info("Closing Redis connection...")
        await redis.close()
        logger.info("Redis connection closed. Worker shutting down.")

if __name__ == "__main__":
    logger.info("Cleanup worker script initializing...")
    asyncio.run(main()) 