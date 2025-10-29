#!/usr/bin/env python3
"""
Скрипт синхронизации каналов из Google Sheets в PostgreSQL.
Запускается периодически (например, каждые 5 минут) для обновления списка каналов.
"""

import asyncio
import asyncpg
import pickle
import base64
import os
import logging
import time
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from config_loader import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION

# Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logging.warning("gspread not installed. Install with: pip install gspread google-auth")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("sync_google_sheets")

# Конфигурация из переменных окружения
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@db:5432/filter")
GOOGLE_SHEETS_ID = os.environ.get("GOOGLE_SHEETS_ID")
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Sheet1")  # Название листа
GOOGLE_COLUMN = int(os.environ.get("GOOGLE_COLUMN", "1"))  # Номер колонки (1 = первая)


def extract_username(url):
    """Извлекает username из различных форматов ссылок Telegram"""
    if not url:
        return None
    
    url = url.strip()
    
    # https://t.me/channelname
    if url.startswith('https://t.me/'):
        return url.replace('https://t.me/', '').split('?')[0].split('/')[0]
    # @channelname
    elif url.startswith('@'):
        return url[1:]
    # channelname
    elif url.startswith('http'):
        # Игнорируем другие HTTP ссылки
        return None
    else:
        # Предполагаем, что это уже username
        return url


def get_channels_from_google_sheets():
    """Читает список каналов из Google Sheets"""
    if not GSPREAD_AVAILABLE:
        logger.error("gspread not available. Please install: pip install gspread google-auth")
        return []
    
    if not GOOGLE_SHEETS_ID:
        logger.error("GOOGLE_SHEETS_ID not set in environment variables")
        return []
    
    if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        logger.error(f"Google credentials file not found: {GOOGLE_CREDENTIALS_FILE}")
        return []
    
    try:
        logger.info(f"Connecting to Google Sheets: {GOOGLE_SHEETS_ID}")
        
        # Настройка авторизации
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=scope
        )
        
        client = gspread.authorize(creds)
        
        # Открываем таблицу
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_ID)
        worksheet = spreadsheet.worksheet(GOOGLE_SHEET_NAME)
        
        # Читаем все значения из указанной колонки
        all_values = worksheet.col_values(GOOGLE_COLUMN)
        
        logger.info(f"Read {len(all_values)} rows from Google Sheets")
        
        # Фильтруем пустые строки и комментарии
        channels = []
        for value in all_values:
            value = value.strip()
            if value and not value.startswith('#'):
                # Извлекаем username из ссылки
                username = extract_username(value)
                if username:
                    channels.append(username)
                else:
                    logger.warning(f"Could not extract username from: {value}")
        
        logger.info(f"Extracted {len(channels)} valid channels from Google Sheets")
        return channels
        
    except Exception as e:
        logger.error(f"Error reading from Google Sheets: {e}", exc_info=True)
        return []


async def get_telegram_entity(client, channel):
    """Получает Telegram entity для канала с retry логикой"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            entity = await client.get_entity(channel)
            return entity
        except FloodWaitError as e:
            logger.warning(f"FloodWait for {channel}: {e.seconds}s, waiting...")
            await asyncio.sleep(e.seconds + 1)
            # Продолжаем попытки после FloodWait
            if attempt < max_retries - 1:
                logger.info(f"Retrying {channel} after FloodWait (attempt {attempt+2}/{max_retries})")
                continue
            else:
                logger.error(f"FloodWait happened on last retry for {channel}, skipping")
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error getting entity for {channel} (attempt {attempt+1}/{max_retries}): {e}")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Failed to get entity for {channel} after {max_retries} attempts: {e}")
                return None
    
    return None


async def sync_channels():
    """Основная функция синхронизации"""
    logger.info("=" * 60)
    logger.info("Starting Google Sheets → PostgreSQL sync")
    logger.info("=" * 60)
    
    # 1. Читаем каналы из Google Sheets
    channels = get_channels_from_google_sheets()
    
    if not channels:
        logger.warning("No channels found in Google Sheets. Nothing to sync.")
        return
    
    logger.info(f"Found {len(channels)} channels to sync")
    
    # 2. Подключаемся к Telegram
    logger.info("Connecting to Telegram...")
    client = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error("Telegram client is not authorized! Please run parser first to authorize.")
            return
        
        logger.info("✓ Telegram connected")
        
        # 3. Подключаемся к PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        conn = await asyncpg.connect(PG_DSN)
        logger.info("✓ PostgreSQL connected")
        
        # 4. Синхронизируем каналы
        success_count = 0
        error_count = 0
        
        for i, channel in enumerate(channels, 1):
            try:
                logger.info(f"[{i}/{len(channels)}] Processing: {channel}")
                
                # Получаем entity
                entity = await get_telegram_entity(client, channel)
                
                if entity is None:
                    error_count += 1
                    continue
                
                # Сериализуем entity
                entity_data = pickle.dumps(entity)
                entity_id = getattr(entity, 'id', None)
                entity_username = getattr(entity, 'username', None) or channel
                
                # Сохраняем в PostgreSQL
                await conn.execute("""
                    INSERT INTO telegram_entities (id, username, entity_data, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE
                    SET username = EXCLUDED.username,
                        entity_data = EXCLUDED.entity_data,
                        updated_at = EXCLUDED.updated_at
                """, entity_id, entity_username, entity_data, datetime.utcnow())
                
                logger.info(f"✓ {channel} → {entity_id}")
                success_count += 1
                
                # Пауза между запросами для предотвращения FloodWait
                # 1.5 секунды - безопасный интервал для Telegram API
                await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.error(f"✗ {channel}: {e}")
                error_count += 1
        
        # 5. Статистика
        logger.info("=" * 60)
        logger.info(f"Sync completed: ✓ {success_count} success, ✗ {error_count} errors")
        logger.info("=" * 60)
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"Critical error during sync: {e}", exc_info=True)
    finally:
        await client.disconnect()


async def main():
    """Точка входа"""
    try:
        await sync_channels()
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == '__main__':
    asyncio.run(main())

