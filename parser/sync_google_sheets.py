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
from telethon.errors import FloodWaitError, ChannelPrivateError, UserNotParticipantError, InviteHashInvalidError, InviteHashExpiredError
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
    """Читает список каналов из Google Sheets и возвращает каналы с метаданными"""
    if not GSPREAD_AVAILABLE:
        logger.error("gspread not available. Please install: pip install gspread google-auth")
        return None, []
    
    if not GOOGLE_SHEETS_ID:
        logger.error("GOOGLE_SHEETS_ID not set in environment variables")
        return None, []
    
    if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        logger.error(f"Google credentials file not found: {GOOGLE_CREDENTIALS_FILE}")
        return None, []
    
    try:
        logger.info(f"Connecting to Google Sheets: {GOOGLE_SHEETS_ID}")
        
        # Настройка авторизации
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=scope
        )
        
        client = gspread.authorize(creds)
        
        # Открываем таблицу
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_ID)
        
        # Показываем доступные листы для отладки
        available_sheets = [ws.title for ws in spreadsheet.worksheets()]
        logger.info(f"Available sheets in spreadsheet: {available_sheets}")
        logger.info(f"Looking for sheet: '{GOOGLE_SHEET_NAME}'")
        
        worksheet = spreadsheet.worksheet(GOOGLE_SHEET_NAME)
        
        # Читаем все значения из указанной колонки
        all_values = worksheet.col_values(GOOGLE_COLUMN)
        
        logger.info(f"Read {len(all_values)} rows from Google Sheets")
        
        # Фильтруем пустые строки и комментарии
        channels_data = []
        for row_idx, value in enumerate(all_values, start=1):
            value = value.strip()
            if value and not value.startswith('#'):
                # Извлекаем username из ссылки
                username = extract_username(value)
                if username:
                    channels_data.append({
                        'username': username,
                        'row': row_idx,
                        'original': value
                    })
                else:
                    logger.warning(f"Could not extract username from: {value}")
        
        logger.info(f"Extracted {len(channels_data)} valid channels from Google Sheets")
        return worksheet, channels_data
        
    except Exception as e:
        logger.error(f"Error reading from Google Sheets: {e}", exc_info=True)
        return None, []


async def get_telegram_entity(client, channel):
    """Получает Telegram entity для канала с retry логикой.
    Возвращает (entity, error_type), где error_type может быть:
    - None: успешно
    - 'not_found': канал не найден
    - 'private': приватный канал (нужна подписка)
    - 'invalid_invite': недействительная invite ссылка
    - 'flood_wait': превышен лимит запросов
    """
    max_retries = 3
    retry_delay = 2
    is_invite_link = channel.startswith('+')
    
    for attempt in range(max_retries):
        try:
            entity = await client.get_entity(channel)
            return entity, None
        except FloodWaitError as e:
            logger.warning(f"FloodWait for {channel}: {e.seconds}s, waiting...")
            await asyncio.sleep(e.seconds + 1)
            if attempt < max_retries - 1:
                logger.info(f"Retrying {channel} after FloodWait (attempt {attempt+2}/{max_retries})")
                continue
            else:
                logger.error(f"FloodWait happened on last retry for {channel}, skipping")
                return None, 'flood_wait'
        except (ChannelPrivateError, UserNotParticipantError) as e:
            logger.error(f"Private channel {channel}: {e}")
            return None, 'private'
        except (InviteHashInvalidError, InviteHashExpiredError) as e:
            logger.error(f"Invalid invite link {channel}: {e}")
            return None, 'invalid_invite'
        except ValueError as e:
            # "Cannot find any entity corresponding to" error
            error_msg = str(e).lower()
            if 'cannot find' in error_msg or 'no user has' in error_msg:
                if is_invite_link:
                    # Invite link not found обычно означает приватный канал
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    logger.error(f"Private invite channel {channel}: {e}")
                    return None, 'private'
                else:
                    logger.error(f"Channel not found {channel}: {e}")
                    return None, 'not_found'
            else:
                if attempt < max_retries - 1:
                    logger.warning(f"Error getting entity for {channel} (attempt {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to get entity for {channel} after {max_retries} attempts: {e}")
                    return None, 'not_found'
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error getting entity for {channel} (attempt {attempt+1}/{max_retries}): {e}")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Failed to get entity for {channel} after {max_retries} attempts: {e}")
                return None, 'not_found'
    
    return None, 'not_found'


def ensure_headers(worksheet):
    """Проверяет и создает заголовки для колонок ID и Status (если это первая строка)"""
    try:
        id_col = GOOGLE_COLUMN + 1
        status_col = GOOGLE_COLUMN + 2
        
        # Проверяем, есть ли заголовки
        try:
            current_id_header = worksheet.cell(1, id_col).value
            current_status_header = worksheet.cell(1, status_col).value
        except:
            current_id_header = None
            current_status_header = None
        
        # Устанавливаем заголовки, если их нет
        if not current_id_header or current_id_header.strip() == '':
            worksheet.update_cell(1, id_col, 'Channel ID')
            # Делаем заголовок жирным
            worksheet.format(f"{chr(64 + id_col)}1", {
                "textFormat": {"bold": True},
                "backgroundColor": {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
        
        if not current_status_header or current_status_header.strip() == '':
            worksheet.update_cell(1, status_col, 'Status')
            # Делаем заголовок жирным
            worksheet.format(f"{chr(64 + status_col)}1", {
                "textFormat": {"bold": True},
                "backgroundColor": {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
        
        logger.info("✓ Headers ensured in Google Sheets")
    except Exception as e:
        logger.warning(f"Could not ensure headers: {e}")


def update_google_sheets_row(worksheet, row, channel_id, status, error_type):
    """Обновляет строку в Google Sheets с ID канала и статусом, применяя цветовую раскраску.
    
    Колонки:
    - GOOGLE_COLUMN: URL канала (уже заполнено)
    - GOOGLE_COLUMN+1: ID канала
    - GOOGLE_COLUMN+2: Статус
    
    Цвета:
    - Красный: канал не найден / недействительная ссылка
    - Желтый: приватный канал
    - Зеленый: успешно (очищаем подсветку)
    """
    id_col = GOOGLE_COLUMN + 1
    status_col = GOOGLE_COLUMN + 2
    
    # Определяем цвет и текст статуса
    if error_type is None:
        # Успех - зеленый
        status_text = 'OK'
        bg_color = {'red': 0.85, 'green': 0.92, 'blue': 0.83}  # Светло-зеленый
    elif error_type == 'private':
        # Приватный - желтый
        status_text = 'PRIVATE'
        bg_color = {'red': 1.0, 'green': 0.95, 'blue': 0.8}  # Светло-желтый
    elif error_type == 'invalid_invite':
        # Недействительная ссылка - красный
        status_text = 'INVALID'
        bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}  # Светло-красный
    elif error_type == 'not_found':
        # Не найден - красный
        status_text = 'NOT FOUND'
        bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}  # Светло-красный
    else:  # flood_wait или другие
        status_text = 'ERROR'
        bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}  # Светло-красный
    
    try:
        # Обновляем ID канала (если есть)
        if channel_id:
            worksheet.update_cell(row, id_col, str(channel_id))
        else:
            worksheet.update_cell(row, id_col, '')
        
        # Обновляем статус
        worksheet.update_cell(row, status_col, status_text)
        
        # Применяем форматирование (цвет фона) к ячейке со статусом
        worksheet.format(f"{chr(64 + status_col)}{row}", {
            "backgroundColor": bg_color
        })
        
    except Exception as e:
        logger.error(f"Error updating Google Sheets row {row}: {e}")


async def sync_channels():
    """Основная функция синхронизации"""
    logger.info("=" * 60)
    logger.info("Starting Google Sheets → PostgreSQL sync")
    logger.info("=" * 60)
    
    # 1. Читаем каналы из Google Sheets
    worksheet, channels_data = get_channels_from_google_sheets()
    
    if not channels_data:
        logger.warning("No channels found in Google Sheets. Nothing to sync.")
        return
    
    logger.info(f"Found {len(channels_data)} channels to sync")
    
    # Создаем заголовки в Google Sheets (если нужно)
    if worksheet:
        ensure_headers(worksheet)
    
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
        private_count = 0
        
        for i, channel_data in enumerate(channels_data, 1):
            channel = channel_data['username']
            row = channel_data['row']
            
            try:
                logger.info(f"[{i}/{len(channels_data)}] Processing: {channel} (row {row})")
                
                # Получаем entity
                entity, error_type = await get_telegram_entity(client, channel)
                
                if entity is None:
                    # Обновляем Google Sheets с ошибкой
                    if worksheet:
                        update_google_sheets_row(worksheet, row, None, None, error_type)
                    
                    if error_type == 'private':
                        logger.warning(f"🟡 {channel} → PRIVATE")
                        private_count += 1
                    else:
                        logger.error(f"🔴 {channel} → {error_type.upper()}")
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
                
                # Обновляем Google Sheets с успехом
                if worksheet:
                    update_google_sheets_row(worksheet, row, entity_id, 'OK', None)
                
                logger.info(f"🟢 {channel} → {entity_id}")
                success_count += 1
                
                # Пауза между запросами для предотвращения FloodWait
                # 1.5 секунды - безопасный интервал для Telegram API
                await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.error(f"✗ {channel}: {e}")
                if worksheet:
                    update_google_sheets_row(worksheet, row, None, None, 'not_found')
                error_count += 1
        
        # 5. Статистика
        logger.info("=" * 60)
        logger.info(f"Sync completed:")
        logger.info(f"  🟢 Success: {success_count}")
        logger.info(f"  🟡 Private: {private_count}")
        logger.info(f"  🔴 Errors: {error_count}")
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

