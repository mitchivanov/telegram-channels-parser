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
    """Получает Telegram entity для канала БЕЗ retry для известных ошибок.
    Возвращает (entity, error_type), где error_type может быть:
    - None: успешно
    - 'not_found': канал не найден
    - 'private': приватный канал (нужна подписка)
    - 'invalid_invite': недействительная invite ссылка
    - 'flood_wait': превышен лимит запросов
    """
    max_retries = 2  # Уменьшено до 2 попыток (только для неопределенных ошибок)
    retry_delay = 5  # Увеличено до 5 секунд между retry
    is_invite_link = channel.startswith('+')
    
    for attempt in range(max_retries):
        try:
            entity = await client.get_entity(channel)
            return entity, None
            
        except FloodWaitError as e:
            # FloodWait - КРИТИЧЕСКАЯ ошибка, останавливаем sync
            logger.error(f"⛔ FLOOD WAIT for {channel}: {e.seconds}s - STOPPING SYNC TO AVOID BAN")
            await asyncio.sleep(e.seconds + 1)
            return None, 'flood_wait'
            
        except (ChannelPrivateError, UserNotParticipantError) as e:
            # Приватный канал - НЕ делаем retry (не поможет)
            logger.warning(f"🟡 Private channel {channel}: {e}")
            return None, 'private'
            
        except (InviteHashInvalidError, InviteHashExpiredError) as e:
            # Недействительная ссылка - НЕ делаем retry (не поможет)
            logger.warning(f"🔴 Invalid invite link {channel}: {e}")
            return None, 'invalid_invite'
            
        except ValueError as e:
            # "Cannot find any entity" или "No user has" - НЕ делаем retry
            error_msg = str(e).lower()
            if 'cannot find' in error_msg or 'no user has' in error_msg or 'nobody is using' in error_msg:
                if is_invite_link:
                    logger.warning(f"🟡 Private/invalid invite {channel}: {e}")
                    return None, 'private'
                else:
                    logger.warning(f"🔴 Channel not found {channel}: {e}")
                    return None, 'not_found'
            else:
                # Неизвестная ValueError - retry
                if attempt < max_retries - 1:
                    logger.warning(f"Unexpected ValueError for {channel} (attempt {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Failed after {max_retries} attempts for {channel}: {e}")
                    return None, 'not_found'
                    
        except Exception as e:
            # Неизвестная ошибка - retry
            if attempt < max_retries - 1:
                logger.warning(f"Unexpected error for {channel} (attempt {attempt+1}/{max_retries}): {e}")
                await asyncio.sleep(retry_delay)
                continue
            else:
                logger.error(f"Failed after {max_retries} attempts for {channel}: {e}")
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


def batch_update_google_sheets(worksheet, updates_batch):
    """Батчевое обновление Google Sheets для минимизации API calls.
    
    updates_batch: список словарей с ключами:
        - row: номер строки
        - channel_id: ID канала или None
        - error_type: тип ошибки или None
    """
    if not updates_batch:
        return
    
    id_col = GOOGLE_COLUMN + 1
    status_col = GOOGLE_COLUMN + 2
    
    # Подготавливаем данные для batch update
    value_ranges = []
    format_requests = []
    
    for update in updates_batch:
        row = update['row']
        channel_id = update.get('channel_id')
        error_type = update.get('error_type')
        
        # Определяем цвет и текст статуса
        if error_type is None:
            status_text = 'OK'
            bg_color = {'red': 0.85, 'green': 0.92, 'blue': 0.83}
        elif error_type == 'private':
            status_text = 'PRIVATE'
            bg_color = {'red': 1.0, 'green': 0.95, 'blue': 0.8}
        elif error_type == 'invalid_invite':
            status_text = 'INVALID'
            bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}
        elif error_type == 'not_found':
            status_text = 'NOT FOUND'
            bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}
        elif error_type == 'flood_wait':
            status_text = 'FLOOD WAIT'
            bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}
        else:
            status_text = 'ERROR'
            bg_color = {'red': 0.96, 'green': 0.8, 'blue': 0.8}
        
        # Добавляем значения для обновления
        id_value = str(channel_id) if channel_id else ''
        value_ranges.append({
            'range': f'{chr(64 + id_col)}{row}:{chr(64 + status_col)}{row}',
            'values': [[id_value, status_text]]
        })
        
        # Добавляем форматирование для статуса
        format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": status_col - 1,
                    "endColumnIndex": status_col
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": bg_color
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
    
    try:
        # Batch update значений (1 API call)
        if value_ranges:
            worksheet.spreadsheet.values_batch_update({
                'valueInputOption': 'RAW',
                'data': value_ranges
            })
        
        # Batch update форматирования (1 API call)
        if format_requests:
            worksheet.spreadsheet.batch_update({
                'requests': format_requests
            })
        
        logger.info(f"✓ Batch updated {len(updates_batch)} rows in Google Sheets (2 API calls)")
    except Exception as e:
        logger.error(f"Error in batch update Google Sheets: {e}")


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
        batch_updates = []  # Батч для Google Sheets
        BATCH_SIZE = 20  # Обновляем Google Sheets каждые 20 каналов
        DELAY_BETWEEN_CHANNELS = 6  # 6 секунд = 10 каналов/минуту
        
        for i, channel_data in enumerate(channels_data, 1):
            channel = channel_data['username']
            row = channel_data['row']
            
            try:
                logger.info(f"[{i}/{len(channels_data)}] Processing: {channel} (row {row})")
                
                # Получаем entity
                entity, error_type = await get_telegram_entity(client, channel)
                
                if entity is None:
                    # Добавляем в батч для обновления
                    batch_updates.append({
                        'row': row,
                        'channel_id': None,
                        'error_type': error_type
                    })
                    
                    if error_type == 'private':
                        logger.warning(f"🟡 {channel} → PRIVATE")
                        private_count += 1
                    elif error_type == 'flood_wait':
                        logger.error(f"⛔ {channel} → FLOOD WAIT - STOPPING SYNC")
                        error_count += 1
                        # Отправляем накопленный батч и выходим
                        if worksheet and batch_updates:
                            batch_update_google_sheets(worksheet, batch_updates)
                        break
                    else:
                        logger.error(f"🔴 {channel} → {error_type.upper()}")
                        error_count += 1
                    
                    # Отправляем батч если достигли размера
                    if len(batch_updates) >= BATCH_SIZE:
                        if worksheet:
                            batch_update_google_sheets(worksheet, batch_updates)
                        batch_updates = []
                    
                    # Пауза между запросами
                    await asyncio.sleep(DELAY_BETWEEN_CHANNELS)
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
                
                # Добавляем в батч для обновления Google Sheets
                batch_updates.append({
                    'row': row,
                    'channel_id': entity_id,
                    'error_type': None
                })
                
                logger.info(f"🟢 {channel} → {entity_id}")
                success_count += 1
                
                # Отправляем батч если достигли размера
                if len(batch_updates) >= BATCH_SIZE:
                    if worksheet:
                        batch_update_google_sheets(worksheet, batch_updates)
                    batch_updates = []
                
                # Пауза между запросами: 6 секунд = 10 каналов/минуту
                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)
                
            except Exception as e:
                logger.error(f"✗ {channel}: {e}")
                batch_updates.append({
                    'row': row,
                    'channel_id': None,
                    'error_type': 'not_found'
                })
                error_count += 1
                
                # Отправляем батч если достигли размера
                if len(batch_updates) >= BATCH_SIZE:
                    if worksheet:
                        batch_update_google_sheets(worksheet, batch_updates)
                    batch_updates = []
                
                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)
        
        # Отправляем оставшийся батч
        if worksheet and batch_updates:
            batch_update_google_sheets(worksheet, batch_updates)
        
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

