import os
import asyncio
import asyncpg
import pickle
import base64
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError
from redis import asyncio as aioredis
import traceback

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('channel_sync')

# Конфигурация
GOOGLE_SHEETS_ID = os.environ.get('GOOGLE_SHEETS_ID')
GOOGLE_SHEET_NAME = os.environ.get('GOOGLE_SHEET_NAME', 'Sheet1')
GOOGLE_COLUMN = int(os.environ.get('GOOGLE_COLUMN', '1'))
SYNC_INTERVAL_MINUTES = int(os.environ.get('SYNC_INTERVAL_MINUTES', '5'))

# Telegram
TELEGRAM_API_ID = int(os.environ.get('TELEGRAM_API_ID'))
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')
TELEGRAM_SESSION = '/app/session/parser.session'  # Используем ту же сессию, что и парсер

# PostgreSQL
PG_DSN = os.environ.get("PG_DSN") or "postgresql://postgres:postgres@db:5432/filter"

# Redis
REDIS_URL = os.environ.get('REDIS_URL', 'redis://redis:6379/0')

# Google Service Account
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON', '/app/config/google_service_account.json')


class ChannelSyncService:
    def __init__(self):
        self.sheets_service = None
        self.telegram_client = None
        self.pg_pool = None
        self.redis = None
        self.is_running = False
        self.notified_errors = set()  # Чтобы не спамить одинаковыми ошибками
        
    async def init(self):
        """Инициализация всех сервисов"""
        try:
            # Redis для уведомлений
            self.redis = await aioredis.from_url(REDIS_URL)
            logger.info("Redis инициализирован")
            
            # Google Sheets
            self._init_google_sheets()
            
            # PostgreSQL
            await self._init_postgres()
            
            # Telegram - ждем сессию от парсера
            await self._init_telegram()
            logger.info("Все сервисы инициализированы")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации: {e}")
            # Если Google Sheets не настроен - даем понятную ошибку
            if "GOOGLE_SERVICE_ACCOUNT_JSON" in str(e) or "invalid_grant" in str(e):
                await self.send_admin_notification(
                    "❌ Ошибка настройки Google Sheets. Проверьте:\n"
                    "1. Файл google_service_account.json в корне проекта\n"
                    "2. Доступ Service Account к таблице\n"
                    "3. GOOGLE_SHEETS_ID в .env"
                )
            raise
        
    def _init_google_sheets(self):
        """Инициализация Google Sheets API"""
        try:
            # Если есть файл с credentials
            if os.path.exists(GOOGLE_SERVICE_ACCOUNT_JSON):
                creds = service_account.Credentials.from_service_account_file(
                    GOOGLE_SERVICE_ACCOUNT_JSON,
                    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
                )
            else:
                # Если credentials в переменной окружения
                service_account_info = json.loads(os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY', '{}'))
                creds = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
                )
            
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            logger.info("Google Sheets API инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets API: {e}")
            raise
            
    async def _wait_for_session(self):
        """Ждем пока парсер создаст сессию"""
        session_file = TELEGRAM_SESSION + '.session'
        
        logger.info(f"Проверяем наличие файла сессии: {session_file}")
        
        notification_sent = False
        check_count = 0
        
        while not os.path.exists(session_file):
            check_count += 1
            
            # Отправляем уведомление только один раз
            if not notification_sent:
                logger.info(f"Файл сессии {session_file} не найден. Ожидание создания сессии парсером...")
                await self.send_admin_notification(
                    "⏳ Channel Sync ожидает авторизации парсера в Telegram. Это нормально при первом запуске."
                )
                notification_sent = True
            else:
                # Каждые 10 проверок (5 минут) выводим сообщение в лог
                if check_count % 10 == 0:
                    logger.info(f"Все еще жду файл сессии... (проверка #{check_count})")
            
            # Ждем 30 секунд перед следующей проверкой
            await asyncio.sleep(30)
            
        logger.info(f"Файл сессии найден: {session_file}")
        await self.send_admin_notification("✅ Channel Sync обнаружил Telegram сессию, начинаю работу!")
        return True

    async def _init_telegram(self):
        """Инициализация Telegram клиента"""
        try:
            # Ждем пока появится файл сессии от парсера
            await self._wait_for_session()
            
            self.telegram_client = TelegramClient(
                TELEGRAM_SESSION,
                TELEGRAM_API_ID,
                TELEGRAM_API_HASH
            )
            
            logger.info("Подключение к Telegram с существующей сессией...")
            await self.telegram_client.connect()
            
            if not await self.telegram_client.is_user_authorized():
                error_msg = "Сессия существует, но пользователь не авторизован. Возможно сессия повреждена."
                logger.error(error_msg)
                await self.send_admin_notification(f"❌ {error_msg}")
                raise RuntimeError(error_msg)
                
            logger.info("Успешно подключились к Telegram с существующей сессией")
                
        except Exception as e:
            logger.critical(f"Критическая ошибка при подключении к Telegram: {e}\n{traceback.format_exc()}")
            raise
        
    async def _init_postgres(self):
        """Инициализация PostgreSQL"""
        self.pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=5)
        
        # Создаем таблицу если её нет
        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS telegram_entities (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(128) UNIQUE,
                    entity_data BYTEA NOT NULL,
                    updated_at TIMESTAMP DEFAULT now()
                );
            """)
        logger.info("PostgreSQL инициализирован")
        
    def extract_username(self, url: str) -> str:
        """Извлечение username из URL"""
        if url.startswith('https://t.me/'):
            part = url[len('https://t.me/'):]
            if part.startswith('+'):
                return url  # invite link
            return part.split('/')[0]
        return url
        
    async def get_channels_from_sheet(self) -> List[str]:
        """Получение списка каналов из Google Sheets"""
        try:
            # Формируем диапазон для чтения (например, A:A для первого столбца)
            column_letter = chr(ord('A') + GOOGLE_COLUMN - 1)
            range_name = f'{GOOGLE_SHEET_NAME}!{column_letter}:{column_letter}'
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEETS_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            channels = [row[0].strip() for row in values if row and row[0].strip()]
            
            logger.info(f"Получено {len(channels)} каналов из Google Sheets")
            return channels
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из Google Sheets: {e}")
            await self.send_admin_notification(f"❌ Ошибка чтения Google Sheets: {e}")
            return []
            
    async def resolve_entity(self, url: str) -> Optional[Dict]:
        """Резолвинг Telegram сущности"""
        username = self.extract_username(url)
        try:
            entity = await self.telegram_client.get_entity(username)
            entity_bytes = pickle.dumps(entity)
            entity_b64 = base64.b64encode(entity_bytes).decode('ascii')
            entity_id = getattr(entity, 'id', None)
            
            return {
                'username': username,
                'id': entity_id,
                'entity_data': entity_bytes,
                'updated_at': datetime.utcnow()
            }
            
        except FloodWaitError as e:
            logger.warning(f"FloodWait для {username}: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
            return await self.resolve_entity(url)  # Повторяем после ожидания
            
        except (ChannelPrivateError, UsernameNotOccupiedError) as e:
            error_msg = f"Канал недоступен: {username} - {type(e).__name__}"
            logger.error(error_msg)
            
            # Отправляем уведомление только если раньше не отправляли
            error_key = f"{username}:{type(e).__name__}"
            if error_key not in self.notified_errors:
                await self.send_admin_notification(f"❌ {error_msg}")
                self.notified_errors.add(error_key)
            
            return None
            
        except Exception as e:
            error_msg = f"Ошибка резолвинга {username}: {e}"
            logger.error(error_msg)
            await self.send_admin_notification(f"❌ {error_msg}")
            return None
            
    async def save_entity(self, entity_data: Dict):
        """Сохранение сущности в PostgreSQL"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO telegram_entities (id, username, entity_data, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE
                SET username = EXCLUDED.username,
                    entity_data = EXCLUDED.entity_data,
                    updated_at = EXCLUDED.updated_at
            """, entity_data['id'], entity_data['username'], 
                entity_data['entity_data'], entity_data['updated_at'])
                
        logger.info(f"Сохранена сущность: {entity_data['username']} ({entity_data['id']})")
        
    async def get_existing_channels(self) -> set:
        """Получение существующих каналов из БД"""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT username FROM telegram_entities")
            return {row['username'] for row in rows}
            
    async def sync_channels(self):
        """Основная функция синхронизации"""
        logger.info("Начинаем синхронизацию каналов...")
        
        try:
            # Получаем каналы из Google Sheets
            sheet_channels = await self.get_channels_from_sheet()
            if not sheet_channels:
                logger.warning("Нет каналов для синхронизации")
                return
                
            # Получаем существующие каналы
            existing_channels = await self.get_existing_channels()
            
            # Находим новые каналы
            new_channels = []
            for url in sheet_channels:
                username = self.extract_username(url)
                if username not in existing_channels:
                    new_channels.append(url)
                    
            if new_channels:
                logger.info(f"Найдено {len(new_channels)} новых каналов для добавления")
                
                # Резолвим и сохраняем новые каналы
                successful_count = 0
                failed_count = 0
                
                for i, url in enumerate(new_channels):
                    logger.info(f"Обработка {i+1}/{len(new_channels)}: {url}")
                    
                    entity_data = await self.resolve_entity(url)
                    if entity_data:
                        await self.save_entity(entity_data)
                        successful_count += 1
                    else:
                        failed_count += 1
                        
                    # Пауза между запросами
                    await asyncio.sleep(1.5)
                
                # Формируем итоговое сообщение
                result_msg = f"✅ Синхронизация завершена!\n"
                result_msg += f"📊 Из {len(new_channels)} новых каналов:\n"
                result_msg += f"✅ Успешно добавлено: {successful_count}\n"
                if failed_count > 0:
                    result_msg += f"❌ Не удалось добавить: {failed_count}"
                    
                await self.send_admin_notification(result_msg)
            else:
                logger.info("Новых каналов не найдено")
                
        except Exception as e:
            error_msg = f"Критическая ошибка синхронизации: {e}"
            logger.error(error_msg)
            await self.send_admin_notification(f"🔥 {error_msg}")
            
    async def send_admin_notification(self, message: str):
        """Отправка уведомления администратору через Redis"""
        try:
            notification = {
                'type': 'channel_sync',
                'message': message,
                'timestamp': datetime.utcnow().isoformat()
            }
            await self.redis.lpush('admin_notifications', json.dumps(notification))
            logger.debug(f"Уведомление отправлено: {message}")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            
    async def run_periodic_sync(self):
        """Запуск периодической синхронизации"""
        self.is_running = True
        
        while self.is_running:
            try:
                await self.sync_channels()
            except Exception as e:
                logger.error(f"Ошибка в цикле синхронизации: {e}")
                
            # Ждем указанный интервал
            logger.info(f"Следующая синхронизация через {SYNC_INTERVAL_MINUTES} минут")
            await asyncio.sleep(SYNC_INTERVAL_MINUTES * 60)
            
    async def stop(self):
        """Остановка сервиса"""
        self.is_running = False
        
        if self.telegram_client:
            await self.telegram_client.disconnect()
            
        if self.pg_pool:
            await self.pg_pool.close()
            
        if self.redis:
            await self.redis.close()
            
        logger.info("Сервис остановлен")


async def main():
    """Точка входа"""
    sync_service = ChannelSyncService()
    
    try:
        await sync_service.init()
        logger.info("Channel Sync Service запущен")
        
        # Запускаем синхронизацию сразу при старте
        await sync_service.sync_channels()
        
        # Запускаем периодическую синхронизацию
        await sync_service.run_periodic_sync()
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        await sync_service.stop()


if __name__ == '__main__':
    asyncio.run(main())
