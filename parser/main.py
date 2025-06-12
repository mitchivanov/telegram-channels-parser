import asyncio
import os
import tempfile
from postgres_state import PostgresStateManager
from db_models import init_db_schema
from rate_limiter import RateLimiter
from floodwait_manager import FloodWaitManager
from kafka_producer import KafkaProducerAsync
from activity_monitor import ActivityMonitor
from utils import filter_message, async_backoff
from config_loader import REDIS_URL, TELEGRAM_2FA_PASSWORD, TELEGRAM_PHONE, TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from config_loader import KAFKA_RAW_TOPIC
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError, UserNotParticipantError, ChannelPrivateError, InviteHashInvalidError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, MessageMediaContact, MessageMediaGeo, MessageMediaPoll, User, Channel, Chat
import logging
from datetime import datetime, timezone, timedelta
from redis.asyncio import Redis
from minio import Minio
from minio.error import S3Error
import json
import traceback
from task_queue_manager import TaskQueueManager
import secrets
import glob
import time
import re
from cachetools import LRUCache
from telethon.utils import pack_bot_file_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

class GlobalFloodWaitManager:
    def __init__(self):
        self.until = None
        self.lock = asyncio.Lock()

    async def set(self, seconds):
        async with self.lock:
            self.until = datetime.utcnow() + timedelta(seconds=seconds)

    async def is_active(self):
        async with self.lock:
            if self.until and self.until > datetime.utcnow():
                return True
            return False

    async def wait_until_free(self):
        while True:
            async with self.lock:
                if not self.until or self.until <= datetime.utcnow():
                    return
                left = (self.until - datetime.utcnow()).total_seconds()
            await asyncio.sleep(min(left, 5))

class ParserCore:
    def __init__(self, state_manager):
        self.state = state_manager
        self.rate_limiter = RateLimiter(requests_per_minute=40)
        self.floodwait = FloodWaitManager()
        self.global_floodwait = GlobalFloodWaitManager()
        self.kafka = KafkaProducerAsync({
            'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'topic': KAFKA_RAW_TOPIC
        })
        self.monitor = ActivityMonitor()
        self.redis = Redis.from_url(REDIS_URL)
        self.client = TelegramClient(
            TELEGRAM_SESSION,
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH
        )
        self.logger = logging.getLogger("parser.core")
        self.is_running = False
        self.max_concurrent = 5
        self.include = []
        self.exclude = []
        self.temp_dir = "/app/temp"
        os.makedirs(self.temp_dir, exist_ok=True)
        self.channels = []
        self.task_queue = TaskQueueManager(num_workers=5)
        self.entity_cache = LRUCache(maxsize=1000)  # LRU in-memory cache
        self.entity_queue = asyncio.Queue()
        self.entity_futures = {}
        self.entity_worker_started = False
        self.entity_redis_ttl = 7 * 24 * 60 * 60  # 7 дней
        # Черный список каналов (не обрабатываем)
        self.blacklisted_channels = [
            "razdazhawbozon2",
            "Childrens_cashback", 
            "razdachaottani"
        ]
        self.logger.info(f"Инициализирован черный список каналов: {self.blacklisted_channels}")

    async def cleanup_old_temp_files(self, max_age_hours=6):
        """Удаляет временные файлы старше указанного возраста (в часах)"""
        self.logger.info(f"[CLEANUP] Запуск очистки временных файлов старше {max_age_hours} часов")
        try:
            now = time.time()
            count = 0
            for file_path in glob.glob(os.path.join(self.temp_dir, "*")):
                try:
                    file_age = now - os.path.getmtime(file_path)
                    if file_age > max_age_hours * 3600:  # Переводим часы в секунды
                        os.remove(file_path)
                        count += 1
                        self.logger.debug(f"[CLEANUP] Удален старый файл: {file_path} (возраст: {file_age/3600:.2f} часов)")
                except Exception as e:
                    self.logger.warning(f"[CLEANUP] Ошибка при проверке/удалении файла {file_path}: {e}")
            self.logger.info(f"[CLEANUP] Удалено {count} старых временных файлов")
        except Exception as e:
            self.logger.error(f"[CLEANUP] Ошибка при очистке временных файлов: {e}")

    async def load_channels_from_db(self):
        self.channels = await self.state.get_all_usernames()
        self.logger.info(f"Загружено {len(self.channels)} каналов из базы telegram_entities")

    async def get_entity_cached(self, channel):
        # 1. In-memory cache
        if channel in self.entity_cache:
            return self.entity_cache[channel]
        # 2. Только Postgres
        entity = await self.state.get_entity_by_username(channel)
        if entity:
            self.entity_cache[channel] = entity
            return entity
        self.logger.error(f"[ENTITY][PG] Не удалось получить entity для {channel} — канал будет пропущен!")
        return None

    async def _get_code_from_admin(self, code_type):
        try:
            # Для текстовых ключей в Redis используем кодировку utf-8
            await self.redis.publish('tg:code:request', code_type.encode('utf-8'))
            self.logger.info(f"Ожидание {code_type} от админа через Redis (ключ: tg:code:response)...")
            
            # Проверяем начальное значение ключа
            initial_code = await self.redis.get('tg:code:response')
            if initial_code:
                initial_code_str = initial_code.decode('utf-8') if initial_code else None
                self.logger.info(f"Найден существующий код в Redis: {initial_code_str}, удаляем его перед ожиданием нового")
                await self.redis.delete('tg:code:response')
            
            # Ожидаем код
            for attempt in range(60):  # 5 минут (60 * 5 секунд)
                self.logger.info(f"Проверка кода, попытка {attempt+1}/60...")
                code = await self.redis.get('tg:code:response')
                if code:
                    code_str = code.decode('utf-8')
                    await self.redis.delete('tg:code:response')
                    self.logger.info(f"Получен {code_type} от админа: {code_str}")
                    return code_str.strip()
                await asyncio.sleep(5)
            
            self.logger.error(f"Таймаут ожидания {code_type} от админа")
            raise RuntimeError(f"Не получен {code_type} от админа в течение 5 минут")
        except Exception as e:
            self.logger.error(f"Ошибка при получении кода от админа: {e}\n{traceback.format_exc()}")
            raise

    async def _start_telethon(self):
        try:
            self.logger.info("Начало подключения к Telegram...")
            await self.client.connect()
            self.logger.info("Клиент Telegram подключен.")
            
            if not await self.client.is_user_authorized():
                self.logger.info("Пользователь не авторизован, запрос кода подтверждения.")
                try:
                    await self.client.send_code_request(TELEGRAM_PHONE)
                    auth_retries = 3
                    
                    while auth_retries > 0:
                        try:
                            code = await self._get_code_from_admin('sms')
                            self.logger.info("Получен код подтверждения, попытка входа.")
                            await self.client.sign_in(TELEGRAM_PHONE, code)
                            break
                        except PhoneCodeInvalidError:
                            auth_retries -= 1
                            self.logger.error(f"Введён неверный код подтверждения. Осталось попыток: {auth_retries}")
                            if auth_retries <= 0:
                                raise RuntimeError("Превышено количество попыток ввода кода. Авторизация не удалась.")
                            
                    if await self.client.is_user_authorized():
                        self.logger.info("Пользователь успешно авторизован.")
                        return
                        
                    try:
                        await self.client.sign_in(TELEGRAM_PHONE, code)
                    except SessionPasswordNeededError:
                        self.logger.info("Требуется пароль 2FA.")
                        for attempt in range(3):
                            try:
                                pw = await self._get_code_from_admin('2fa')
                                await self.client.sign_in(password=pw)
                                self.logger.info("Вход с 2FA выполнен.")
                                return
                            except Exception as e:
                                self.logger.error(f"Ошибка 2FA аутентификации (попытка {attempt+1}/3): {e}")
                                if attempt == 2:  # последняя попытка
                                    raise
                except Exception as e:
                    self.logger.critical(f"Ошибка авторизации: {e}\n{traceback.format_exc()}")
                    raise RuntimeError(f"Не удалось авторизоваться в Telegram: {e}")
            else:
                self.logger.info("Пользователь уже авторизован в Telegram.")
        except Exception as e:
            self.logger.critical(f"Критическая ошибка при подключении к Telegram: {e}\n{traceback.format_exc()}")
            raise

    async def start(self):
        try:
            self.logger.info("Starting parser initialization...")
            session_dir = os.path.dirname(TELEGRAM_SESSION)
            if session_dir and not os.path.exists(session_dir):
                os.makedirs(session_dir)
                self.logger.info(f"Created session directory: {session_dir}")

            # Очистка старых временных файлов при запуске
            await self.cleanup_old_temp_files(max_age_hours=6)

            await self._start_telethon()
            await self.kafka.start()
            await self.task_queue.start()
            await self.load_channels_from_db()
            # Загружаем черный список каналов из Redis
            await self.load_blacklisted_channels()
            self.is_running = True
            self.logger.info(f"Parser started with {len(self.channels)} channels configured (из базы)")
            self.logger.info(f"Черный список содержит {len(self.blacklisted_channels)} каналов")

            # Запускаем периодическую очистку временных файлов (каждые 30 минут)
            asyncio.create_task(self._start_periodic_cleanup(interval_minutes=30))

            if not self.channels:
                self.logger.warning("No channels configured! Please check telegram_entities table in Postgres")
                self.logger.info("Parser will continue running but won't process any channels")

            await self.monitor.start_monitoring()
            await self.run()
        except Exception as e:
            self.logger.critical(f"Critical error during parser startup: {e}\n{traceback.format_exc()}")
            await self.stop()
            raise

    async def _start_periodic_cleanup(self, interval_minutes=30):
        """Запускает периодическую очистку временных файлов"""
        self.logger.info(f"[CLEANUP] Запуск периодической очистки временных файлов каждые {interval_minutes} минут")
        while self.is_running:
            await asyncio.sleep(interval_minutes * 60)
            if self.is_running:  # Проверяем снова, т.к. могло измениться за время сна
                await self.cleanup_old_temp_files(max_age_hours=6)

    async def stop(self):
        self.is_running = False
        await self.task_queue.stop()
        await self.kafka.stop()
        await self.client.disconnect()
        await self.monitor.stop_monitoring()
        self.logger.info("Parser stopped")
        try:
            for f in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, f))
            os.rmdir(self.temp_dir)
        except Exception as e:
            self.logger.warning(f"Ошибка при очистке временных файлов: {e}")

    async def run(self):
        semaphore = asyncio.Semaphore(self.max_concurrent)
        self.logger.info(f"Запуск цикла обработки каналов, всего каналов: {len(self.channels)}")
        batch_size = 10
        idx = 0
        batch_counter = 1
        total_processed_channels = 0
        start_time = datetime.utcnow()
        
        while self.is_running:
            try:
                await self.monitor.update_activity()
                self.logger.info(f"[MONITOR] Активность обновлена перед batch {batch_counter}")
                current_time = datetime.utcnow()
                elapsed = (current_time - start_time).total_seconds()
                self.logger.info(f"[STATS] Время работы: {elapsed:.1f} сек. Обработано каналов: {total_processed_channels}/{len(self.channels)}")
                self.logger.info(f"[RUN] Начало итерации {batch_counter}, индекс {idx}")
                
                # Проверка глобальной паузы парсера
                self.logger.info("Проверка глобальной паузы парсера...")
                global_pause = await self.redis.get("parser:global_pause")
                if global_pause and global_pause.decode('utf-8') == "1":
                    self.logger.info("[GLOBAL PAUSE] Парсер поставлен на глобальную паузу, ожидание...")
                    await asyncio.sleep(30)  # Проверяем каждые 30 секунд
                    continue
                
                # Проверяем флаг сброса last_message_ids
                reset_flag = await self.redis.get("parser:reset_last_ids")
                if reset_flag and reset_flag.decode('utf-8') == "1":
                    self.logger.info("[RESET] Получен сигнал сброса last_message_ids, сбрасываем для всех каналов...")
                    try:
                        await self.state.reset_all_last_ids()
                        await self.redis.delete("parser:reset_last_ids")
                        self.logger.info("[RESET] last_message_ids сброшены для всех каналов")
                    except Exception as e:
                        self.logger.error(f"[RESET] Ошибка при сбросе last_message_ids: {e}")
                
                self.logger.info("Проверка глобального floodwait...")
                if await self.global_floodwait.is_active():
                    left = (self.global_floodwait.until - datetime.utcnow()).total_seconds()
                    self.logger.warning(f"[GLOBAL FLOODWAIT] Ожидание {left:.1f} сек...")
                    await self.global_floodwait.wait_until_free()
                    continue
                self.logger.info("Создание задач для обработки каналов...")
                batch = self.channels[idx:idx+batch_size]
                if not batch:
                    idx = 0
                    batch = self.channels[:batch_size]
                    batch_counter = 1
                    self.logger.info(f"[RUN] Закончен полный проход по каналам, начинаем заново")
                self.logger.info(f"[RUN] Текущий батч {batch_counter}: {batch}")
                tasks = []
                for channel in batch:
                    self.logger.info(f"[TASK] Старт задачи для канала: {channel}")
                    async def wrapped_process_channel(channel=channel, semaphore=semaphore):
                        self.logger.info(f"[TASK] Начало обработки канала: {channel}")
                        try:
                            await self._process_channel(channel, semaphore)
                            self.logger.info(f"[TASK] Завершена обработка канала: {channel}")
                        except Exception as e:
                            self.logger.error(f"[TASK] Ошибка в задаче обработки канала {channel}: {e}")
                    tasks.append(wrapped_process_channel())
                self.logger.info(f"Запущено {len(tasks)} задач для обработки каналов (batch {batch_counter})")
                # Fire-and-forget: просто запускаем задачи, не ждём их завершения
                for t in tasks:
                    asyncio.create_task(t)
                total_processed_channels += len(batch)
                idx += batch_size
                batch_counter += 1
                await self.monitor.update_activity()
                self.logger.info(f"[MONITOR] Активность обновлена после batch {batch_counter-1}")
                self.logger.info(f"[RUN] Ожидание 10 секунд перед следующим батчем...")
                await asyncio.sleep(10)
            except Exception as e:
                self.logger.critical(f"[RUN] Критическая ошибка в основном цикле: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(10)

    async def entity_resolver_worker(self):
        while True:
            username_or_invite, fut = await self.entity_queue.get()
            key = (username_or_invite['type'], username_or_invite['value'])
            try:
                # Кэш проверяем ещё раз (мог появиться за время ожидания)
                if key in self.entity_cache:
                    fut.set_result(self.entity_cache[key])
                else:
                    redis_key = f"entity_type:{key[0]}:{key[1]}"
                    redis_result = await self.redis.get(redis_key)
                    if redis_result:
                        result = redis_result.decode()
                        self.entity_cache[key] = result
                        fut.set_result(result)
                    else:
                        if username_or_invite['type'] == 'invite':
                            result = 'channel'
                        else:
                            try:
                                entity = await self.client.get_entity(username_or_invite['value'])
                                from telethon.tl.types import User, Channel, Chat
                                if isinstance(entity, User):
                                    result = 'user'
                                elif isinstance(entity, (Channel, Chat)):
                                    result = 'channel'
                                else:
                                    result = 'unknown'
                            except Exception as e:
                                self.logger.warning(f"[LINK_FILTER][QUEUE] Не удалось разрешить {username_or_invite['value']}: {e}")
                                result = 'unknown'
                        self.entity_cache[key] = result
                        await self.redis.set(redis_key, result, ex=self.entity_redis_ttl)
                        fut.set_result(result)
            except Exception as e:
                fut.set_result('unknown')
            await asyncio.sleep(5)  # Rate limit: 1 запрос в 5 секунд

    async def resolve_entity_type_queued(self, username_or_invite):
        key = (username_or_invite['type'], username_or_invite['value'])
        # 1. In-memory LRU cache
        if key in self.entity_cache:
            return self.entity_cache[key]
        # 2. Redis cache
        redis_key = f"entity_type:{key[0]}:{key[1]}"
        redis_result = await self.redis.get(redis_key)
        if redis_result:
            result = redis_result.decode()
            self.entity_cache[key] = result
            return result
        # 3. Очередь
        if key in self.entity_futures:
            return await self.entity_futures[key]
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.entity_futures[key] = fut
        await self.entity_queue.put((username_or_invite, fut))
        result = await fut
        self.entity_futures.pop(key, None)
        return result

    async def process_message_handler(self, task):
        def extract_telegram_links(text):
            if not text:
                return []
            url_pattern = r"https?://t\.me/([a-zA-Z0-9_]+|\+[^\s/]+|joinchat/[^\s/]+)"
            at_pattern = r"(?<!\w)@([a-zA-Z0-9_]{5,32})"
            urls = re.findall(url_pattern, text)
            ats = re.findall(at_pattern, text)
            links = []
            for u in urls:
                if u.startswith('+') or u.startswith('joinchat/'):
                    links.append({'type': 'invite', 'value': u})
                else:
                    links.append({'type': 'username', 'value': u})
            for a in ats:
                links.append({'type': 'username', 'value': a})
            return links

        # --- Фильтрация ссылок перед отправкой в Kafka ---
        if not self.entity_worker_started:
            asyncio.create_task(self.entity_resolver_worker())
            self.entity_worker_started = True
            
        # Проверяем наличие медиа в сообщении
        if task.get("type") == "process_album":
            msgs = task["messages"]
            # В альбоме всегда есть медиа, можно продолжать
        else:
            msg = task["message"]
            # Проверка для одиночного сообщения - обрабатываем только с медиа
            if not hasattr(msg, 'media') or not msg.media:
                self.logger.info(f"[MEDIA_FILTER] Сообщение не содержит медиа, пропускаем. ID: {getattr(msg, 'id', 'unknown')}")
                return
                
        if task.get("type") == "process_album":
            msgs = task["messages"]
            text = msgs[0].message if msgs and hasattr(msgs[0], 'message') else ''
        else:
            msg = task["message"]
            text = msg.message if msg and hasattr(msg, 'message') else ''

        links = extract_telegram_links(text)
        # 1. Если в посте нет ссылок — отбрасываем
        if not links:
            self.logger.info(f"[LINK_FILTER] Пост не содержит ссылок, отбрасывается. text={text}")
            return
        user_links = []
        channel_links = []
        for l in links:
            # Кэш проверяется внутри resolve_entity_type_queued, но явно проверяем здесь для читаемости
            t = await self.resolve_entity_type_queued(l)
            if t == 'user':
                user_links.append(l)
            elif t == 'channel':
                channel_links.append(l)
        if user_links and not channel_links:
            pass
        elif user_links and channel_links:
            for l in channel_links:
                if l['type'] == 'invite':
                    text = re.sub(rf'https?://t\.me/{re.escape(l["value"])}', '', text)
                elif l['type'] == 'username':
                    text = re.sub(rf'https?://t\.me/{re.escape(l["value"])}', '', text)
                    text = re.sub(rf'(?<!\w)@{re.escape(l["value"])}', '', text)
            if task.get("type") == "process_album":
                msgs[0].message = text
            else:
                msg.message = text
        elif channel_links and not user_links:
            self.logger.info(f"[LINK_FILTER] Пост содержит только ссылки на каналы/группы, не отправляем дальше. text={text}")
            return
        
        # ОБРАБОТКА АЛЬБОМОВ (МЕДИАГРУПП)
        if task.get("type") == "process_album":
            channel = task["channel"]
            msgs = task["messages"]
            try:
                self.logger.info(f"[PROCESS][ALBUM][START] Обработка альбома в канале {channel}, {len(msgs)} сообщений")
                start_total = time.time()
                album_data = {
                    'channel': channel,
                    'id': msgs[0].id if msgs else 0,
                    'date': (msgs[0].date.isoformat() if hasattr(msgs[0], 'date') else '') if msgs else '',
                    'text': msgs[0].message if msgs and hasattr(msgs[0], 'message') else '',
                    'media': [],
                    'is_album': True,
                    'grouped_id': msgs[0].grouped_id if msgs and hasattr(msgs[0], 'grouped_id') else None
                }
                
                # Обработка каждого сообщения в альбоме для получения file_id или скачивания
                for msg in msgs:
                    if msg.media:
                        try:
                            media_obj = None
                            media_type = None
                            caption = msg.message or ""
                            mime = None
                            size = None
                            
                            # Определение типа медиа и объекта
                            if isinstance(msg.media, MessageMediaPhoto) and getattr(msg.media, 'photo', None):
                                media_obj = msg.media.photo
                                media_type = "photo"
                            elif isinstance(msg.media, MessageMediaDocument) and getattr(msg.media, 'document', None):
                                media_obj = msg.media.document
                                doc = msg.media.document
                                mime = doc.mime_type if hasattr(doc, 'mime_type') else None
                                size = doc.size if hasattr(doc, 'size') else None
                                # Определяем тип по mime
                                if mime:
                                    if mime.startswith("video"):
                                        media_type = "video"
                                    elif mime.startswith("audio"):
                                        media_type = "audio"
                                    elif mime == "application/x-tgsticker":
                                        media_type = "sticker"
                                    elif mime == "image/webp":
                                        media_type = "sticker"
                                    elif mime == "image/gif":
                                        media_type = "animation"
                                    else:
                                        media_type = "document"
                                else:
                                    media_type = "document"
                                
                            # Пробуем получить file_id
                            if media_obj is not None:
                                try:
                                    self.logger.info(f"[PROCESS][ALBUM] Attempting pack_bot_file_id for media_obj of type: {type(media_obj)}")
                                    file_id = pack_bot_file_id(media_obj)
                                    media_item = {
                                        "type": media_type,
                                        "file_id": file_id,
                                        "caption": caption
                                    }
                                    if mime:
                                        media_item["mime_type"] = mime
                                    if size:
                                        media_item["size"] = size
                                    album_data['media'].append(media_item)
                                    self.logger.info(f"[PROCESS][ALBUM] Получен file_id для {channel} #{msg.id}: {file_id} (type={media_type})")
                                except Exception as e:
                                    self.logger.error(f"[PROCESS][ALBUM] Не удалось получить file_id для media_obj типа {type(media_obj)}: {e}. Traceback: {traceback.format_exc()}", exc_info=False)
                                    # Fallback на скачивание, если file_id получить не удалось
                                    unique_id = secrets.token_hex(8)
                                    file_path = os.path.join(self.temp_dir, f"{msg.id}_{unique_id}_{media_type or 'media'}")
                                    try:
                                        downloaded_path = await self.client.download_media(msg, file=file_path)
                                        if downloaded_path and os.path.exists(downloaded_path):
                                            media_item = {
                                                "type": media_type or "media",
                                                "local_path": downloaded_path,
                                                "caption": caption
                                            }
                                            if mime:
                                                media_item["mime_type"] = mime
                                            if size:
                                                media_item["size"] = size
                                            album_data['media'].append(media_item)
                                    except Exception as e2:
                                        self.logger.error(f"[PROCESS][ALBUM][FALLBACK] Ошибка: {e2}")
                            # Поддержка других типов медиа (контакты, гео и т.д.) для полноты
                            elif isinstance(msg.media, MessageMediaContact):
                                album_data['media'].append({
                                    "type": "contact",
                                    "phone": msg.media.phone_number,
                                    "first_name": msg.media.first_name,
                                    "last_name": msg.media.last_name
                                })
                            elif isinstance(msg.media, MessageMediaGeo):
                                geo = msg.media.geo
                                album_data['media'].append({
                                    "type": "geo",
                                    "lat": geo.lat,
                                    "long": geo.long
                                })
                            elif isinstance(msg.media, MessageMediaPoll):
                                poll = msg.media.poll
                                album_data['media'].append({
                                    "type": "poll",
                                    "question": poll.question,
                                    "answers": [a.text for a in poll.answers]
                                })
                        except Exception as e:
                            self.logger.error(f"[PROCESS][ALBUM] Ошибка обработки медиа в альбоме: {e}\n{traceback.format_exc()}")
                
                # Отправка альбома в Kafka
                self.logger.info(f"[PROCESS][ALBUM] Отправка альбома в Kafka: {len(album_data['media'])} медиа")
                try:
                    await async_backoff(self.kafka.send, album_data, logger=self.logger)
                    self.logger.info(f"[PROCESS][ALBUM] Альбом отправлен в Kafka, {len(album_data['media'])} медиа")
                except Exception as e:
                    self.logger.error(f"[PROCESS][ALBUM] Ошибка отправки альбома в Kafka: {e}\n{traceback.format_exc()}")
                    # Очистка скачанных файлов при ошибке отправки в Kafka
                    self.logger.info(f"[PROCESS][ALBUM][CLEANUP] Попытка очистки скачанных файлов альбома из-за ошибки отправки в Kafka.")
                    for media_item_to_clean in album_data.get('media', []):
                        local_path_album_clean = media_item_to_clean.get("local_path")
                        if local_path_album_clean and os.path.exists(local_path_album_clean):
                            try:
                                os.remove(local_path_album_clean)
                                self.logger.info(f"[PROCESS][ALBUM][CLEANUP] Удален временный файл: {local_path_album_clean}")
                            except Exception as e_clean_album:
                                self.logger.warning(f"[PROCESS][ALBUM][CLEANUP] Ошибка при удалении временного файла {local_path_album_clean}: {e_clean_album}")
                
                self.logger.info(f"[PROCESS][ALBUM][END] Обработка альбома завершена за {time.time() - start_total:.2f} сек")
                return
            except Exception as e:
                self.logger.error(f"[PROCESS][ALBUM][FATAL] Ошибка обработки альбома: {e}\n{traceback.format_exc()}")
                return
            
        # Обычная обработка одиночного сообщения
        channel = task["channel"]
        msg = task["message"]
        try:
            self.logger.info(f"[PROCESS][START] Обработка сообщения {channel} #{getattr(msg, 'id', 'unknown')}")
            start_total = time.time()
            media_list = []
            import time as _time
            # Логируем состояние клиента
            self.logger.info(f"[DEBUG] Telethon connected: {self.client.is_connected()}, authorized: {await self.client.is_user_authorized()}")
            # Логируем содержимое temp_dir до скачивания
            try:
                files_dir = os.listdir(self.temp_dir)
                self.logger.info(f"[DEBUG] Содержимое {self.temp_dir} до скачивания: {files_dir}")
            except Exception as e:
                self.logger.error(f"[DEBUG] Ошибка при логировании содержимого temp_dir: {e}")
            # Скачивание и загрузка медиа
            if msg.media:
                start_media = _time.time()
                # Универсальная обработка медиа
                try:
                    media_obj = None
                    media_type = None
                    caption = msg.message or ""
                    mime = None
                    size = None
                    # Определяем тип медиа и объект
                    if isinstance(msg.media, MessageMediaPhoto) and getattr(msg.media, 'photo', None):
                        media_obj = msg.media.photo
                        media_type = "photo"
                    elif isinstance(msg.media, MessageMediaDocument) and getattr(msg.media, 'document', None):
                        media_obj = msg.media.document
                        doc = msg.media.document
                        mime = doc.mime_type if hasattr(doc, 'mime_type') else None
                        size = doc.size if hasattr(doc, 'size') else None
                        # Определяем тип по mime
                        if mime:
                            if mime.startswith("video"):
                                media_type = "video"
                            elif mime.startswith("audio"):
                                media_type = "audio"
                            elif mime == "application/x-tgsticker":
                                media_type = "sticker"
                            elif mime == "image/webp":
                                media_type = "sticker"
                            elif mime == "image/gif":
                                media_type = "animation"
                            else:
                                media_type = "document"
                        else:
                            media_type = "document"
                    # Пробуем получить file_id для любого медиа
                    if media_obj is not None:
                        try:
                            self.logger.info(f"[PROCESS][MEDIA] Attempting pack_bot_file_id for media_obj of type: {type(media_obj)}")
                            file_id = pack_bot_file_id(media_obj)
                            media_item = {
                                "type": media_type,
                                "file_id": file_id,
                                "caption": caption
                            }
                            if mime:
                                media_item["mime_type"] = mime
                            if size:
                                media_item["size"] = size
                            media_list.append(media_item)
                            self.logger.info(f"[PROCESS][MEDIA] Получен file_id для {channel} #{msg.id}: {file_id} (type={media_type})")
                        except Exception as e:
                            self.logger.error(f"[PROCESS][MEDIA] Не удалось получить file_id для media_obj типа {type(media_obj)}: {e}. Traceback: {traceback.format_exc()}", exc_info=False)
                            unique_id = secrets.token_hex(8)
                            file_path = os.path.join(self.temp_dir, f"{msg.id}_{unique_id}_{media_type or 'media'}")
                            try:
                                downloaded_path = await self.client.download_media(msg, file=file_path)
                                if downloaded_path and os.path.exists(downloaded_path):
                                    media_item = {
                                        "type": media_type or "media",
                                        "local_path": downloaded_path,
                                        "caption": caption
                                    }
                                    if mime:
                                        media_item["mime_type"] = mime
                                    if size:
                                        media_item["size"] = size
                                    media_list.append(media_item)
                            except Exception as e2:
                                self.logger.error(f"[PROCESS][MEDIA][FALLBACK] Ошибка: {e2}")
                    # Контакты, гео, опросы — как раньше
                    elif isinstance(msg.media, MessageMediaContact):
                        self.logger.info(f"[PROCESS][CONTACT] Обработка контакта для {channel} #{msg.id}")
                        media_list.append({
                            "type": "contact",
                            "phone": msg.media.phone_number,
                            "first_name": msg.media.first_name,
                            "last_name": msg.media.last_name
                        })
                    elif isinstance(msg.media, MessageMediaGeo):
                        self.logger.info(f"[PROCESS][GEO] Обработка геолокации для {channel} #{msg.id}")
                        geo = msg.media.geo
                        media_list.append({
                            "type": "geo",
                            "lat": geo.lat,
                            "long": geo.long
                        })
                    elif isinstance(msg.media, MessageMediaPoll):
                        self.logger.info(f"[PROCESS][POLL] Обработка опроса для {channel} #{msg.id}")
                        poll = msg.media.poll
                        media_list.append({
                            "type": "poll",
                            "question": poll.question,
                            "answers": [a.text for a in poll.answers]
                        })
                except Exception as e:
                    self.logger.error(f"[PROCESS][MEDIA][UNIVERSAL] Ошибка: {e}\n{traceback.format_exc()}")
            # Формируем payload и отправляем в Kafka
            start_payload = _time.time()
            data = {
                'channel': channel,
                'id': msg.id,
                'date': msg.date.isoformat() if msg.date else '',
                'text': msg.message or '',
                'media': media_list
            }
            self.logger.info(f"[PROCESS][KAFKA] Payload для отправки: {json.dumps(data, ensure_ascii=False)[:500]}")
            try:
                await async_backoff(self.kafka.send, data, logger=self.logger)
                self.logger.info(f"[PROCESS][KAFKA] Сообщение отправлено: {channel} #{msg.id} (медиа: {len(media_list)})")
            except Exception as e:
                self.logger.error(f"[PROCESS][KAFKA] Ошибка отправки в Kafka: {e}\n{traceback.format_exc()}")
                # Очистка скачанных файлов при ошибке отправки в Kafka
                self.logger.info(f"[PROCESS][KAFKA][CLEANUP] Попытка очистки скачанных файлов сообщения из-за ошибки отправки в Kafka.")
                for media_item_to_clean in media_list: # media_list содержит элементы с local_path
                    local_path_msg_clean = media_item_to_clean.get("local_path")
                    if local_path_msg_clean and os.path.exists(local_path_msg_clean):
                        try:
                            os.remove(local_path_msg_clean)
                            self.logger.info(f"[PROCESS][KAFKA][CLEANUP] Удален временный файл: {local_path_msg_clean}")
                        except Exception as e_clean_msg:
                            self.logger.warning(f"[PROCESS][KAFKA][CLEANUP] Ошибка при удалении временного файла {local_path_msg_clean}: {e_clean_msg}")
            
            self.logger.info(f"[PROCESS][KAFKA] Время формирования и отправки payload: {_time.time() - start_payload:.2f} сек")
            # Логируем содержимое temp_dir после отправки
            try:
                files_dir = os.listdir(self.temp_dir)
                self.logger.info(f"[DEBUG] Содержимое {self.temp_dir} после отправки: {files_dir}")
            except Exception as e:
                self.logger.error(f"[DEBUG] Ошибка при логировании содержимого temp_dir: {e}")
            self.logger.info(f"[PROCESS][END] Обработка сообщения {channel} #{getattr(msg, 'id', 'unknown')} завершена за {time.time() - start_total:.2f} сек")
        except Exception as e:
            self.logger.error(f"[PROCESS][FATAL] Ошибка обработки сообщения {channel} #{getattr(msg, 'id', 'unknown')}: {e}\n{traceback.format_exc()}")

    async def _process_channel(self, channel, semaphore):
        self.logger.info(f"[CHANNEL] Начало обработки: {channel}")
        
        # Проверяем, находится ли канал в черном списке
        if channel in self.blacklisted_channels:
            self.logger.info(f"[CHANNEL] {channel} находится в черном списке, пропускаем")
            return
            
        # Проверим состояние очереди задач
        queue_stats = self.task_queue.get_stats()
        self.logger.info(f"[CHANNEL] Состояние очереди задач: {queue_stats}")
        
        async with semaphore:
            if await self.global_floodwait.is_active():
                self.logger.info(f"[CHANNEL] Floodwait активен, пропуск: {channel}")
                return
            is_fw, wait = await self.floodwait.is_floodwait(channel)
            if is_fw:
                self.logger.warning(f"FloodWait for {channel}, skip for {wait:.1f}s")
                return
            await self.rate_limiter.acquire(key=channel)
            try:
                entity = await self.get_entity_cached(channel)
                if not entity:
                    self.logger.error(f"[CHANNEL] Не удалось получить entity для {channel}, пропуск")
                    return
                self.logger.info(f"[CHANNEL] Получен entity для {channel}: id={getattr(entity, 'id', 'None')}, title={getattr(entity, 'title', 'None')}")
                
                last_id = await self.state.get_last_id(channel)

                # --- NEW LOGIC FOR FIRST RUN ---
                if last_id == 0:
                    self.logger.info(f"[CHANNEL][FIRST_RUN] Канал {channel} обрабатывается впервые. Получение ID последнего сообщения для инициализации.")
                    try:
                        # Fetch only the latest message (or a few) to get its ID
                        history_for_init = await async_backoff(
                            self.client,
                            GetHistoryRequest,
                            peer=entity,
                            limit=1, # Only need the very last message to set initial last_id
                            offset_id=0,
                            offset_date=None,
                            add_offset=0,
                            max_id=0,
                            min_id=0,
                            hash=0,
                            logger=self.logger
                        )
                        if history_for_init and history_for_init.messages:
                            latest_message_id = history_for_init.messages[0].id
                            await self.state.set_last_id(channel, latest_message_id)
                            self.logger.info(f"[CHANNEL][FIRST_RUN] Для канала {channel} установлен начальный last_id = {latest_message_id}. Сообщения с этой итерации обрабатываться не будут.")
                        else:
                            # No messages in the channel, last_id remains 0 (or whatever default)
                            # It will be re-evaluated on the next cycle.
                            self.logger.info(f"[CHANNEL][FIRST_RUN] В канале {channel} нет сообщений для инициализации last_id. last_id остается {last_id}.")
                        return # Exit after initializing last_id for the first run, no processing this time.
                    except FloodWaitError as e:
                        self.logger.critical(f"[FLOODWAIT][FIRST_RUN_INIT] FloodWaitError при инициализации last_id для {channel}: {e.seconds} секунд. Трассировка: {traceback.format_exc()}")
                        await self.global_floodwait.set(e.seconds)
                        await self._notify_admin(channel, f"FloodWaitError (first run init for last_id): {e.seconds}s")
                        return
                    except (UserNotParticipantError, ChannelPrivateError, InviteHashInvalidError) as e:
                        self.logger.error(f"[CHANNEL][FIRST_RUN_INIT] Ошибка доступа к {channel} при инициализации last_id: {type(e).__name__}: {e}. Трассировка: {traceback.format_exc()}")
                        await self._notify_admin_not_member(channel, e) # Potentially blacklist
                        return
                    except Exception as e:
                        self.logger.error(f"[CHANNEL][FIRST_RUN_INIT] Ошибка при получении последнего ID для инициализации {channel}: {e}\n{traceback.format_exc()}")
                        # Не выходим глобально, просто этот канал будет пропущен в текущей итерации и попробует снова
                        return
                # --- END OF NEW LOGIC FOR FIRST RUN ---
                
                # Existing logic for subsequent runs (now last_id is guaranteed to be non-zero if messages existed)
                try:
                    history = await async_backoff(
                        self.client,
                        GetHistoryRequest,
                        peer=entity,
                        limit=10, # Standard limit for normal operation
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        max_id=0,
                        min_id=0, # We filter by m.id > last_id later, so min_id=0 is fine here.
                        hash=0,
                        logger=self.logger
                    )
                    self.logger.info(f"[CHANNEL] Получена история для {channel}: {len(history.messages)} сообщений (текущий last_id: {last_id})")
                except FloodWaitError as e:
                    self.logger.critical(f"[FLOODWAIT] FloodWaitError для {channel}: {e.seconds} секунд ожидания. Трассировка: {traceback.format_exc()}")
                    await self.global_floodwait.set(e.seconds)
                    await self._notify_admin(channel, f"FloodWaitError: {e.seconds}s")
                    return
                except UserNotParticipantError as e:
                    self.logger.error(f"[NOT_PARTICIPANT] UserNotParticipantError для {channel}: {e}. Трассировка: {traceback.format_exc()}")
                    await self._notify_admin_not_member(channel, e)
                    return
                except ChannelPrivateError as e:
                    self.logger.error(f"[PRIVATE] ChannelPrivateError для {channel}: {e}. Трассировка: {traceback.format_exc()}")
                    await self._notify_admin_not_member(channel, e)
                    return
                except InviteHashInvalidError as e:
                    self.logger.error(f"[INVITE_INVALID] InviteHashInvalidError для {channel}: {e}. Трассировка: {traceback.format_exc()}")
                    await self._notify_admin_not_member(channel, e)
                    return
                except Exception as e:
                    self.logger.error(f"[CHANNEL] Ошибка получения истории для {channel}: {e}\n{traceback.format_exc()}")
                    return
                    
                if not history.messages:
                    self.logger.info(f"[CHANNEL] {channel}: История пуста, пропуск")
                    return
                    
                messages = sorted(history.messages, key=lambda m: m.id)
                self.logger.info(f"[CHANNEL] {channel}: Получено {len(messages)} сообщений, последний ID: {last_id}")
                
                new_msgs = [m for m in messages if m.id > last_id]
                self.logger.info(f"[CHANNEL] {channel}: Найдено {len(new_msgs)} новых сообщений")
                
                if not new_msgs:
                    self.logger.info(f"[CHANNEL] {channel}: Нет новых сообщений, пропуск")
                    return
                
                # Сохраним максимальный ID сообщения
                max_msg_id = max([m.id for m in new_msgs]) if new_msgs else last_id
                
                # Подробная информация о новых сообщениях для дебага
                for msg in new_msgs:
                    self.logger.info(f"[CHANNEL] {channel} #{msg.id} — обрабатываю сообщение (дата: {msg.date})")
                    
                # Считаем количество задач, которые нужно отправить
                tasks_to_add = 0
                for msg in new_msgs:
                    # Пропускаем сообщения без медиа
                    if not hasattr(msg, 'media') or not msg.media:
                        self.logger.info(f"[MEDIA_FILTER] Пропускаем сообщение без медиа {channel} #{msg.id}")
                        continue
                        
                    if not filter_message(msg.message or '', self.include, self.exclude):
                        continue
                    if hasattr(msg, 'grouped_id') and msg.grouped_id:
                        if await self.state.is_album_processed(msg.grouped_id):
                            continue
                        album_msgs = [m for m in new_msgs if getattr(m, 'grouped_id', None) == msg.grouped_id]
                        tasks_to_add += len(album_msgs)
                    else:
                        tasks_to_add += 1
                        
                self.logger.info(f"[CHANNEL] {channel}: Будет добавлено {tasks_to_add} задач в очередь")
                
                # Добавляем задачи в очередь
                # 1. Сначала обрабатываем альбомы (grouped_id)
                processed_albums = set()
                for msg in new_msgs:
                    # Пропускаем сообщения без медиа
                    if not hasattr(msg, 'media') or not msg.media:
                        continue
                        
                    if not filter_message(msg.message or '', self.include, self.exclude):
                        continue
                    if hasattr(msg, 'grouped_id') and msg.grouped_id:
                        gid = msg.grouped_id
                        if gid in processed_albums:
                            continue
                        if await self.state.is_album_processed(gid):
                            self.logger.info(f"[ALBUM] Альбом {gid} уже обработан, пропуск")
                            continue
                        album_msgs = [m for m in new_msgs if getattr(m, 'grouped_id', None) == gid]
                        self.logger.info(f"[ALBUM] Начало обработки альбома {gid} (сообщений: {len(album_msgs)})")
                        await self.task_queue.add_task({
                            "type": "process_album",
                            "channel": channel,
                            "messages": album_msgs,
                            "handler": self.process_message_handler
                        })
                        await self.state.mark_album_processed(gid)
                        processed_albums.add(gid)
                # 2. Одиночные сообщения (без grouped_id)
                for msg in new_msgs:
                    # Пропускаем сообщения без медиа
                    if not hasattr(msg, 'media') or not msg.media:
                        continue
                        
                    if not filter_message(msg.message or '', self.include, self.exclude):
                        continue
                    if hasattr(msg, 'grouped_id') and msg.grouped_id:
                        continue  # уже обработано выше
                    self.logger.info(f"[CHANNEL] {channel} #{msg.id} — отправка задачи на обработку сообщения")
                    await self.task_queue.add_task({
                        "type": "process_message",
                        "channel": channel,
                        "message": msg,
                        "handler": self.process_message_handler
                    })
                
                # Обновляем last_id в самом конце после отправки всех задач в очередь
                self.logger.info(f"[CHANNEL] Обновление last_id для {channel} на {max_msg_id}")
                await self.state.set_last_id(channel, max_msg_id)
                await self.monitor.update_activity()
                
                # Проверяем состояние очереди после добавления задач
                queue_stats = self.task_queue.get_stats()
                self.logger.info(f"[CHANNEL] После обработки {channel}: Состояние очереди задач: {queue_stats}")
                
                self.logger.info(f"[CHANNEL] Конец обработки: {channel}. Обработано {len(new_msgs)} сообщений")
            except (UserNotParticipantError, ChannelPrivateError, InviteHashInvalidError) as e:
                self.logger.error(f"[CHANNEL] Ошибка доступа к {channel}: {type(e).__name__}: {e}. Трассировка: {traceback.format_exc()}")
                await self._notify_admin_not_member(channel, e)
            except FloodWaitError as e:
                self.logger.critical(f"[FLOODWAIT] FloodWaitError (глобальный) для {channel}: {e.seconds} секунд ожидания. Трассировка: {traceback.format_exc()}")
                await self.global_floodwait.set(e.seconds)
                await self._notify_admin(channel, f"FloodWaitError: {e.seconds}s")
            except Exception as e:
                self.logger.error(f"Ошибка при обработке {channel}: {e}\n{traceback.format_exc()}")
                await self._notify_admin(channel, e)

    async def _notify_admin(self, channel, error):
        try:
            alert = {
                "type": "parser_error" if not str(error).startswith("FloodWaitError") else "floodwait",
                "channel": str(channel),
                "error": str(error)
            }
            alert_str = str(alert)
            await self.redis.publish("tg:parser:alerts", alert_str.encode('utf-8'))
            self.logger.info(f"[ALERT] Уведомление админу отправлено: {alert}")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке уведомления админу: {e}")

    async def _notify_admin_not_member(self, channel, error):
        try:
            # Проверяем, стоит ли добавить канал в черный список
            if isinstance(error, (ChannelPrivateError)) and channel not in self.blacklisted_channels:
                self.logger.warning(f"[BLACKLIST] Канал {channel} будет добавлен в черный список из-за ошибки: {error}")
                self.blacklisted_channels.append(channel)
                # Сохраняем черный список в Redis для персистентности между перезапусками
                await self.redis.set("parser:blacklisted_channels", json.dumps(self.blacklisted_channels))
                self.logger.info(f"[BLACKLIST] Обновленный черный список: {self.blacklisted_channels}")
                
            alert = {
                "type": "not_member",
                "channel": str(channel),
                "error": str(error)
            }
            alert_str = str(alert)
            await self.redis.publish("tg:parser:alerts", alert_str.encode('utf-8'))
            self.logger.info(f"[ALERT] Не член канала, уведомление админу отправлено: {alert}")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке уведомления админу (not_member): {e}")

    async def load_blacklisted_channels(self):
        """Загружаем черный список каналов из Redis"""
        try:
            blacklist_json = await self.redis.get("parser:blacklisted_channels")
            if blacklist_json:
                self.blacklisted_channels = json.loads(blacklist_json)
                self.logger.info(f"[BLACKLIST] Загружены {len(self.blacklisted_channels)} каналов из черного списка Redis")
            else:
                self.logger.info("[BLACKLIST] Черный список каналов не найден в Redis")
        except Exception as e:
            self.logger.error(f"[BLACKLIST] Ошибка при загрузке черного списка: {e}")

async def main():
    # Подключение к Postgres и инициализация схемы
    state_manager = await PostgresStateManager.create()
    async with state_manager.pool.acquire() as conn:
        await init_db_schema(conn)
    parser = ParserCore(state_manager)
    try:
        await parser.start()
    except KeyboardInterrupt:
        print("Остановка по Ctrl+C")
        await parser.stop()

if __name__ == "__main__":
    asyncio.run(main()) 