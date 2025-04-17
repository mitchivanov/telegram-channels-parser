import asyncio
import os
import tempfile
from state import StateManager
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
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, MessageMediaContact, MessageMediaGeo, MessageMediaPoll
import logging
from datetime import datetime, timezone, timedelta
from redis.asyncio import Redis
from minio import Minio
from minio.error import S3Error
import json
import traceback
from task_queue_manager import TaskQueueManager

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
    def __init__(self):
        self.state = StateManager()
        self.rate_limiter = RateLimiter(requests_per_minute=40)
        self.floodwait = FloodWaitManager()
        self.global_floodwait = GlobalFloodWaitManager()
        self.kafka = KafkaProducerAsync({
            'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'topic': KAFKA_RAW_TOPIC
        })
        self.monitor = ActivityMonitor()
        # Не используем decode_responses=True для Redis, так как хранятся бинарные данные
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
        
        # Проверяем наличие файла и загружаем каналы
        sources_file = 'updated_sources.txt'
        if os.path.exists(sources_file):
            self.logger.info(f"Чтение списка каналов из {sources_file}")
            with open(sources_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.channels.append(line)
            self.logger.info(f"Загружено {len(self.channels)} каналов")
        else:
            self.logger.error(f"Файл {sources_file} не найден! Список каналов пуст.")
            
        self.entity_cache = {}  # in-memory cache

    async def get_entity_cached(self, channel):
        # 1. In-memory cache
        if channel in self.entity_cache:
            return self.entity_cache[channel]
        # 2. Redis cache через self.redis
        if self.redis:
            try:
                redis_key = f"entity:{channel}"
                data = await self.redis.get(redis_key)
                if data:
                    import pickle
                    entity = pickle.loads(data)
                    self.entity_cache[channel] = entity
                    return entity
            except Exception as e:
                self.logger.warning(f"[ENTITY_CACHE] Ошибка чтения из Redis: {e}")
        # 3. Telegram API с backoff
        try:
            entity = await async_backoff(self.client.get_entity, channel, logger=self.logger)
            self.entity_cache[channel] = entity
            # Пишем в Redis
            if self.redis:
                try:
                    import pickle
                    serialized_data = pickle.dumps(entity)
                    await self.redis.set(f"entity:{channel}", serialized_data, ex=86400)
                except Exception as e:
                    self.logger.warning(f"[ENTITY_CACHE] Ошибка записи в Redis: {e}")
            return entity
        except Exception as e:
            self.logger.error(f"[ENTITY_CACHE] Ошибка получения entity для {channel}: {e}")
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
            # Проверяем, существует ли директория для сессии
            session_dir = os.path.dirname(TELEGRAM_SESSION)
            if session_dir and not os.path.exists(session_dir):
                os.makedirs(session_dir)
                self.logger.info(f"Created session directory: {session_dir}")

            await self._start_telethon()
            await self.kafka.start()
            await self.task_queue.start()
            self.is_running = True
            self.logger.info(f"Parser started with {len(self.channels)} channels configured")
            
            # Проверяем, есть ли каналы для обработки
            if not self.channels:
                self.logger.warning("No channels configured! Please check updated_sources.txt")
                self.logger.info("Parser will continue running but won't process any channels")
            
            await self.monitor.start_monitoring()
            await self.run()
        except Exception as e:
            self.logger.critical(f"Critical error during parser startup: {e}\n{traceback.format_exc()}")
            await self.stop()
            raise

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
        batch_size = 1
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

    async def process_message_handler(self, task):
        channel = task["channel"]
        msg = task["message"]
        try:
            self.logger.info(f"[PROCESS][START] Обработка сообщения {channel} #{getattr(msg, 'id', 'unknown')}")
            media_list = []
            import time
            # Скачивание и загрузка медиа
            if msg.media:
                start_media = time.time()
                if isinstance(msg.media, MessageMediaPhoto):
                    file_path = os.path.join(self.temp_dir, f"{msg.id}_photo")
                    try:
                        self.logger.info(f"[PROCESS][PHOTO] Начало скачивания media для {channel} #{msg.id}")
                        await self.client.download_media(msg, file=file_path)
                        self.logger.info(f"[PROCESS][PHOTO] Скачивание завершено для {channel} #{msg.id}")
                        if os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            self.logger.info(f"[PROCESS][PHOTO] Файл скачан: {file_path}, размер: {file_size} байт")
                            media_list.append({
                                "type": "photo",
                                "local_path": file_path,
                                "caption": msg.message or ""
                            })
                        else:
                            self.logger.error(f"[PROCESS][PHOTO] Файл не найден после скачивания: {file_path}")
                    except Exception as e:
                        self.logger.error(f"[PROCESS][PHOTO] Ошибка: {e}\n{traceback.format_exc()}")
                elif isinstance(msg.media, MessageMediaDocument):
                    file_path = os.path.join(self.temp_dir, f"{msg.id}_document")
                    try:
                        self.logger.info(f"[PROCESS][DOC] Начало скачивания media для {channel} #{msg.id}")
                        await self.client.download_media(msg, file=file_path)
                        self.logger.info(f"[PROCESS][DOC] Скачивание завершено для {channel} #{msg.id}")
                        if os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            self.logger.info(f"[PROCESS][DOC] Файл скачан: {file_path}, размер: {file_size} байт")
                            doc = msg.document
                            mime = doc.mime_type if hasattr(doc, 'mime_type') else None
                            size = doc.size if hasattr(doc, 'size') else None
                            media_type = "document"
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
                            media_list.append({
                                "type": media_type,
                                "local_path": file_path,
                                "mime_type": mime,
                                "size": size,
                                "caption": msg.message or ""
                            })
                        else:
                            self.logger.error(f"[PROCESS][DOC] Файл не найден после скачивания: {file_path}")
                    except Exception as e:
                        self.logger.error(f"[PROCESS][DOC] Ошибка: {e}\n{traceback.format_exc()}")
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
                self.logger.info(f"[PROCESS][MEDIA] Время обработки медиа: {time.time() - start_media:.2f} сек")
            # Формируем payload и отправляем в Kafka
            data = {
                'channel': channel,
                'id': msg.id,
                'date': msg.date.isoformat() if msg.date else '',
                'text': msg.message or '',
                'media': media_list
            }
            self.logger.info(f"[PROCESS][KAFKA] Payload для отправки: {json.dumps(data, ensure_ascii=False)[:500]}")
            await async_backoff(self.kafka.send, data, logger=self.logger)
            self.logger.info(f"[PROCESS][KAFKA] Сообщение отправлено: {channel} #{msg.id} (медиа: {len(media_list)})")
            await self.state.set_last_id(channel, msg.id)
            await self.monitor.update_activity()
        except Exception as e:
            self.logger.error(f"[PROCESS][FATAL] Ошибка обработки сообщения {channel} #{getattr(msg, 'id', 'unknown')}: {e}\n{traceback.format_exc()}")

    async def _process_channel(self, channel, semaphore):
        self.logger.info(f"[CHANNEL] Начало обработки: {channel}")
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
                self.logger.info(f"[CHANNEL] Получен entity для {channel}: {entity}")
                last_id = self.state.get_last_id(channel)
                try:
                    history = await async_backoff(
                        self.client,
                        GetHistoryRequest,
                        peer=entity,
                        limit=10,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        max_id=0,
                        min_id=0,
                        hash=0,
                        logger=self.logger
                    )
                    self.logger.info(f"[CHANNEL] Получена история для {channel}: {len(history.messages)} сообщений")
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
                messages = sorted(history.messages, key=lambda m: m.id)
                self.logger.info(f"[CHANNEL] {channel}: Получено {len(messages)} сообщений, последний ID: {last_id}")
                new_msgs = [m for m in messages if m.id > last_id]
                self.logger.info(f"[CHANNEL] {channel}: Найдено {len(new_msgs)} новых сообщений")
                if not new_msgs:
                    self.logger.info(f"[CHANNEL] {channel}: Нет новых сообщений, пропуск")
                    return
                for msg in new_msgs:
                    if not filter_message(msg.message or '', self.include, self.exclude):
                        continue
                    # Альбомы: grouped_id
                    if hasattr(msg, 'grouped_id') and msg.grouped_id:
                        if self.state.is_album_processed(msg.grouped_id):
                            self.logger.info(f"[ALBUM] Альбом {msg.grouped_id} уже обработан, пропуск")
                            continue
                        album_msgs = [m for m in new_msgs if getattr(m, 'grouped_id', None) == msg.grouped_id]
                        self.logger.info(f"[ALBUM] Начало обработки альбома {msg.grouped_id} (сообщений: {len(album_msgs)})")
                        for album_msg in album_msgs:
                            await self.task_queue.add_task({
                                "type": "process_message",
                                "channel": channel,
                                "message": album_msg,
                                "handler": self.process_message_handler
                            })
                        await self.state.mark_album_processed(msg.grouped_id)
                        await self.state.set_last_id(channel, msg.id)
                        await self.monitor.update_activity()
                        continue
                    # Одиночные сообщения
                    self.logger.info(f"[CHANNEL] {channel} #{msg.id} — отправка задачи на обработку сообщения")
                    await self.task_queue.add_task({
                        "type": "process_message",
                        "channel": channel,
                        "message": msg,
                        "handler": self.process_message_handler
                    })
                    await self.state.set_last_id(channel, msg.id)
                    await self.monitor.update_activity()
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

async def main():
    parser = ParserCore()
    try:
        await parser.start()
    except KeyboardInterrupt:
        print("Остановка по Ctrl+C")
        await parser.stop()

if __name__ == "__main__":
    asyncio.run(main()) 