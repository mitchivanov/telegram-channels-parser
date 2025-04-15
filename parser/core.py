import asyncio
import os
import tempfile
from .state import StateManager
from .rate_limiter import RateLimiter
from .floodwait_manager import FloodWaitManager
from .kafka_producer import KafkaProducerAsync
from .activity_monitor import ActivityMonitor
from .utils import filter_message
from .config_loader import REDIS_URL, TELEGRAM_2FA_PASSWORD, TELEGRAM_PHONE, TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, MessageMediaContact, MessageMediaGeo, MessageMediaPoll
import logging
from datetime import datetime, timezone, timedelta
from redis.asyncio import Redis
from minio import Minio
from minio.error import S3Error

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
    def __init__(self, tg_config, kafka_config):
        self.tg_config = tg_config
        self.kafka_config = kafka_config
        self.state = StateManager()
        self.rate_limiter = RateLimiter(requests_per_minute=40)
        self.floodwait = FloodWaitManager()
        self.global_floodwait = GlobalFloodWaitManager()
        self.kafka = KafkaProducerAsync(kafka_config)
        self.monitor = ActivityMonitor()
        self.redis = Redis.from_url(REDIS_URL, decode_responses=True)
        self.client = TelegramClient(
            TELEGRAM_SESSION,
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH
        )
        self.channels = tg_config['channels']
        self.logger = logging.getLogger("parser.core")
        self.is_running = False
        self.max_concurrent = 5
        self.include = tg_config.get('include', [])
        self.exclude = tg_config.get('exclude', [])
        self.temp_dir = tempfile.mkdtemp(prefix="tg_media_")
        self.minio = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        self.minio_bucket = MINIO_BUCKET

    async def _ensure_minio_bucket(self):
        found = self.minio.bucket_exists(self.minio_bucket)
        if not found:
            self.minio.make_bucket(self.minio_bucket)

    async def _upload_to_minio(self, file_path, object_name):
        try:
            self.minio.fput_object(self.minio_bucket, object_name, file_path)
            url = f"http://{MINIO_ENDPOINT}/{self.minio_bucket}/{object_name}"
            return url
        except S3Error as e:
            self.logger.error(f"Ошибка загрузки в Minio: {e}")
            return None

    async def _get_code_from_admin(self, code_type):
        await self.redis.publish('tg:code:request', code_type)
        self.logger.info(f"Ожидание {code_type} от админа через Redis...")
        for _ in range(60):
            code = await self.redis.get('tg:code:response')
            if code:
                await self.redis.delete('tg:code:response')
                return code.strip()
            await asyncio.sleep(5)
        raise RuntimeError(f"Не получен {code_type} от админа в течение 5 минут")

    async def _start_telethon(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            await self.client.send_code_request(TELEGRAM_PHONE)
            while True:
                try:
                    code = await self._get_code_from_admin('sms')
                    await self.client.sign_in(TELEGRAM_PHONE, code)
                    break
                except PhoneCodeInvalidError:
                    self.logger.error("Введён неверный код подтверждения. Повтор запроса.")
            if await self.client.is_user_authorized():
                return
            try:
                await self.client.sign_in(TELEGRAM_PHONE, code)
            except SessionPasswordNeededError:
                if TELEGRAM_2FA_PASSWORD:
                    await self.client.sign_in(password=TELEGRAM_2FA_PASSWORD)
                else:
                    pw = await self._get_code_from_admin('2fa')
                    await self.client.sign_in(password=pw)

    async def start(self):
        await self._ensure_minio_bucket()
        await self._start_telethon()
        await self.kafka.start()
        self.is_running = True
        self.logger.info("Parser started")
        await self.monitor.start_monitoring()
        await self.run()

    async def stop(self):
        self.is_running = False
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
        while self.is_running:
            if await self.global_floodwait.is_active():
                left = (self.global_floodwait.until - datetime.utcnow()).total_seconds()
                self.logger.warning(f"[GLOBAL FLOODWAIT] Ожидание {left:.1f} сек...")
                await self.global_floodwait.wait_until_free()
                continue
            tasks = []
            for channel in self.channels:
                tasks.append(self._process_channel(channel, semaphore))
            await asyncio.gather(*tasks)
            await asyncio.sleep(15)

    async def _download_media(self, message, media_type):
        try:
            file_path = os.path.join(self.temp_dir, f"{message.id}_{media_type}")
            await self.client.download_media(message, file=file_path)
            if os.path.exists(file_path):
                # Загружаем в Minio
                object_name = os.path.basename(file_path)
                url = await self._upload_to_minio(file_path, object_name)
                os.remove(file_path)
                return url
        except Exception as e:
            self.logger.warning(f"Ошибка скачивания/загрузки медиа: {e}")
        return None

    async def _extract_media(self, message):
        media_list = []
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                url = await self._download_media(message, "photo")
                if url:
                    media_list.append({
                        "type": "photo",
                        "url": url,
                        "caption": message.message or ""
                    })
            elif isinstance(message.media, MessageMediaDocument):
                url = await self._download_media(message, "document")
                if url:
                    doc = message.document
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
                        "url": url,
                        "mime_type": mime,
                        "size": size,
                        "caption": message.message or ""
                    })
            elif isinstance(message.media, MessageMediaWebPage):
                pass
            elif isinstance(message.media, MessageMediaContact):
                media_list.append({
                    "type": "contact",
                    "phone": message.media.phone_number,
                    "first_name": message.media.first_name,
                    "last_name": message.media.last_name
                })
            elif isinstance(message.media, MessageMediaGeo):
                geo = message.media.geo
                media_list.append({
                    "type": "geo",
                    "lat": geo.lat,
                    "long": geo.long
                })
            elif isinstance(message.media, MessageMediaPoll):
                poll = message.media.poll
                media_list.append({
                    "type": "poll",
                    "question": poll.question,
                    "answers": [a.text for a in poll.answers]
                })
        return media_list

    async def _process_channel(self, channel, semaphore):
        async with semaphore:
            if await self.global_floodwait.is_active():
                return
            is_fw, wait = await self.floodwait.is_floodwait(channel)
            if is_fw:
                self.logger.warning(f"FloodWait for {channel}, skip for {wait:.1f}s")
                return
            await self.rate_limiter.acquire(key=channel)
            try:
                entity = await self.client.get_entity(channel)
                last_id = self.state.get_last_id(channel)
                history = await self.client(GetHistoryRequest(
                    peer=entity,
                    limit=10,
                    offset_id=0,
                    offset_date=None,
                    add_offset=0,
                    max_id=0,
                    min_id=0,
                    hash=0
                ))
                messages = sorted(history.messages, key=lambda m: m.id)
                new_msgs = [m for m in messages if m.id > last_id]
                for msg in new_msgs:
                    if not filter_message(msg.message or '', self.include, self.exclude):
                        continue
                    media = await self._extract_media(msg)
                    data = {
                        'channel': channel,
                        'id': msg.id,
                        'date': msg.date.isoformat() if msg.date else '',
                        'text': msg.message or '',
                        'media': media
                    }
                    await self.kafka.send(data)
                    self.logger.info(f"[Kafka] {channel} #{msg.id} отправлено (медиа: {len(media)})")
                    self.state.set_last_id(channel, msg.id)
                    await self.monitor.update_activity()
            except FloodWaitError as e:
                await self.global_floodwait.set(e.seconds)
                self.logger.critical(f"[GLOBAL FLOODWAIT] FloodWaitError: {e.seconds}s. Блокируем все запросы!")
            except Exception as e:
                self.logger.error(f"Ошибка при обработке {channel}: {e}") 