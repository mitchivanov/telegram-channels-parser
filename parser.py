from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from bot.config import CONFIG, logger
import asyncio
import os
import tempfile
from collections import defaultdict
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio, FSInputFile, BufferedInputFile
from datetime import datetime, timezone, timedelta
import re
import random
import socket
import aiohttp
import signal
import atexit
import time
import traceback

import validators
from telethon import TelegramClient
from telethon.errors import FloodWaitError, PhoneNumberBannedError, PhoneCodeInvalidError, SessionPasswordNeededError, AuthKeyUnregisteredError, AuthKeyNotFound
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from bot.utils.channel_cache import channel_cache

# Функция для получения списка каналов из конфигурации и файла sources.txt
def get_all_source_channels():
    """Получает список каналов из конфигурации и файла sources.txt"""
    source_channels = set()
    
    # Добавляем каналы из конфигурации
    if 'channels' in CONFIG and 'source' in CONFIG['channels']:
        for channel in CONFIG['channels']['source']:
            source_channels.add(channel)
            
    # Проверяем наличие файла sources.txt
    sources_file = CONFIG.get('sources_file', 'sources.txt')
    if os.path.exists(sources_file):
        try:
            with open(sources_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        source_channels.add(line)
            logger.info(f"Загружены каналы из файла {sources_file}")
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {sources_file}: {e}")
    
    # Преобразуем имена каналов в числовые идентификаторы, если это строки
    result = []
    for channel in source_channels:
        try:
            # Если канал задан числом, добавляем как есть
            if isinstance(channel, int) or (isinstance(channel, str) and channel.isdigit()):
                result.append(int(channel))
            # Иначе это username, добавляем как строку
            else:
                result.append(channel)
        except:
            # В случае ошибки добавляем как строку
            result.append(str(channel))
    
    logger.info(f"Всего получено {len(result)} каналов-источников")
    return result

# Класс для управления ограничением скорости запросов к API Telegram
class RateLimiter:
    """Класс для управления скоростью запросов к API Telegram"""
    
    def __init__(self, requests_per_minute=20):
        """
        Инициализирует ограничитель скорости.
        
        Args:
            requests_per_minute: Максимальное количество запросов в минуту
        """
        self.requests_per_minute = requests_per_minute
        self.interval = 60 / requests_per_minute  # Интервал между запросами в секундах
        self.last_request_time = 0
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        """
        Приобретает разрешение на выполнение запроса.
        Блокирует выполнение, если необходимо соблюдать интервал.
        """
        async with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            
            # Если с момента последнего запроса прошло меньше интервала, ждем
            if elapsed < self.interval:
                wait_time = self.interval - elapsed
                await asyncio.sleep(wait_time)
                
            # Обновляем время последнего запроса
            self.last_request_time = time.time()

# Глобальный кэш для сущностей каналов
_channel_entity_cache = {}

# После инициализации ChannelCache добавим миграцию
from bot.utils.channel_cache import channel_cache
channel_cache.migrate_old_cache(_channel_entity_cache)

# Функция для получения сущности канала
async def resolve_channel_entity(client, channel_id):
    """
    Разрешает идентификатор канала в объект сущности Telegram.
    Использует кэширование для уменьшения количества запросов.
    
    Args:
        client: Экземпляр TelegramClient
        channel_id: Числовой ID или строка с именем канала
        
    Returns:
        Объект сущности канала или None при ошибке
    """
    # Проверяем кэш в памяти
    cache_key = str(channel_id)
    if cache_key in _channel_entity_cache:
        return _channel_entity_cache[cache_key]

    # Затем проверяем SQLite кэш
    cached = channel_cache.get_channel(channel_id)
    if cached:
        logger.info(f"Восстановлена сущность канала {channel_id} из кэша БД")
        entity = InputPeerChannel(
            channel_id=cached[0],
            access_hash=cached[1]
        )
        _channel_entity_cache[cache_key] = entity
        return entity
    
    try:
        # Для числовых ID используем InputPeerChannel напрямую, минуя ResolveUsernameRequest
        if isinstance(channel_id, int) or (isinstance(channel_id, str) and channel_id.isdigit()):
            # Преобразуем строку в число если нужно
            numeric_id = int(channel_id)
            from telethon.tl.types import InputPeerChannel, PeerChannel
            
            try:
                # Пытаемся получить сущность напрямую через InputPeerChannel
                # Используем случайное значение для access_hash, которое будет обновлено при запросе
                input_peer = InputPeerChannel(channel_id=numeric_id, access_hash=0)
                entity = await client.get_entity(input_peer)
                _channel_entity_cache[cache_key] = entity
                logger.info(f"Получена сущность для канала {channel_id} через InputPeerChannel")
                return entity
            except Exception:
                # Если не удалось получить через InputPeerChannel, используем обычный метод
                entity = await client.get_entity(PeerChannel(numeric_id))
                _channel_entity_cache[cache_key] = entity
                logger.info(f"Получена сущность для канала {channel_id} через PeerChannel")
                return entity
        
        # Для username используем get_entity, который вызывает ResolveUsernameRequest
        entity = await client.get_entity(channel_id)
        _channel_entity_cache[cache_key] = entity
        logger.info(f"Получена сущность для канала {channel_id}")
        return entity
    except ValueError as e:
        logger.warning(f"Не удалось получить сущность для канала {channel_id}: {e}")
        return None
    except FloodWaitError as e:
        wait_time = e.seconds
        logger.warning(f"FloodWait при получении сущности для канала {channel_id}: необходимо подождать {wait_time} секунд")
        # Возвращаем None и сохраняем ошибку в кэше на короткое время
        _channel_entity_cache[cache_key] = None
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении сущности для канала {channel_id}: {e}")
        return None

    # После успешного получения сущности сохраняем в кэш
    try:
        channel_data = {
            'id': entity.id,
            'access_hash': entity.access_hash,
            'title': getattr(entity, 'title', ''),
            'username': getattr(entity, 'username', None)
        }
        channel_cache.save_channel(channel_data)
    except Exception as e:
        logger.error(f"Ошибка сохранения в кэш: {e}")

    return entity

# Импортируем созданные модули
from bot.data.storage import ParserStateStorage
from bot.utils.retry import retry_with_backoff, RetryError
from bot.utils.monitoring import HealthMonitor

# Класс монитора активности
class ActivityMonitor:
    """Монитор активности парсера для обнаружения зависаний и таймаутов"""
    
    def __init__(self, timeout=180, callback=None):
        """
        Инициализирует монитор активности.
        
        Args:
            timeout: Таймаут в секундах, после которого вызывается callback
            callback: Функция, вызываемая при обнаружении таймаута
        """
        self.timeout = timeout
        self.callback = callback
        self.last_activity = time.time()
        self.is_monitoring = False
        self.monitor_task = None
        
    def start_monitoring(self):
        """Запускает мониторинг активности"""
        if self.is_monitoring:
            return
            
        self.is_monitoring = True
        self.last_activity = time.time()
        self.monitor_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"Запущен мониторинг активности с таймаутом {self.timeout} секунд")
        
    async def update_activity(self):
        """Обновляет время последней активности"""
        self.last_activity = time.time()
        
    async def _monitoring_loop(self):
        """Основной цикл мониторинга"""
        try:
            while self.is_monitoring:
                current_time = time.time()
                elapsed = current_time - self.last_activity
                
                # Если прошло слишком много времени без активности
                if elapsed > self.timeout:
                    logger.warning(f"Обнаружен таймаут активности: {elapsed:.1f} секунд без обновления")
                    
                    if self.callback:
                        try:
                            self.callback()
                            logger.info("Вызван обработчик таймаута активности")
                        except Exception as e:
                            logger.error(f"Ошибка в обработчике таймаута: {e}")
                    
                    # Сбрасываем таймер, чтобы не вызывать обработчик слишком часто
                    self.last_activity = time.time()
                
                # Проверяем каждые 5 секунд
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            logger.info("Задача мониторинга активности отменена")
        except Exception as e:
            logger.error(f"Ошибка в цикле мониторинга активности: {e}")
        finally:
            self.is_monitoring = False
            
    def stop_monitoring(self):
        """Останавливает мониторинг активности"""
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            logger.info("Мониторинг активности остановлен")

class Parser:
    def __init__(self, client, storage, config, monitor):
        """Инициализирует парсер"""
        self.client = client
        self.storage = storage
        self.config = config
        self.monitor = monitor
        
        # Добавляем настройки конкурентности и механизм остановки
        self.max_concurrent_requests = config.get('parser', {}).get('max_concurrent_requests', 5)
        self.stop_event = asyncio.Event()
        self.is_running = False
        
        logger.info(f"Парсер инициализирован. Макс. конкурентных запросов: {self.max_concurrent_requests}")
        
        # Основные параметры
        self.api_id = CONFIG['telegram']['api_id']
        self.api_hash = CONFIG['telegram']['api_hash']
        self.phone = CONFIG['telegram']['phone']
        self.temp_dir = tempfile.mkdtemp(prefix="telethon_media_")
        self.start_time = datetime.now(timezone.utc)
        
        # Путь к сессии
        self.session_path = CONFIG.get('session_path', os.path.join(CONFIG.get('session_dir', 'sessions'), self.phone))
        logger.info(f"Путь к файлу сессии: {self.session_path}")
        
        # Персистентное хранилище состояния
        self.storage = ParserStateStorage(storage_file="parser_state.json")
        
        # Инициализируем структуры данных из загруженного состояния
        self.last_message_ids = {}
        self.processed_album_ids = set(self.storage.state["processed_albums"])
        
        # Кэширование информации о каналах
        self.channel_cache = {}  # username -> is_channel
        
        # Отслеживание активности каналов
        self.channel_activity = {}  # channel_id -> {last_post_date, check_interval}
        self.default_interval = CONFIG['settings']['parse_interval']
        self.low_activity_interval = self.default_interval * 6  # в 6 раз реже проверяем неактивные каналы
        self.error_retry_interval = config.get('parser', {}).get('error_retry_interval', 300)  # интервал повтора после ошибки
        
        # Параметры для экспоненциального backoff
        self.max_retries = 10
        self.base_retry_delay = 5  # начальная задержка 5 секунд
        self.max_retry_delay = 300  # максимальная задержка 5 минут
        self.connection_attempts = 0
        
        # Инициализируем ограничитель скорости запросов
        self.rate_limiter = RateLimiter(requests_per_minute=40)  # Увеличиваем до 40 запросов в минуту
        
        # Флаг активной блокировки по FloodWait
        self.flood_wait_active = False
        self.flood_wait_until = None
        
        # Регистрируем обработчики для грациозного завершения
        self._setup_signal_handlers()
        atexit.register(self._cleanup_resources)
        
        logger.info(f"Парсер инициализирован, время запуска: {self.start_time}")
        
    def _setup_signal_handlers(self):
        """Настраивает обработчики сигналов для грациозного завершения"""
        try:
            # На Unix системах
            signal.signal(signal.SIGINT, self._handle_shutdown)
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            logger.info("Зарегистрированы обработчики сигналов для грациозного завершения")
        except (AttributeError, ValueError) as e:
            # На Windows некоторые сигналы могут быть недоступны
            logger.warning(f"Не удалось зарегистрировать обработчики сигналов: {e}")
        
    def _handle_shutdown(self, signum, frame):
        """Обработчик сигналов завершения"""
        logger.info(f"Получен сигнал {signum}, начинаем грациозное завершение")
        self.is_running = False
        
        # Создаем задачу для корректной очистки ресурсов
        # Используем asyncio.get_event_loop().create_task вместо asyncio.create_task
        # для совместимости с разными версиями Python
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self._cleanup_resources())
        else:
            # Если цикл событий не запущен, выполняем синхронно
            try:
                import shutil
                shutil.rmtree(self.temp_dir, ignore_errors=True)
                logger.info(f"Временная директория {self.temp_dir} удалена (синхронно)")
            except Exception as e:
                logger.error(f"Ошибка при удалении временной директории: {e}")
        
    async def _cleanup_resources(self):
        """Освобождает ресурсы при завершении работы"""
        logger.info("Очистка ресурсов парсера...")
        
        # Сохраняем состояние
        if hasattr(self, 'storage') and self.storage:
            self.storage.save_state()
            
        # Останавливаем мониторинг
        if self.monitor:
            try:
                await self.monitor.stop_monitoring()
                logger.info("Мониторинг остановлен")
            except Exception as e:
                logger.error(f"Ошибка при остановке мониторинга: {e}")
            
        # Отключаемся от Telegram
        if self.client and self.client.is_connected():
            try:
                await self.client.disconnect()
                logger.info("Соединение с Telegram закрыто")
            except Exception as e:
                logger.error(f"Ошибка при отключении от Telegram: {e}")
                
        # Удаляем временную директорию
        try:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info(f"Временная директория {self.temp_dir} удалена")
        except Exception as e:
            logger.error(f"Ошибка при удалении временной директории: {e}")
        
        logger.info("Очистка ресурсов завершена")
        
    def set_bot(self, bot):
        self.bot = bot
        
        # Инициализируем монитор работоспособности
        if self.bot and not self.monitor:
            self.monitor = HealthMonitor(
                notify_func=self._send_notification,
                admin_ids=CONFIG['settings']['admins']
            )
            # Запускаем мониторинг в фоновом режиме
            asyncio.create_task(self.monitor.start_monitoring())
            
        logger.info("Привязан экземпляр бота к парсеру")
        
    async def _send_notification(self, user_id, message):
        """Отправляет уведомление через бота"""
        if self.bot:
            try:
                await self.bot.send_message(user_id, message)
                return True
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления: {e}")
        return False
        
    async def initialize_channel_activity(self):
        """Инициализирует отслеживание активности каналов"""
        # Получаем список каналов из конфигурации и файла sources.txt
        source_channels = get_all_source_channels()
        
        # Ограничим число каналов для первичной инициализации, чтобы избежать FloodWait
        max_initial_channels = min(30, len(source_channels))
        if len(source_channels) > max_initial_channels:
            logger.warning(f"Слишком много каналов ({len(source_channels)}), инициализируем только {max_initial_channels} для первого запуска")
            # Предпочитаем числовые ID, так как они меньше вызывают FloodWait
            numeric_channels = [c for c in source_channels if isinstance(c, int) or (isinstance(c, str) and c.isdigit())]
            if len(numeric_channels) >= max_initial_channels:
                source_channels = numeric_channels[:max_initial_channels]
            else:
                # Если недостаточно числовых ID, берем случайные каналы
                random.shuffle(source_channels)
                source_channels = source_channels[:max_initial_channels]
        
        logger.info(f"Начало инициализации активности для {len(source_channels)} каналов")
        
        # Обрабатываем каналы небольшими партиями, чтобы избежать FloodWait
        batch_size = 10  # Увеличиваем размер партии до 10 каналов
        initialized_channels = 0
        
        for i in range(0, len(source_channels), batch_size):
            # Проверяем активную FloodWait блокировку
            if await self.check_flood_wait():
                logger.warning("Активна блокировка FloodWait, прерываем инициализацию каналов")
                # Сохраняем то, что успели инициализировать
                break
                
            batch = source_channels[i:i+batch_size]
            
            # Создаем задачи для асинхронной инициализации каналов в текущей партии
            tasks = []
            for channel_id in batch:
                task = asyncio.create_task(self._initialize_channel(channel_id))
                tasks.append(task)
                
                # Добавляем небольшую задержку между созданием задач
                await asyncio.sleep(0.3)  # Короткая пауза между задачами
            
            # Ждем завершения всех задач текущей партии
            for task in asyncio.as_completed(tasks):
                try:
                    result = await task
                    if result:
                        initialized_channels += 1
                except FloodWaitError as e:
                    # Если получили FloodWait, устанавливаем блокировку и прерываем цикл
                    wait_seconds = e.seconds
                    logger.warning(f"FloodWait при инициализации каналов: необходимо подождать {wait_seconds} секунд")
                    self.set_flood_wait(wait_seconds)
                    # Прерываем обработку
                    break
                except Exception as e:
                    logger.error(f"Ошибка при инициализации канала: {e}")
            
            # Проверяем, не установлена ли блокировка FloodWait
            if self.flood_wait_active:
                logger.warning("Обнаружена блокировка FloodWait, прерываем инициализацию каналов")
                break
                
            # Делаем паузу между партиями, чтобы избежать FloodWait
            if i + batch_size < len(source_channels):
                pause_time = random.uniform(5.0, 8.0)  # Уменьшаем паузу до 5-8 секунд
                logger.info(f"Инициализировано {initialized_channels} каналов. Пауза {pause_time:.1f} сек перед следующей партией")
                await asyncio.sleep(pause_time)
                
        logger.info(f"Инициализация активности завершена для {initialized_channels} каналов из {len(source_channels)}")
        
        # Сохраняем состояние после инициализации
        self.storage.save_state()
        
    async def _initialize_channel(self, channel_id):
        """Инициализирует активность для одного канала"""
        channel_id_str = str(channel_id)
        
        # Пытаемся загрузить сохраненные данные
        saved_activity = self.storage.get_channel_activity(channel_id)
        
        if saved_activity:
            # Преобразуем строковые даты обратно в datetime
            try:
                saved_activity['last_post_date'] = datetime.fromisoformat(saved_activity['last_post_date'])
                saved_activity['next_check'] = datetime.fromisoformat(saved_activity['next_check'])
                self.channel_activity[channel_id] = saved_activity
                logger.info(f"Загружена активность канала {channel_id} из сохраненного состояния")
                return True
            except (KeyError, ValueError) as e:
                logger.warning(f"Ошибка при загрузке активности канала {channel_id}: {e}")
        
        # Если нет сохраненных данных или они повреждены, получаем информацию через API
        try:
            # Добавляем небольшую случайную задержку перед запросом для распределения нагрузки
            await asyncio.sleep(random.uniform(0.1, 0.5))  # Уменьшаем задержку
            
            # Получаем канал через механизм разрешения сущностей
            entity = await resolve_channel_entity(self.client, channel_id)
            if not entity:
                logger.warning(f"Не удалось получить сущность для канала {channel_id}, пропускаем")
                return False
            
            # Добавляем еще небольшую задержку после получения сущности
            await asyncio.sleep(random.uniform(0.1, 0.3))  # Уменьшаем задержку
            
            # Получаем последнее сообщение канала
            try:
                history = await retry_with_backoff(
                    lambda: self.client(GetHistoryRequest(
                        peer=entity,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        limit=1,
                        max_id=0,
                        min_id=0,
                        hash=0
                    )),
                    log_prefix=f"Получение истории канала {channel_id}",
                    max_attempts=5,  # Уменьшаем количество попыток, чтобы не перегружать API
                    base_delay=2.0,   # Увеличиваем базовую задержку между попытками
                    max_delay=60.0    # Ограничиваем максимальную задержку 60 секундами
                )
            except FloodWaitError as e:
                # Если получили FloodWaitError, логируем и пропускаем канал
                wait_time = e.seconds
                logger.warning(f"FloodWait при инициализации канала {channel_id}: необходимо подождать {wait_time} секунд")
                
                # Если время ожидания слишком большое, сохраняем значения по умолчанию
                if wait_time > 60:  # Если ждать нужно больше минуты
                    self.channel_activity[channel_id] = {
                        'last_post_date': datetime.now(timezone.utc),
                        'check_interval': self.low_activity_interval,
                        'next_check': datetime.now(timezone.utc) + timedelta(seconds=wait_time)
                    }
                    self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
                    return False
                
                # Иначе ждем и пробуем еще раз
                await asyncio.sleep(wait_time)
                # Повторно пытаемся получить историю, но уже без повторов
                try:
                    history = await self.client(GetHistoryRequest(
                        peer=entity,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        limit=1,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))
                except Exception as e2:
                    logger.error(f"Повторная ошибка при получении истории канала {channel_id}: {e2}")
                    # Устанавливаем значения по умолчанию
                    self.channel_activity[channel_id] = {
                        'last_post_date': datetime.now(timezone.utc),
                        'check_interval': self.low_activity_interval,
                        'next_check': datetime.now(timezone.utc) + timedelta(hours=1)  # Отложить проверку на час
                    }
                    self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
                    return False
            except Exception as e:
                logger.error(f"Ошибка при получении истории канала {channel_id}: {e}")
                # Устанавливаем значения по умолчанию с долгим интервалом проверки
                self.channel_activity[channel_id] = {
                    'last_post_date': datetime.now(timezone.utc),
                    'check_interval': self.low_activity_interval,
                    'next_check': datetime.now(timezone.utc) + timedelta(minutes=30)  # Отложить проверку на 30 минут
                }
                self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
                return False
            
            if history.messages:
                last_message = history.messages[0]
                # Сохраняем дату последнего сообщения и устанавливаем интервал проверки
                last_post_date = last_message.date
                
                # Определяем интервал проверки на основе активности
                now = datetime.now(timezone.utc)
                days_since_last_post = (now - last_post_date).days
                
                if days_since_last_post > 3:
                    # Неактивный канал - проверяем реже
                    check_interval = self.low_activity_interval
                    logger.info(f"Канал {channel_id} неактивен {days_since_last_post} дней, интервал: {check_interval} сек")
                else:
                    # Активный канал - проверяем чаще
                    check_interval = self.default_interval
                    logger.info(f"Канал {channel_id} активен, последний пост: {last_post_date}, интервал: {check_interval} сек")
                
                self.channel_activity[channel_id] = {
                    'last_post_date': last_post_date,
                    'check_interval': check_interval,
                    'next_check': now
                }
            else:
                # Если нет сообщений, устанавливаем долгий интервал
                self.channel_activity[channel_id] = {
                    'last_post_date': datetime.now(timezone.utc) - timedelta(days=10),
                    'check_interval': self.low_activity_interval,
                    'next_check': datetime.now(timezone.utc)
                }
                logger.info(f"Канал {channel_id} пуст, установлен долгий интервал проверки")
                
            # Сохраняем данные в хранилище
            self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
            return True
                
        except Exception as e:
            logger.error(f"Ошибка при инициализации активности канала {channel_id}: {e}")
            # Устанавливаем значения по умолчанию
            self.channel_activity[channel_id] = {
                'last_post_date': datetime.now(timezone.utc),
                'check_interval': self.low_activity_interval,
                'next_check': datetime.now(timezone.utc) + timedelta(minutes=15)  # Добавляем задержку
            }
            self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
            return False
    
    async def is_channel(self, username):
        """Проверяет, является ли username каналом с кэшированием результата"""
        if username in self.channel_cache:
            return self.channel_cache[username]
            
        try:
            entity = await self.client.get_entity(username)
            from telethon.tl.types import Channel
            result = isinstance(entity, Channel)
            self.channel_cache[username] = result
            return result
        except Exception as e:
            logger.warning(f"Ошибка при проверке {username}: {e}")
            return False  # Если не удалось проверить, предполагаем, что это не канал
    
    async def filter_telegram_links_precise(self, text):
        """Точная фильтрация ссылок на каналы с проверкой через API"""
        if not text:
            return text
            
        # Удаляем прямые ссылки на каналы и приглашения
        text = re.sub(
            r'https?://(?:t(?:elegram)?\.me|telegram\.dog)/(?:joinchat/|\+)?[a-zA-Z0-9_\-]+',
            '[ссылка удалена]',
            text
        )
        
        # Находим все @упоминания
        mentions = re.findall(r'@([a-zA-Z0-9_]+)', text)
        
        # Проверяем каждое упоминание через API
        for mention in mentions:
            if await self.is_channel(mention):
                text = text.replace(f'@{mention}', '[канал удален]')
        
        return text
    
    async def connect_with_retry(self):
        """Подключается к Telegram API с множественными попытками в случае ошибок"""
        logger.info(f"Используется сессия: {self.session_path}")
        
        # Устанавливаем более обширные параметры клиента для улучшения надежности
        session_param = self.session_path
        # Проверяем наличие session string в конфигурации
        if 'session_string' in CONFIG['telegram'] and CONFIG['telegram']['session_string'] and CONFIG['telegram']['session_string'] != "ВАШ_SESSION_STRING":
            session_param = CONFIG['telegram']['session_string']
            logger.info("Используется session string из конфигурации")
        
        self.client = TelegramClient(
            session_param,
            CONFIG['telegram']['api_id'],
            CONFIG['telegram']['api_hash'],
            device_model="Telegram Parser Bot",
            system_version="Python 3.x",
            app_version="1.0.0",
            timeout=30,  # увеличенный таймаут для стабильности
            connection_retries=5,  # уменьшаем количество автоматических попыток
            retry_delay=5  # увеличиваем задержку между автоматическими попытками
        )
        
        # Получаем коллбэки для авторизации
        callbacks = await self.create_code_callback()
        code_callback = callbacks["code_callback"]
        password_callback = callbacks["password_callback"]
        
        attempts = 0
        max_attempts = 5
        
        while attempts < max_attempts:
            try:
                logger.info(f"Попытка подключения к Telegram (попытка {attempts + 1}/{max_attempts})...")
                await self.client.connect()
                
                # Проверяем, авторизован ли пользователь
                if not await self.client.is_user_authorized():
                    logger.info(f"Начало авторизации для номера {self.phone}")
                    await self.client.send_code_request(self.phone)
                    logger.info("Запрос кода авторизации отправлен")
                    
                    # Запрашиваем код через callback
                    code = await code_callback()
                    if not code:
                        logger.error("Не получен код авторизации")
                        attempts += 1
                        continue
                        
                    # Вводим полученный код
                    await self.client.sign_in(self.phone, code)
                
                # Проверяем успешное подключение
                me = await self.client.get_me()
                if me:
                    logger.info(f"Успешно авторизован как {me.first_name} (ID: {me.id})")
                    # Сохраняем имя пользователя и идентификатор для использования в логах
                    self.user_info = {
                        'id': me.id,
                        'name': f"{me.first_name} {me.last_name if me.last_name else ''}".strip(),
                        'username': me.username
                    }
                    # Получаем и логируем session string для будущей автоматизации
                    session_string = self.client.session.save()
                    logger.info(f"ВАШ SESSION STRING (сохраните для автоматизации): {session_string}")
                    logger.info(f"Добавьте его в config.json или как TELEGRAM_SESSION_STRING в переменные окружения")
                    return True
            except PhoneCodeInvalidError:
                logger.error("Введен неверный код подтверждения")
                # Повторно запрашиваем код
                continue
            except PhoneNumberBannedError:
                logger.critical(f"Номер {self.phone} заблокирован в Telegram")
                # Это критическая ошибка, дальнейшие попытки бессмысленны
                break
            except SessionPasswordNeededError:
                logger.info("Требуется пароль двухфакторной аутентификации")
                try:
                    # Сначала пытаемся использовать пароль из конфигурации
                    if 'two_fa_password' in CONFIG['telegram'] and CONFIG['telegram']['two_fa_password']:
                        password = CONFIG['telegram']['two_fa_password']
                        logger.info("Используем пароль 2FA из конфигурации")
                    else:
                        # Если в конфигурации нет пароля, запрашиваем его через callback
                        logger.info("Пароль 2FA не найден в конфигурации, запрашиваем у администратора")
                        password = await password_callback()
                        
                    if not password:
                        logger.error("Не получен пароль двухфакторной аутентификации")
                        attempts += 1
                        continue
                    
                    # Вводим полученный пароль
                    await self.client.sign_in(password=password)
                    logger.info("Двухфакторная аутентификация успешна")
                    return True
                except Exception as e:
                    logger.error(f"Ошибка при вводе пароля двухфакторной аутентификации: {e}")
            except (AuthKeyUnregisteredError, AuthKeyNotFound) as e:
                logger.error(f"Ошибка ключа авторизации: {e}. Пересоздание сессии...")
                # При ошибке с ключом авторизации полностью пересоздаем сессию
                try:
                    if os.path.exists(f"{self.session_path}.session"):
                        os.remove(f"{self.session_path}.session")
                        logger.info(f"Файл сессии {self.session_path}.session удален")
                except Exception as file_error:
                    logger.error(f"Не удалось удалить файл сессии: {file_error}")
            except FloodWaitError as e:
                wait_time = e.seconds
                logger.warning(f"FloodWait: необходимо подождать {wait_time} секунд перед следующей попыткой")
                
                # Для слишком больших задержек завершаем работу с ошибкой
                if wait_time > 3600:  # Более часа
                    logger.critical(f"API Telegram требует подождать {wait_time/3600:.1f} часов. Прерывание работы.")
                    
                    # Отправляем уведомление администраторам
                    if self.bot:
                        for admin_id in CONFIG['settings']['admins']:
                            try:
                                await self.bot.send_message(
                                    chat_id=admin_id, 
                                    text=f"⚠️ КРИТИЧЕСКАЯ ОШИБКА\n\n"
                                         f"API Telegram требует подождать {wait_time/3600:.1f} часов.\n\n"
                                         f"Работа бота прервана. Повторите попытку позже."
                                )
                            except:
                                pass
                    
                    return False
                
                # Для небольших задержек ждем и пробуем снова
                logger.info(f"Ожидание {wait_time} секунд...")
                await asyncio.sleep(min(wait_time + 5, 300))  # Ожидаем указанное время + 5 сек, но не более 5 минут
            except Exception as e:
                logger.error(f"Ошибка подключения: {e}")
            
            # Увеличиваем время ожидания экспоненциально
            backoff = min(300, (2 ** attempts) * 5)  # 5, 10, 20, 40, ... до 300 секунд максимум
            logger.info(f"Повторная попытка через {backoff} секунд...")
            await asyncio.sleep(backoff)
            attempts += 1
            
            # Отключаем клиент перед новой попыткой, если он был создан
            if self.client:
                try:
                    await self.client.disconnect()
                except:
                    pass
                
        logger.critical(f"Не удалось подключиться к Telegram после {max_attempts} попыток")
        return False
                
    async def create_code_callback(self):
        """Создает обработчик для получения кода авторизации от администратора"""
        # Получаем словарь для хранения кодов из message_handler
        from bot.handlers.message_handler import auth_codes
        
        # Временный обработчик для получения кода от админа
        async def code_callback():
            # Отправляем сообщение всем администраторам с просьбой прислать код
            for admin_id in CONFIG['settings']['admins']:
                try:
                    await self.bot.send_message(
                        chat_id=admin_id,
                        text="Требуется код авторизации. Пожалуйста, отправьте код из SMS в течение 5 минут."
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить запрос кода админу {admin_id}: {e}")
            
            # Ждем 5 минут для получения кода от любого админа
            timeout = time.time() + 300  # 5 минут
            
            while time.time() < timeout:
                # Проверяем, не пришел ли код от какого-либо админа
                for admin_id, code in auth_codes.items():
                    if code:
                        result = code
                        # Очищаем код после использования
                        auth_codes[admin_id] = None
                        return result
                
                # Ждем немного и проверяем снова
                await asyncio.sleep(2)
            
            # Если время истекло, возвращаем None
            logger.error("Время ожидания кода авторизации истекло")
            return None
        
        # Временный обработчик для получения пароля двухфакторной аутентификации
        async def password_callback():
            # Отправляем сообщение всем администраторам с просьбой прислать пароль
            for admin_id in CONFIG['settings']['admins']:
                try:
                    await self.bot.send_message(
                        chat_id=admin_id,
                        text="Требуется пароль двухфакторной аутентификации. Пожалуйста, отправьте пароль в течение 5 минут."
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить запрос пароля админу {admin_id}: {e}")
            
            # Ждем 5 минут для получения пароля от любого админа
            timeout = time.time() + 300  # 5 минут
            
            while time.time() < timeout:
                # Проверяем, не пришел ли пароль от какого-либо админа
                for admin_id, code in auth_codes.items():
                    if code:
                        result = code
                        # Очищаем код после использования
                        auth_codes[admin_id] = None
                        return result
                
                # Ждем немного и проверяем снова
                await asyncio.sleep(2)
            
            # Если время истекло, возвращаем None
            logger.error("Время ожидания пароля двухфакторной аутентификации истекло")
            return None
        
        # В Telethon нет методов set_callback и set_password_callback
        # Вместо этого мы будем использовать эти функции при запросе кода
        # и пароля в методе connect_with_retry
        
        return {"code_callback": code_callback, "password_callback": password_callback}
    
    async def api_request_with_retry(self, request_func, *args, **kwargs):
        """Выполняет API запрос с автоматическими повторными попытками при ошибках сети"""
        try:
            # Используем улучшенную функцию retry из utils
            return await retry_with_backoff(
                lambda: request_func(*args, **kwargs),
                max_attempts=self.max_retries,
                base_delay=self.base_retry_delay,
                max_delay=self.max_retry_delay,
                log_prefix="API запрос"
            )
        except RetryError as e:
            logger.error(f"Не удалось выполнить запрос после {e.attempts_made} попыток: {e.original_error}")
                
            # Если клиент отключился из-за ошибки, переподключаемся
            if not self.client.is_connected():
                logger.info("Клиент отключен, выполняем переподключение...")
            await self.connect_with_retry()
            
            # Записываем ошибку в монитор
            if self.monitor:
                self.monitor.record_error(
                    f"Ошибка API запроса после {e.attempts_made} попыток: {e.original_error}"
                )
                
            raise
    
    def _get_channels_to_check(self):
        """Возвращает список каналов, которые нужно проверить в текущей итерации"""
        # Если список каналов пуст, заполняем его из списка источников
        if not self.channel_activity:
            logger.warning("Список каналов пуст, загружаем из списка источников")
            source_channels = get_all_source_channels()
            current_time = datetime.now(timezone.utc)
            
            # Заполняем активность каналов значениями по умолчанию
            for channel_id in source_channels:
                # Преобразуем строковые имена к @username формату для удобства
                if isinstance(channel_id, str) and not channel_id.isdigit() and not channel_id.startswith('@'):
                    channel_key = '@' + channel_id
                else:
                    channel_key = channel_id
                    
                self.channel_activity[channel_key] = {
                    'last_post_date': current_time,
                    'check_interval': self.default_interval,
                    'next_check': current_time  # Проверяем сразу
                }
                
        # Получаем текущее время для сравнения
        current_time = datetime.now(timezone.utc)
        channels_to_check = []
        
        # Перебираем все каналы
        for channel_id, activity in self.channel_activity.items():
            # Если время следующей проверки наступило или уже прошло
            if 'next_check' in activity and current_time >= activity['next_check']:
                channels_to_check.append(channel_id)
                # Обновляем время следующей проверки
                if 'check_interval' in activity:
                    interval = activity['check_interval']
                else:
                    interval = self.default_interval
                    
                self.channel_activity[channel_id]['next_check'] = current_time + timedelta(seconds=interval)
                
        # В случае отсутствия каналов для проверки, берем хотя бы несколько
        if not channels_to_check and self.channel_activity:
            # Берем 5 случайных каналов для проверки
            all_channels = list(self.channel_activity.keys())
            random.shuffle(all_channels)
            # Предпочитаем каналы с числовыми ID, так как они реже вызывают ошибки
            numeric_channels = [c for c in all_channels if isinstance(c, int) or (isinstance(c, str) and c.isdigit())]
            if numeric_channels:
                channels_to_check = numeric_channels[:min(5, len(numeric_channels))]
            else:
                channels_to_check = all_channels[:min(5, len(all_channels))]
                
            logger.info("Нет каналов по расписанию, выбрано 5 случайных каналов")
            
            # Обновляем время следующей проверки для этих каналов
            for channel_id in channels_to_check:
                self.channel_activity[channel_id]['next_check'] = current_time + timedelta(
                    seconds=self.channel_activity[channel_id].get('check_interval', self.default_interval)
                )
                
        return channels_to_check
            
    async def _check_channel_messages(self, channel_id: int) -> bool:
        """
        Проверяет новые сообщения в указанном канале.
        
        Args:
            channel_id: ID канала для проверки
            
        Returns:
            True при успешной обработке, False при ошибке
        """
        try:
            # Получаем последний обработанный ID сообщения
            last_message_id = self.storage.get_last_message_id(channel_id)
            
            # Получаем объект канала
            entity = await self.api_request_with_retry(self.client.get_entity, channel_id)
            
            # Получаем новые сообщения
            messages = await self._fetch_channel_messages(entity, last_message_id)
            
            # Обрабатываем новые сообщения
            processed_count = await self._process_new_messages(channel_id, messages)
            
            # Обновляем информацию о канале
            current_time = datetime.now(timezone.utc)
            if processed_count and processed_count > 0:
                # Если были новые сообщения, устанавливаем стандартный интервал проверки
                self.channel_activity[channel_id] = {
                    'last_post_date': current_time,
                    'check_interval': self.default_interval,
                    'next_check': current_time + timedelta(seconds=self.default_interval)
                }
            else:
                # Если новых сообщений нет, увеличиваем интервал проверки
                self.channel_activity[channel_id] = {
                    'last_post_date': current_time - timedelta(days=1),
                    'check_interval': self.low_activity_interval,
                    'next_check': current_time + timedelta(seconds=self.low_activity_interval)
                }
            
            # Сохраняем данные канала
            self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке сообщений канала {channel_id}: {e}")
            logger.exception(e)
            
            # Обновляем активность канала для повторной попытки позже
            current_time = datetime.now(timezone.utc)
            self.channel_activity[channel_id] = {
                'last_post_date': current_time - timedelta(days=1),
                'check_interval': self.error_retry_interval,
                'next_check': current_time + timedelta(seconds=self.error_retry_interval)
            }
            self.storage.update_channel_activity(channel_id, self.channel_activity[channel_id])
            
            return False

    async def initialize_last_messages(self):
        """Инициализация: запоминаем ID последних сообщений для всех каналов"""
        logger.info("Инициализация: получение ID последних сообщений")
        source_channels = get_all_source_channels()
        
        for channel_id in source_channels:
            # Проверяем, есть ли уже сохраненный ID для этого канала
            last_id = self.storage.get_last_message_id(channel_id)
            if last_id > 0:
                logger.info(f"Канал {channel_id}: последний обработанный ID = {last_id} (из хранилища)")
                self.last_message_ids[channel_id] = last_id
                continue
                
            try:
                # Получаем объект канала с помощью функции из channel_utils
                entity = await resolve_channel_entity(self.client, channel_id)
                if not entity:
                    logger.warning(f"Не удалось получить entity для канала {channel_id}, пропускаем")
                    continue
                    
                history = await retry_with_backoff(
                    lambda: self.client(GetHistoryRequest(
                        peer=entity,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        limit=1,
                        max_id=0,
                        min_id=0,
                        hash=0
                    )),
                    log_prefix=f"Получение истории канала {channel_id}"
                )
                
                if history.messages:
                    last_message = history.messages[0]
                    self.last_message_ids[channel_id] = last_message.id
                    # Сохраняем в хранилище
                    self.storage.set_last_message_id(channel_id, last_message.id)
                    logger.info(f"Канал {channel_id}: последнее сообщение ID = {last_message.id}, дата = {last_message.date}")
            except Exception as e:
                logger.error(f"Ошибка при инициализации канала {channel_id}: {e}")
                
        # Сохраняем состояние после инициализации
        self.storage.save_state()

    def filter_message(self, text):
        if not text:
            return False
            
        text = text.lower()
        
        # Фильтрация по ключевым словам
        if not CONFIG['filters']['include']:
            has_include = True
        else:
            has_include = any(kw.lower() in text for kw in CONFIG['filters']['include'])
            
        has_exclude = any(kw.lower() in text for kw in CONFIG['filters']['exclude'])
        
        return has_include and not has_exclude

    async def download_media_with_retry(self, media, path):
        """Скачивает медиа с повторными попытками при ошибках сети"""
        try:
            return await retry_with_backoff(
                lambda: self.client.download_media(media, file=path),
                max_attempts=self.max_retries,
                base_delay=self.base_retry_delay,
                max_delay=self.max_retry_delay,
                log_prefix="Скачивание медиа"
            )
        except RetryError as e:
            logger.error(f"Не удалось скачать медиа после {e.attempts_made} попыток: {e.original_error}")
            
            # Записываем ошибку в монитор
            if self.monitor:
                self.monitor.record_error(f"Ошибка при скачивании медиа: {e.original_error}")
                
                # Если клиент отключился из-за ошибки, переподключаемся
                if not self.client.is_connected():
                    await self.connect_with_retry()
        
            raise

    async def process_album(self, source_id, messages):
        """Обрабатывает альбом сообщений с фильтрацией каналов"""
        if not self.bot or not messages:
            return
            
        for channel_id in CONFIG['channels']['target']:
            try:
                media_group = []
                media_files = []
                
                # Берем подпись из первого сообщения и фильтруем ссылки
                caption = messages[0].message if messages and hasattr(messages[0], 'message') else ""
                filtered_caption = await self.filter_telegram_links_precise(caption)
                
                for message in messages:
                    if not hasattr(message, 'media') or not message.media:
                        continue
                        
                    # Скачиваем медиа с поддержкой переподключений
                    try:
                        media_path = await self.download_media_with_retry(
                            message.media,
                            os.path.join(self.temp_dir, "")
                        )
                        media_files.append(media_path)
                        
                        # Определяем тип медиа
                        if media_path.endswith(('.jpg', '.jpeg', '.png', '.webp')):
                            media = InputMediaPhoto(
                                media=FSInputFile(media_path),
                                caption=filtered_caption if len(media_group) == 0 else None
                            )
                            media_group.append(media)
                            
                        elif media_path.endswith(('.mp4', '.avi', '.mov', '.mkv')):
                            media = InputMediaVideo(
                                media=FSInputFile(media_path),
                                caption=filtered_caption if len(media_group) == 0 else None
                            )
                            media_group.append(media)
                            
                        elif media_path.endswith(('.mp3', '.ogg', '.m4a', '.wav')):
                            media = InputMediaAudio(
                                media=FSInputFile(media_path),
                                caption=filtered_caption if len(media_group) == 0 else None
                            )
                            media_group.append(media)
                            
                        else:
                            media = InputMediaDocument(
                                media=FSInputFile(media_path),
                                caption=filtered_caption if len(media_group) == 0 else None
                            )
                            media_group.append(media)
                        
                    except Exception as e:
                        logger.error(f"Не удалось скачать медиа для альбома: {e}")
                        continue
                
                # Отправляем группу, если есть медиа
                if media_group:
                    await self.bot.send_media_group(
                        chat_id=channel_id,
                        media=media_group
                    )
                    logger.info(f"Альбом с {len(media_group)} медиа из {source_id} отправлен в {channel_id}")
                
                # Удаляем временные файлы
                for file_path in media_files:
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файл {file_path}: {e}")
                        
            except Exception as e:
                logger.error(f"Ошибка отправки альбома в {channel_id}: {e}")

    async def process_single_message(self, source_id, message):
        """Обрабатывает одиночное сообщение с фильтрацией каналов"""
        if not self.bot:
            logger.error("Не установлен экземпляр бота для отправки сообщений")
            return
            
        for channel_id in CONFIG['channels']['target']:
            try:
                # Получаем и фильтруем текст сообщения
                caption = message.message or ""
                filtered_caption = await self.filter_telegram_links_precise(caption)
                
                # Обрабатываем сообщение в зависимости от типа медиа
                if hasattr(message, 'media') and message.media:
                    try:
                        # Скачиваем медиа с поддержкой переподключений
                        media_path = await self.download_media_with_retry(
                            message.media, 
                            os.path.join(self.temp_dir, "")
                        )
                        
                        # Определяем тип медиа и отправляем с отфильтрованным текстом
                        if media_path.endswith(('.jpg', '.jpeg', '.png', '.webp')):
                            # Фото
                            await self.bot.send_photo(
                                chat_id=channel_id,
                                photo=FSInputFile(media_path),
                                caption=filtered_caption
                            )
                            
                        elif media_path.endswith(('.mp4', '.avi', '.mov', '.mkv')):
                            # Видео
                            await self.bot.send_video(
                                chat_id=channel_id,
                                video=FSInputFile(media_path),
                                caption=filtered_caption
                            )
                            
                        elif media_path.endswith(('.mp3', '.ogg', '.m4a', '.wav')):
                            # Аудио
                            await self.bot.send_audio(
                                chat_id=channel_id,
                                audio=FSInputFile(media_path),
                                caption=filtered_caption
                            )
                            
                        elif media_path.endswith('.gif'):
                            # Анимация/GIF
                            await self.bot.send_animation(
                                chat_id=channel_id,
                                animation=FSInputFile(media_path),
                                caption=filtered_caption
                            )
                            
                        elif media_path.endswith(('.tgs')):
                            # Стикер
                            await self.bot.send_sticker(
                                chat_id=channel_id,
                                sticker=FSInputFile(media_path)
                            )
                            # Если есть подпись, отправим ее отдельно
                            if filtered_caption:
                                await self.bot.send_message(
                                    chat_id=channel_id,
                                    text=filtered_caption
                                )
                                
                        else:
                            # Документ (любой другой тип файла)
                            await self.bot.send_document(
                                chat_id=channel_id,
                                document=FSInputFile(media_path),
                                caption=filtered_caption
                            )
                        
                        # Удаляем временный файл
                        try:
                            os.remove(media_path)
                        except:
                            pass
                            
                    except Exception as e:
                        logger.error(f"Не удалось скачать медиа для сообщения: {e}")
                        # Отправляем хотя бы текст, если не удалось скачать медиа
                        if filtered_caption:
                            await self.bot.send_message(
                                chat_id=channel_id,
                                text=f"Не удалось скачать медиа: {e}\n\n{filtered_caption}",
                                parse_mode='HTML'
                            )
                        continue
                            
                elif hasattr(message, 'poll'):
                    # Опрос
                    from aiogram.types import PollType
                    
                    await self.bot.send_poll(
                        chat_id=channel_id,
                        question=message.poll.question,
                        options=[answer.text for answer in message.poll.answers],
                        is_anonymous=message.poll.public_voters,
                        type=PollType.QUIZ if message.poll.quiz else PollType.REGULAR,
                        allows_multiple_answers=message.poll.multiple_choice
                    )
                    
                elif hasattr(message, 'message') and message.message:
                    # Просто текст с отфильтрованными ссылками
                    await self.bot.send_message(
                        chat_id=channel_id,
                        text=filtered_caption,
                        parse_mode='HTML'
                    )
                    
                else:
                    # Если тип сообщения не определен
                    logger.warning(f"Неизвестный тип сообщения: {type(message)}")
                    
                logger.info(f"Сообщение из {source_id} скопировано в {channel_id}")
            except Exception as e:
                logger.error(f"Ошибка копирования в {channel_id}: {e}")
    
    def __del__(self):
        """Очистка при уничтожении объекта"""
        # Обеспечиваем корректное завершение работы
        if asyncio.get_event_loop().is_running():
            asyncio.create_task(self._cleanup_resources())
        else:
            try:
                import shutil
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except:
                pass 

    async def _fetch_channel_messages(self, entity, last_message_id):
        """Получает новые сообщения из канала с учетом последнего обработанного ID"""
        try:
            # Проверяем, нет ли активной блокировки FloodWait
            if await self.check_flood_wait():
                logger.warning(f"Пропуск получения сообщений из канала {entity.id} из-за активной блокировки FloodWait")
                return []
                
            # Получаем историю сообщений с учетом ограничений скорости
            history = await self.rate_limited_request(
                self.client.get_messages,
                entity=entity,
                limit=30,  # Уменьшаем лимит для более эффективной обработки
                min_id=last_message_id  # Получаем только сообщения новее last_message_id
            )
            
            if not history:
                return []
                
            # Фильтруем сообщения, которые старше времени запуска
            return [msg for msg in history if msg.date >= self.start_time]
            
        except FloodWaitError as e:
            wait_seconds = e.seconds
            logger.warning(f"FloodWait при получении сообщений канала {entity.id}: {wait_seconds} сек")
            self.set_flood_wait(wait_seconds)
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из канала {entity.id}: {e}")
            logger.exception(e)
            return []
            
    async def _process_new_messages(self, channel_id, messages):
        """Обрабатывает новые сообщения из канала"""
        if not messages:
            return 0
            
        # Сортируем сообщения по ID (от старых к новым)
        messages.sort(key=lambda msg: msg.id)
        
        # Словарь для группировки сообщений в альбомы
        albums = defaultdict(list)
        processed_count = 0
        
        # Проходим по всем сообщениям
        for message in messages:
            try:
                # Фильтруем сообщения по ключевым словам
                if not self.filter_message(message.message or ""):
                    continue
                    
                # Обновляем последний обработанный ID
                self.storage.set_last_message_id(channel_id, message.id)
                
                # Группируем в альбомы, если есть grouped_id
                if hasattr(message, 'grouped_id') and message.grouped_id:
                    if not self.storage.is_album_processed(message.grouped_id):
                        albums[message.grouped_id].append(message)
                else:
                    # Одиночное сообщение
                    await self.process_single_message(channel_id, message)
                    
                # Инкрементируем счетчик обработанных сообщений
                self.storage.increment_messages_count()
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения {message.id} из канала {channel_id}: {e}")
                logger.exception(e)
        
        # Обрабатываем альбомы после обработки всех сообщений
        for grouped_id, album_messages in albums.items():
            try:
                if self.storage.is_album_processed(grouped_id):
                    continue
                    
                await self.process_album(channel_id, album_messages)
                self.storage.mark_album_processed(grouped_id)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке альбома {grouped_id} из канала {channel_id}: {e}")
                logger.exception(e)
        
        return processed_count
        
    def _process_monitoring_timeout(self):
        """Обработчик таймаута монитора активности"""
        logger.critical("Обнаружен таймаут активности парсера (180 секунд без обновления)!")
        
        # Записываем ошибку в основной монитор
        if self.monitor:
            self.monitor.record_error(
                "Критическая ошибка: обнаружен таймаут активности парсера (180 секунд)",
                is_critical=True
            )

    async def check_messages(self):
        """Проверяет новые сообщения во всех каналах"""
        try:
            logger.info("Проверка новых сообщений в каналах")
            
            # Получаем список каналов для проверки на основе их активности
            channels_to_check = self._get_channels_to_check()
            
            if not channels_to_check:
                logger.info("Нет каналов для проверки в текущей итерации")
                return
                
            logger.info(f"Запланировано проверить {len(channels_to_check)} каналов")
            
            # Ограничиваем количество каналов для проверки за один раз
            max_channels_per_batch = self.config.get('parser', {}).get('max_channels_per_batch', 20)  # Увеличиваем до 20
            
            # Если каналов слишком много, обрабатываем их частями
            if len(channels_to_check) > max_channels_per_batch:
                logger.info(f"Слишком много каналов ({len(channels_to_check)}), ограничиваем до {max_channels_per_batch}")
                # Перемешиваем список каналов для равномерной проверки
                random.shuffle(channels_to_check)
                channels_to_check = channels_to_check[:max_channels_per_batch]
            
            # Создаем семафор для ограничения числа одновременных запросов
            semaphore = asyncio.Semaphore(min(self.max_concurrent_requests, 10))  # Не более 10 одновременных запросов
            
            # Оборачиваем каждый вызов в корутину с семафором
            async def check_channel_with_semaphore(channel):
                async with semaphore:
                    try:
                        # Добавляем небольшую случайную задержку для распределения нагрузки
                        await asyncio.sleep(random.uniform(0.1, 0.3))  # Уменьшаем задержку
                        return await self._check_channel_messages(channel)
                    except FloodWaitError as e:
                        # Обрабатываем FloodWait ошибки
                        wait_seconds = e.seconds
                        self.set_flood_wait(wait_seconds)
                        logger.warning(f"FloodWait при проверке канала {channel}: {wait_seconds} сек")
                        return False
                    except Exception as e:
                        logger.error(f"Ошибка при проверке канала {channel}: {e}")
                        logger.exception(e)
                        return False
            
            # Запускаем параллельную обработку каналов
            start_time = time.time()
            tasks = [check_channel_with_semaphore(channel) for channel in channels_to_check]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Проверяем результаты
            success_count = sum(1 for r in results if r is True)
            error_count = sum(1 for r in results if isinstance(r, Exception) or r is False)
            
            # Сохраняем состояние
            self.storage.state["processed_albums"] = list(self.processed_album_ids)
            self.storage.save_state()
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logger.info(f"Проверка всех каналов завершена. Успешно: {success_count}, с ошибками: {error_count}. Время: {elapsed_time:.2f} сек.")
            
        except Exception as e:
            logger.error(f"Ошибка при проверке сообщений: {e}")
            logger.exception(e)

    async def stop(self):
        """Грациозная остановка парсера"""
        logger.info("Остановка парсера...")
        self.stop_event.set()
        
        # Даём время на завершение текущих операций
        try:
            # Ожидаем до 30 секунд для завершения текущих операций
            for _ in range(30):
                if not self.is_running:
                    break
                await asyncio.sleep(1)
            
            logger.info("Парсер успешно остановлен")
        except Exception as e:
            logger.error(f"Ошибка при остановке парсера: {e}")
            logger.exception(e)
        
        # Закрываем клиент Telegram
        if self.client:
            await self.client.disconnect()
            
        # Сохраняем состояние
        self.storage.save_state()

    async def run(self):
        """Запускает циклический процесс парсинга"""
        try:
            logger.info("Парсер запущен")
            self.is_running = True
            
            # Инициализация монитора активности
            activity_monitor = ActivityMonitor(timeout=180, callback=self._process_monitoring_timeout)
            activity_monitor.start_monitoring()
            
            # Инициализируем информацию о каналах перед началом работы
            logger.info("Инициализация информации о каналах")
            await self.initialize_last_messages()
            await self.initialize_channel_activity()
            logger.info("Инициализация каналов завершена")
            
            # Основной цикл работы парсера
            while not self.stop_event.is_set():
                try:
                    # Обновляем активность в мониторе
                    await activity_monitor.update_activity()
                    
                    # Проверяем, нет ли активной блокировки FloodWait
                    if await self.check_flood_wait():
                        # Если блокировка активна, делаем длинную паузу
                        logger.info("Активна блокировка FloodWait, ожидание 15 минут перед повторной проверкой...")
                        
                        # Делаем паузу по 1 секунде, чтобы можно было прервать работу
                        for _ in range(900):  # 15 минут
                            if self.stop_event.is_set():
                                logger.info("Получен сигнал остановки во время ожидания FloodWait")
                                break
                            await asyncio.sleep(1)
                        continue
                    
                    # Проверяем сообщения в каналах
                    await self.check_messages()
                    
                    # Пауза между циклами, проверяем флаг остановки каждую секунду
                    check_interval = self.config.get('parser', {}).get('check_interval_seconds', 60)
                    logger.info(f"Пауза {check_interval} секунд до следующей проверки")
                    
                    for _ in range(check_interval):
                        if self.stop_event.is_set():
                            logger.info("Получен сигнал остановки во время паузы")
                            break
                        await asyncio.sleep(1)
                        
                except FloodWaitError as e:
                    # Если получили FloodWait ошибку, устанавливаем блокировку
                    wait_seconds = e.seconds
                    self.set_flood_wait(wait_seconds)
                    
                    # Уведомляем администраторов о длительной блокировке
                    if wait_seconds > 3600:  # Более часа
                        logger.critical(f"Получена FloodWait ошибка на {wait_seconds/3600:.1f} часов")
                        
                        # Отправляем уведомление администраторам
                        if self.bot:
                            for admin_id in CONFIG['settings']['admins']:
                                try:
                                    await self.bot.send_message(
                                        chat_id=admin_id, 
                                        text=f"⚠️ ВНИМАНИЕ!\n\n"
                                             f"API Telegram требует подождать {wait_seconds/3600:.1f} часов.\n\n"
                                             f"Парсер будет автоматически возобновлен после истечения времени ожидания."
                                    )
                                except Exception as e:
                                    logger.error(f"Не удалось отправить уведомление администратору {admin_id}: {e}")
                    
                    logger.info(f"Пауза из-за FloodWait: {wait_seconds} секунд")
                    
                    # Делаем паузу по 1 секунде, чтобы можно было прервать работу
                    for i in range(min(900, wait_seconds)):  # Максимум 15 минут или указанное время
                        if self.stop_event.is_set():
                            logger.info("Получен сигнал остановки во время ожидания после ошибки")
                            break
                        
                        # Каждые 60 секунд логируем информацию
                        if i % 60 == 0 and i > 0:
                            logger.info(f"FloodWait: прошло {i} сек из {wait_seconds} сек")
                        
                        await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка в цикле парсинга: {e}")
                    logger.exception(e)
                    
                    # Записываем ошибку в монитор
                    if self.monitor:
                        self.monitor.record_error(f"Ошибка в цикле парсинга: {e}")
                    
                    # Ждем немного перед следующей попыткой, но проверяем флаг остановки
                    error_wait = min(30, self.config.get('parser', {}).get('error_wait_seconds', 30))
                    logger.info(f"Ожидание {error_wait} секунд перед следующей попыткой...")
                    
                    for _ in range(error_wait):
                        if self.stop_event.is_set():
                            logger.info("Получен сигнал остановки во время ожидания после ошибки")
                            break
                        await asyncio.sleep(1)
                    
        except Exception as e:
            logger.critical(f"Критическая ошибка в парсере: {e}")
            logger.exception(e)
            
            # Записываем критическую ошибку в монитор
            if self.monitor:
                self.monitor.record_error(f"Критическая ошибка в парсере: {e}", is_critical=True)
        finally:
            self.is_running = False
            
            # Останавливаем монитор активности
            if 'activity_monitor' in locals():
                activity_monitor.stop_monitoring()
                
            logger.info("Парсер остановлен")

    # Метод для проверки и обработки активной FloodWait блокировки
    async def check_flood_wait(self):
        """Проверяет, активна ли блокировка по FloodWait и ждет, если необходимо"""
        if not self.flood_wait_active or not self.flood_wait_until:
            return False
            
        now = datetime.now(timezone.utc)
        if now < self.flood_wait_until:
            wait_seconds = (self.flood_wait_until - now).total_seconds()
            
            # Если ждать осталось мало, дожидаемся
            if wait_seconds < 300:  # Менее 5 минут
                logger.info(f"Ожидание снятия блокировки FloodWait: {wait_seconds:.1f} сек.")
                await asyncio.sleep(wait_seconds)
                self.flood_wait_active = False
                self.flood_wait_until = None
                return False
            else:
                # Если ждать долго, возвращаем True (блокировка все еще активна)
                remaining_hours = wait_seconds / 3600
                logger.warning(f"Активна блокировка FloodWait, осталось ждать {remaining_hours:.1f} часов")
                return True
                
        # Если время блокировки прошло
        self.flood_wait_active = False
        self.flood_wait_until = None
        return False
        
    # Метод для установки блокировки FloodWait
    def set_flood_wait(self, seconds):
        """Устанавливает блокировку FloodWait на указанное количество секунд"""
        self.flood_wait_active = True
        self.flood_wait_until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        
        # Логируем информацию о блокировке
        wait_minutes = seconds / 60
        if wait_minutes > 60:
            wait_hours = wait_minutes / 60
            logger.warning(f"Установлена блокировка FloodWait на {wait_hours:.1f} часов (до {self.flood_wait_until})")
        else:
            logger.warning(f"Установлена блокировка FloodWait на {wait_minutes:.1f} минут (до {self.flood_wait_until})")
            
    # Метод для выполнения API запроса с учетом ограничений скорости
    async def rate_limited_request(self, request_func, *args, **kwargs):
        """Выполняет API запрос с учетом ограничений скорости"""
        # Сначала проверяем, нет ли активной блокировки FloodWait
        if await self.check_flood_wait():
            raise FloodWaitError(f"Активна блокировка FloodWait до {self.flood_wait_until}")
            
        # Приобретаем разрешение у ограничителя скорости
        await self.rate_limiter.acquire()
        
        try:
            # Выполняем запрос
            result = await request_func(*args, **kwargs)
            return result
        except FloodWaitError as e:
            # Если получили FloodWait, устанавливаем блокировку
            wait_seconds = e.seconds
            self.set_flood_wait(wait_seconds)
            
            # Для коротких ожиданий просто ждем и повторяем
            if wait_seconds <= 60:  # Не более минуты
                logger.info(f"Короткое ожидание FloodWait: {wait_seconds} сек")
                await asyncio.sleep(wait_seconds)
                # Повторно пробуем запрос
                return await request_func(*args, **kwargs)
            
            # Для долгих ожиданий пробрасываем исключение
            raise 