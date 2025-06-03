import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from db import SessionLocal
from models import Filter
import logging
import redis.asyncio as aioredis
from utils import extract_cashback_percent, post_text_hash

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s() - %(message)s',
)
logger = logging.getLogger("kafka_worker")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_posts")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MODERATION_QUEUE = "moderation_queue"
APPROVED_QUEUE = "approved_queue"
PAUSE_KEY_PREFIX = "filtering:paused:"

# Словарь соответствия ID каналов их названиям
CHANNEL_NAMES = {
    '-1002503014558': 'Премиум канал',
    '-1002687437494': 'Базовый канал',
    '-1001655410418': 'Бесплатный канал',
}

def build_channel_topics():
    topics = {}
    for k, v in os.environ.items():
        if k.startswith('CHANNEL') and k.endswith('_ID') and v.strip():
            num = k[len('CHANNEL'):-len('_ID')]
            topic_env = f'KAFKA_CHANNEL{num}_TOPIC'
            topic = os.environ.get(topic_env, f'channel{num}_posts')
            topics[v] = topic
    return topics

CHANNEL_TOPICS = build_channel_topics()

async def get_filters(db: AsyncSession):
    result = await db.execute(select(Filter).options(selectinload(Filter.channel_obj)))
    filters = result.scalars().all()
    return {f.channel_obj.name: f for f in filters if f.channel_obj}

async def add_to_moderation_queue_redis(redis, post, target_channel):
    channel_name = CHANNEL_NAMES.get(target_channel, target_channel)
    logger.info(f"[MODERATION_DETAIL] Получен пост для отправки в очередь модерации, канал {target_channel} ({channel_name})")
    logger.info(f"[MODERATION_DETAIL] Исходные данные поста: {json.dumps(post, ensure_ascii=False)}")
    post_copy = dict(post)
    
    # Используем название канала вместо ID, если оно есть в словаре
    post_copy["target_channel"] = target_channel
    post_copy["target_channel_name"] = channel_name
    
    # Фильтруем медиа как в send_to_channel_topic
    media = post_copy.get("media", [])
    logger.info(f"[MODERATION_DETAIL] Исходные медиа файлы ({len(media)}): {json.dumps(media, ensure_ascii=False)}")
    media_out = []
    for i, m in enumerate(media):
        if m.get("file_id"):
            logger.info(f"[MODERATION_DETAIL] [{i}] Добавляю медиа с file_id: {m.get('file_id')}")
            media_out.append(m)
        elif m.get("local_path") and os.path.exists(m["local_path"]):
            logger.info(f"[MODERATION_DETAIL] [{i}] Добавляю медиа с local_path: {m['local_path']}")
            # Создаем или увеличиваем счетчик модерации для файла
            local_path = m["local_path"]
            try:
                # Используем отдельный счетчик для модерации
                await redis.incr(f"moderation:{local_path}")
                logger.info(f"[MODERATION_COUNTER] Увеличен счетчик модерации для {local_path}")
                # Устанавливаем TTL только если ключ новый (не имеет TTL)
                ttl = await redis.ttl(f"moderation:{local_path}")
                if ttl == -1:  # Ключ существует, но без TTL
                    await redis.expire(f"moderation:{local_path}", 86400)  # 24 часа
                    logger.info(f"[MODERATION_COUNTER] Установлен TTL 24 часа для счетчика модерации {local_path}")
            except Exception as e:
                logger.error(f"[MODERATION_COUNTER] Ошибка при инкременте счетчика модерации для {local_path}: {e}")
            media_out.append(m)
        elif m.get("url"):
            logger.info(f"[MODERATION_DETAIL] [{i}] Добавляю медиа с url: {m.get('url')}")
            media_out.append(m)
        else:
            logger.error(f"[MODERATION_DETAIL] [{i}] Нет подходящего источника для медиа: {json.dumps(m, ensure_ascii=False)}")
    post_copy["media"] = media_out
    logger.info(f"[MODERATION_DETAIL] Подготовленные медиа файлы ({len(media_out)}): {json.dumps(media_out, ensure_ascii=False)}")
    post_json = json.dumps(post_copy, ensure_ascii=False)
    logger.info(f"[MODERATION_DETAIL] Подготовленные данные для очереди: {post_json}")
    await redis.rpush(MODERATION_QUEUE, post_json)
    logger.info(f"[MODERATION] Пост добавлен в Redis moderation_queue для канала {channel_name} ({target_channel})")

async def send_to_channel_topic(producer, topic, post_filtered):
    logger.info(f"[KAFKA] Отправляю пост в топик {topic}")
    media = post_filtered.get("media", [])
    media_out = []
    for m in media:
        # Приоритет 1: file_id (новый формат)
        if m.get("file_id"):
            media_out.append({
                "type": m.get("type", "photo"),
                "file_id": m["file_id"],
                "mime_type": m.get("mime_type")
            })
        # Приоритет 2: local_path (старый формат)
        elif m.get("local_path"):
            local_path = m["local_path"]
            if not os.path.exists(local_path):
                logger.error(f"[FILTER] Файл не найден: {local_path}")
                continue
            media_out.append({"type": m.get("type", "photo"), "local_path": local_path})
        # Приоритет 3: url (старый формат)
        else:
            url = m.get("url")
            media_out.append({"type": m.get("type", "photo"), "url": url})
    post_out = {
        "text": post_filtered.get("text", ""),
        "media": media_out
    }
    try:
        await producer.send_and_wait(topic, post_out)
        logger.info(f"[KAFKA] Пост успешно отправлен в топик {topic}")
        return True
    except Exception as e:
        logger.error(f"[KAFKA] Ошибка при отправке в топик {topic}: {e}")
        return False

async def approved_queue_worker(redis, producer):
    logger.info("[APPROVED_QUEUE] Запуск воркера очереди одобренных постов...")
    while True:
        post_json = await redis.lpop(APPROVED_QUEUE)
        if post_json:
            try:
                post = json.loads(post_json)
                # Получаем ID канала из поля target_channel
                channel = post.get("target_channel") or post.get("channel")
                topic = CHANNEL_TOPICS.get(channel)
                if not topic:
                    logger.error(f"[APPROVED_QUEUE] Не найден топик для канала {channel}")
                    continue
                
                # Уменьшаем счетчики модерации для всех медиа-файлов в посте
                for media_item in post.get("media", []):
                    if media_item.get("local_path") and os.path.exists(media_item["local_path"]):
                        local_path = media_item["local_path"]
                        try:
                            # Уменьшаем счетчик модерации
                            mod_count = await redis.decr(f"moderation:{local_path}")
                            logger.info(f"[MODERATION_COUNTER] Уменьшен счетчик модерации для {local_path}: {mod_count}")
                            
                            # Если счетчик модерации достиг нуля, удаляем его
                            if mod_count <= 0:
                                await redis.delete(f"moderation:{local_path}")
                                logger.info(f"[MODERATION_COUNTER] Счетчик модерации для {local_path} удален (достиг 0)")
                                
                                # Проверяем, есть ли еще счетчик file:*
                                file_count = await redis.get(f"file:{local_path}")
                                
                                # Если нет счетчика file:* (или он 0), помечаем файл на удаление
                                if not file_count or int(file_count) <= 0:
                                    # Помечаем на удаление через 5 минут
                                    await redis.set(f'delete_after:{local_path}', 1, ex=300)
                                    logger.info(f"[MODERATION_COUNTER] Файл {local_path} помечен на удаление через 5 минут (оба счетчика 0)")
                        except Exception as e:
                            logger.error(f"[MODERATION_COUNTER] Ошибка при декременте счетчика модерации для {local_path}: {e}")
                
                logger.info(f"[APPROVED_QUEUE] Отправляю одобренный пост в топик {topic}")
                await send_to_channel_topic(producer, topic, post)
            except Exception as e:
                logger.error(f"[APPROVED_QUEUE] Ошибка обработки одобренного поста: {e}")
        else:
            await asyncio.sleep(1)

async def kafka_filter_worker():
    logger.info("[KAFKA] Запуск consumer и producer...")
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )
    await consumer.start()
    logger.info("[KAFKA] Consumer запущен")
    await producer.start()
    logger.info("[KAFKA] Producer запущен")
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    # Запускаем воркер очереди одобренных постов
    asyncio.create_task(approved_queue_worker(redis, producer))
    try:
        while True:
            async for msg in consumer:
                logger.info(f"[KAFKA] Получено сообщение из топика {msg.topic}: {str(msg.value)[:100]}")
                post = msg.value
                # Проверка на 100% дублирование текста поста
                text = ""
                if post.get("media") and len(post["media"]) > 0:
                    text = post["media"][0].get("caption", "")
                if not text:
                    text = post.get("text", "")
                hash_val = post_text_hash(text)
                hash_key = f"post_hash:{hash_val}"
                is_duplicate = await redis.get(hash_key)
                if is_duplicate:
                    logger.info(f"[DUPLICATE] Найден дубликат поста по хешу {hash_val}, пропускаю обработку.")
                    continue
                await redis.set(hash_key, 1, ex=7*24*60*60)  # TTL 7 дней
                # Сохраняем исходный канал для истории, если есть
                if "channel" in post:
                    post["source_channel"] = post["channel"]
                    del post["channel"]
                media_files = []
                # Собираем local_path из медиа для последующей очистки
                for m in post.get("media", []):
                    if m.get("local_path") and os.path.exists(m["local_path"]):
                        media_files.append(m["local_path"])
                sent_to_any_channel = False
                sent_to_moderation = False  # Новый флаг для отслеживания отправки на модерацию
                send_tasks = []
                send_channels = []
                media_files_to_count = set()
                # Собираем local_path для подсчета
                for m in post.get("media", []):
                    if m.get("local_path") and os.path.exists(m["local_path"]):
                        media_files_to_count.add(m["local_path"])
                num_channels = 0
                async with SessionLocal() as db:
                    filters = await get_filters(db)
                    logger.info(f"[FILTER] Загружено {len(filters)} фильтров. Начинаю обработку поста.")
                    text = post.get("text", "").lower()
                    available_channels = []
                    for env_key, env_value in os.environ.items():
                        if env_key.startswith('CHANNEL') and env_key.endswith('_ID') and env_value.strip():
                            available_channels.append(env_value)
                    logger.info(f"[DEBUG] available_channels: {available_channels}")
                    for target_channel in available_channels:
                        pause_key = f"{PAUSE_KEY_PREFIX}{target_channel}"
                        paused = await redis.get(pause_key)
                        # Универсальная проверка paused
                        paused_str = paused.decode() if isinstance(paused, bytes) else str(paused) if paused is not None else None
                        logger.info(f"[DEBUG] Проверяю паузу: ключ={pause_key}, значение={paused}, тип={type(paused)}, канал={target_channel}")
                        if paused_str is None or paused_str == "1":
                            logger.info(f"[PAUSE] Канал {target_channel} на паузе, пропускаю обработку.")
                            continue
                        post_filtered = dict(post)
                        skip = False
                        if target_channel in filters:
                            logger.info(f"[FILTER] Найден фильтр для канала {target_channel}, применяю фильтрацию")
                            rule = filters[target_channel]
                            # --- Кэшбек фильтрация ---
                            min_cb = getattr(rule, 'min_cashback_percent', None)
                            max_cb = getattr(rule, 'max_cashback_percent', None)
                            use_cashback = min_cb is not None or max_cb is not None
                            cashback_ok = True
                            if use_cashback:
                                percents = extract_cashback_percent(text)
                                logger.info(f"[CASHBACK] Извлечённые проценты: {percents} для канала {target_channel}")
                                if not percents:
                                    if rule.moderation_required:
                                        channel_name = CHANNEL_NAMES.get(target_channel, target_channel)
                                        logger.info(f"[MODERATION][CASHBACK] Не удалось извлечь процент кэшбэка, отправляю на модерацию для {target_channel} ({channel_name})")
                                        await add_to_moderation_queue_redis(redis, post_filtered, target_channel)
                                        sent_to_moderation = True
                                        skip = True
                                        continue
                                    else:
                                        logger.info(f"[CASHBACK] Не удалось извлечь процент кэшбэка, пост пропущен для {target_channel}")
                                        skip = True
                                else:
                                    min_v = min_cb if min_cb is not None else 0
                                    max_v = max_cb if max_cb is not None else 100
                                    cashback_ok = any(min_v <= p <= max_v for p in percents)
                                    if not cashback_ok:
                                        logger.info(f"[CASHBACK] Пост не прошёл по диапазону кэшбэка {min_v}-{max_v}% для {target_channel}")
                                        skip = True
                            # --- Ключевые слова ---
                            if not skip and rule.keywords and not any(kw.lower() in text for kw in rule.keywords):
                                logger.info(f"[FILTER] Пост не прошёл по ключевым словам для {target_channel}")
                                skip = True
                            if not skip and rule.stopwords and any(sw.lower() in text for sw in rule.stopwords):
                                logger.info(f"[FILTER] Пост не прошёл по стоп-словам для {target_channel}")
                                skip = True
                            if not skip and rule.remove_channel_links:
                                logger.info(f"[FILTER] Флаг remove_channel_links игнорируется для {target_channel} (функция отключена)")
                            if not skip and rule.moderation_required and not use_cashback:
                                channel_name = CHANNEL_NAMES.get(target_channel, target_channel)
                                logger.info(f"[MODERATION] Требуется модерация для {target_channel} ({channel_name}), отправляю в Redis moderation_queue")
                                await add_to_moderation_queue_redis(redis, post_filtered, target_channel)
                                sent_to_moderation = True  # Устанавливаем флаг отправки на модерацию
                                skip = True
                                continue
                        else:
                            logger.warning(f"[FILTER] Фильтр для канала {target_channel} не найден — сообщение отброшено!")
                            continue
                        topic = CHANNEL_TOPICS.get(target_channel)
                        if not skip and topic:
                            logger.info(f"[KAFKA] Отправляю пост в топик {topic} для канала {target_channel}")
                            send_tasks.append(send_to_channel_topic(producer, topic, post_filtered))
                            send_channels.append(target_channel)
                        elif not skip:
                            logger.warning(f"[KAFKA] Не найден топик для канала {target_channel}")
                    num_channels = len(send_channels)
                    # Обновляем счетчики для local_path
                    if num_channels > 0 and media_files_to_count:
                        for f in media_files_to_count:
                            await redis.incrby(f"file:{f}", num_channels)
                    results = []
                    if send_tasks:
                        results = await asyncio.gather(*send_tasks, return_exceptions=True)
                        for ch, res in zip(send_channels, results):
                            logger.info(f"[KAFKA] Результат отправки в канал {ch}: {res}")
                        sent_to_any_channel = any(res is True for res in results)
                # Удаляем временные файлы, если они не были отправлены ни в каналы, ни на модерацию
                if not sent_to_any_channel and not sent_to_moderation and media_files:
                    for f in media_files:
                        try:
                            os.remove(f)
                            logger.info(f"[CLEANUP] Временный файл удалён: {f}")
                        except Exception as e:
                            logger.warning(f"[CLEANUP] Не удалось удалить файл {f}: {e}")
                # Дополнительная проверка: если файлы не были добавлены в счетчики, но все еще существуют,
                # добавим их в delete_after с небольшим TTL для последующей очистки cleanup_worker'ом
                elif media_files and not sent_to_moderation:  # Важно! Не удаляем файлы, отправленные на модерацию
                    for f in media_files:
                        if f not in media_files_to_count and os.path.exists(f):
                            try:
                                # Проверяем, есть ли уже счетчик для этого файла
                                file_counter = await redis.get(f"file:{f}")
                                if not file_counter:
                                    # Если счетчика нет, помечаем файл на удаление через 5 минут
                                    await redis.set(f'delete_after:{f}', 1, ex=300)
                                    logger.info(f"[CLEANUP] Файл {f} не прошел фильтрацию, помечен на удаление через 5 минут")
                            except Exception as e:
                                logger.warning(f"[CLEANUP] Ошибка при обработке файла без счетчика {f}: {e}")
                elif sent_to_moderation and media_files:
                    # Для файлов на модерации не используем delete_after, а вместо этого полагаемся на 
                    # счетчики модерации, установленные в add_to_moderation_queue_redis
                    logger.info(f"[CLEANUP] {len(media_files)} файлов отправлены на модерацию и будут управляться счетчиками moderation:*")
    finally:
        logger.info("[KAFKA] Остановка consumer и producer...")
        await consumer.stop()
        await producer.stop()
        logger.info("[KAFKA] Consumer и producer остановлены")

if __name__ == "__main__":
    asyncio.run(kafka_filter_worker()) 