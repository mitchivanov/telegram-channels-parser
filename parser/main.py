import asyncio
import json
import os
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from aiokafka import AIOKafkaProducer

# Загрузка конфигов
with open(os.environ.get('TELEGRAM_CONFIG', '/app/config/telegram_config.json'), encoding='utf-8') as f:
    tg_config = json.load(f)
with open(os.environ.get('KAFKA_CONFIG', '/app/config/kafka_config.json'), encoding='utf-8') as f:
    kafka_config = json.load(f)

API_ID = tg_config['api_id']
API_HASH = tg_config['api_hash']
PHONE = tg_config['phone']
SESSION = tg_config['session']
CHANNELS = tg_config['channels']
KAFKA_BOOTSTRAP = kafka_config['bootstrap_servers']
KAFKA_TOPIC = kafka_config['topic']

LAST_IDS_FILE = '/app/last_ids.json'

async def save_last_ids(last_ids):
    with open(LAST_IDS_FILE, 'w', encoding='utf-8') as f:
        json.dump(last_ids, f)

def load_last_ids():
    if os.path.exists(LAST_IDS_FILE):
        with open(LAST_IDS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

async def main():
    last_ids = load_last_ids()
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(phone=PHONE)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
    await producer.start()
    try:
        while True:
            for channel in CHANNELS:
                try:
                    entity = await client.get_entity(channel)
                    history = await client(GetHistoryRequest(peer=entity, limit=10, offset_id=0, offset_date=None, add_offset=0, max_id=0, min_id=0, hash=0))
                    messages = history.messages
                    messages = sorted(messages, key=lambda m: m.id)
                    last_id = int(last_ids.get(channel, 0))
                    new_msgs = [m for m in messages if m.id > last_id]
                    for msg in new_msgs:
                        data = {
                            'channel': channel,
                            'id': msg.id,
                            'date': msg.date.isoformat(),
                            'text': msg.message or '',
                        }
                        await producer.send_and_wait(KAFKA_TOPIC, data)
                        print(f"[Kafka] {channel} #{msg.id} отправлено")
                        last_ids[channel] = msg.id
                    await save_last_ids(last_ids)
                except Exception as e:
                    print(f"Ошибка при обработке {channel}: {e}")
            await asyncio.sleep(15)
    finally:
        await producer.stop()
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main()) 