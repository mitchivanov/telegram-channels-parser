import asyncio
import json
import os
import logging
from parser.core import ParserCore

logging.basicConfig(level=logging.INFO)

TELEGRAM_CONFIG = os.environ.get('TELEGRAM_CONFIG', '/app/config/telegram_config.json')
KAFKA_CONFIG = os.environ.get('KAFKA_CONFIG', '/app/config/kafka_config.json')

with open(TELEGRAM_CONFIG, encoding='utf-8') as f:
    tg_config = json.load(f)
with open(KAFKA_CONFIG, encoding='utf-8') as f:
    kafka_config = json.load(f)

async def main():
    parser = ParserCore(tg_config, kafka_config)
    try:
        await parser.start()
    except KeyboardInterrupt:
        print("Остановка по Ctrl+C")
        await parser.stop()

if __name__ == '__main__':
    asyncio.run(main()) 