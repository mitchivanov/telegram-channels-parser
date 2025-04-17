import asyncio
import time
import logging

class RateLimiter:
    def __init__(self, requests_per_minute=40):
        self.requests_per_minute = requests_per_minute
        self.interval = 60 / requests_per_minute
        self.last_request_time = {}
        self.lock = asyncio.Lock()
        self.timestamps = {}
        self.logger = logging.getLogger("parser.rate_limiter")

    async def acquire(self, key='global'):
        async with self.lock:
            now = time.time()
            last = self.last_request_time.get(key, 0)
            elapsed = now - last
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self.last_request_time[key] = time.time()
            self.logger.info(f"[RATE_LIMIT] Запрос разрешён для {key}, всего за минуту: {len(self.timestamps.get(key, []))}")

    def reset(self, key):
        self.logger.info(f"[RATE_LIMIT] Сброс лимита для {key}")
        self.timestamps.pop(key, None) 