import asyncio
import time

class RateLimiter:
    def __init__(self, requests_per_minute=20):
        self.requests_per_minute = requests_per_minute
        self.interval = 60 / requests_per_minute
        self.last_request_time = {}
        self.lock = asyncio.Lock()

    async def acquire(self, key='global'):
        async with self.lock:
            now = time.time()
            last = self.last_request_time.get(key, 0)
            elapsed = now - last
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self.last_request_time[key] = time.time() 