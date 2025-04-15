import asyncio
from datetime import datetime, timedelta

class FloodWaitManager:
    def __init__(self):
        self.floodwait_until = {}  # channel_id -> datetime
        self.lock = asyncio.Lock()

    async def set_floodwait(self, channel_id, seconds):
        async with self.lock:
            self.floodwait_until[channel_id] = datetime.utcnow() + timedelta(seconds=seconds)

    async def is_floodwait(self, channel_id):
        async with self.lock:
            until = self.floodwait_until.get(channel_id)
            if until and until > datetime.utcnow():
                return (True, (until - datetime.utcnow()).total_seconds())
            return (False, 0) 