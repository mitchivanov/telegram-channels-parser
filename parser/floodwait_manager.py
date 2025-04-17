import asyncio
from datetime import datetime, timedelta
import logging

class FloodWaitManager:
    def __init__(self):
        self.floodwaits = {}
        self.logger = logging.getLogger("parser.floodwait_manager")
        self.floodwait_until = {}  # channel_id -> datetime
        self.lock = asyncio.Lock()

    async def set_floodwait(self, channel_id, seconds):
        async with self.lock:
            until = datetime.utcnow() + timedelta(seconds=seconds)
            self.floodwaits[channel_id] = until
            self.logger.warning(f"[FLOODWAIT] Установлен floodwait для {channel_id} на {seconds} секунд (до {until})")
            self.floodwait_until[channel_id] = until

    async def is_floodwait(self, channel_id):
        async with self.lock:
            until = self.floodwait_until.get(channel_id)
            if until and until > datetime.utcnow():
                self.logger.info(f"[FLOODWAIT] Канал {channel_id} в floodwait до {until}")
                return (True, (until - datetime.utcnow()).total_seconds())
            else:
                self.logger.info(f"[FLOODWAIT] Канал {channel_id} не в floodwait")
                return (False, 0) 