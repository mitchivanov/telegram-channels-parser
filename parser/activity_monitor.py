import asyncio
import time
import logging

class ActivityMonitor:
    def __init__(self, timeout=180):
        self.timeout = timeout
        self.last_activity = time.time()
        self.is_monitoring = False
        self.monitor_task = None
        self.logger = logging.getLogger("parser.activity_monitor")

    async def start_monitoring(self):
        if self.is_monitoring:
            return
        self.is_monitoring = True
        self.last_activity = time.time()
        self.monitor_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info(f"Activity monitor started with timeout {self.timeout}s")

    async def update_activity(self):
        self.last_activity = time.time()

    async def _monitoring_loop(self):
        try:
            while self.is_monitoring:
                elapsed = time.time() - self.last_activity
                if elapsed > self.timeout:
                    self.logger.critical(f"Parser hang detected: {elapsed:.1f}s without activity!")
                    self.last_activity = time.time()
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.logger.info("Activity monitor cancelled")
        finally:
            self.is_monitoring = False

    async def stop_monitoring(self):
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            self.logger.info("Activity monitor stopped") 