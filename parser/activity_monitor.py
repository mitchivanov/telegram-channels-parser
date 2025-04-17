import asyncio
import time
import logging
from datetime import datetime, timezone

class ActivityMonitor:
    def __init__(self, timeout=180):
        self.timeout = timeout
        self.last_activity = None
        self.task = None
        self.is_monitoring = False
        self.logger = logging.getLogger("parser.activity_monitor")

    async def start_monitoring(self):
        self.logger.info(f"[MONITOR] Старт мониторинга активности, таймаут: {self.timeout}s")
        self.last_activity = time.time()
        self.is_monitoring = True
        self.task = asyncio.create_task(self._monitoring_loop())

    async def update_activity(self):
        self.last_activity = time.time()
        self.logger.info(f"[MONITOR] Обновление активности: {datetime.now(timezone.utc)}")

    async def stop_monitoring(self):
        self.logger.info("[MONITOR] Остановка мониторинга активности")
        self.is_monitoring = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                self.logger.info("[MONITOR] Задача мониторинга отменена")
            except Exception as e:
                self.logger.warning(f"[MONITOR] Ошибка при остановке мониторинга: {e}")

    async def _monitoring_loop(self):
        try:
            self.logger.info("[MONITOR] Запуск фонового мониторинга активности")
            while self.is_monitoring:
                elapsed = time.time() - self.last_activity
                if elapsed > self.timeout:
                    self.logger.critical(f"Parser hang detected: {elapsed:.1f}s without activity!")
                    self.last_activity = time.time()
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.logger.info("[MONITOR] Мониторинг активности отменен")
        finally:
            self.is_monitoring = False 