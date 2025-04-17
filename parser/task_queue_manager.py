import asyncio
import logging
import os
import math

class TaskQueueManager:
    def __init__(self, num_workers=5, task_timeout=300, max_retries=3, retry_base_delay=2):
        self.queue = asyncio.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.logger = logging.getLogger("parser.task_queue")
        self.running = False
        self.task_timeout = int(os.environ.get("TASK_QUEUE_TASK_TIMEOUT", task_timeout))
        self.max_retries = int(os.environ.get("TASK_QUEUE_MAX_RETRIES", max_retries))
        self.retry_base_delay = float(os.environ.get("TASK_QUEUE_RETRY_BASE_DELAY", retry_base_delay))

    async def start(self):
        self.running = True
        self.logger.info(f"[QUEUE] Запуск {self.num_workers} воркеров очереди задач")
        for i in range(self.num_workers):
            worker = asyncio.create_task(self.worker_loop(i))
            self.workers.append(worker)

    async def stop(self):
        self.running = False
        self.logger.info("[QUEUE] Остановка воркеров очереди задач")
        # Положим None в очередь для graceful shutdown
        for _ in range(self.num_workers):
            await self.queue.put(None)
        await asyncio.gather(*self.workers)
        self.workers = []

    async def add_task(self, task):
        await self.queue.put(task)
        self.logger.debug(f"[QUEUE] Задача добавлена: {task.get('type')}")

    async def worker_loop(self, worker_id):
        self.logger.info(f"[QUEUE] Воркeр {worker_id} запущен")
        while self.running:
            task = await self.queue.get()
            if task is None:
                self.logger.info(f"[QUEUE] Воркeр {worker_id} завершает работу")
                break
            try:
                await self.handle_with_retry(task, worker_id)
            except Exception as e:
                self.logger.error(f"[QUEUE] Неожиданная ошибка в воркере {worker_id}: {e}")
            finally:
                self.queue.task_done()

    async def handle_with_retry(self, task, worker_id):
        task_type = task.get('type')
        handler = task.get('handler')
        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            try:
                self.logger.info(f"[QUEUE] Воркeр {worker_id} выполняет задачу: {task_type}, попытка {attempt}/{self.max_retries}")
                await asyncio.wait_for(handler(task), timeout=self.task_timeout)
                self.logger.info(f"[QUEUE] Воркeр {worker_id} успешно выполнил задачу: {task_type}, попытка {attempt}")
                return
            except asyncio.TimeoutError:
                self.logger.warning(f"[QUEUE] Таймаут задачи {task_type} в воркере {worker_id} (попытка {attempt})")
            except Exception as e:
                self.logger.error(f"[QUEUE] Ошибка задачи {task_type} в воркере {worker_id} (попытка {attempt}): {e}")
            if attempt < self.max_retries:
                delay = self.retry_base_delay * math.pow(2, attempt-1)
                self.logger.info(f"[QUEUE] Повтор задачи {task_type} через {delay:.1f} сек (воркер {worker_id})")
                await asyncio.sleep(delay)
        self.logger.critical(f"[QUEUE] Задача {task_type} окончательно провалена после {self.max_retries} попыток (воркер {worker_id}): {task}")
        # Если есть future, не оставлять его незавершённым
        future = task.get('result_future')
        if future and not future.done():
            future.set_result(None)

    async def handle_task(self, task, worker_id):
        # task: {'type': 'download_media'|'upload_minio'|'send_kafka', ...}
        task_type = task.get('type')
        self.logger.info(f"[QUEUE] Воркeр {worker_id} выполняет задачу: {task_type}")
        # Здесь будут вызовы соответствующих функций, которые будут переданы при инициализации
        handler = task.get('handler')
        if handler:
            await handler(task)
        else:
            self.logger.warning(f"[QUEUE] Нет handler для задачи: {task}") 