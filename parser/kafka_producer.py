from aiokafka import AIOKafkaProducer
import asyncio
import json
import logging
import time
import traceback

class KafkaProducerAsync:
    def __init__(self, config):
        self.bootstrap_servers = config['bootstrap_servers']
        self.topic = config['topic']
        self.producer = None
        self.logger = logging.getLogger("parser.kafka")
        self.connected = False
        self.sent_count = 0
        self.error_count = 0
        self.last_stats_time = 0

    async def start(self):
        self.logger.info(f"[KAFKA] Инициализация KafkaProducer: {self.bootstrap_servers}, topic: {self.topic}")
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                retry_backoff_ms=500,
                request_timeout_ms=10000  # Увеличиваем таймаут запроса до 10 сек
            )
            await self.producer.start()
            self.connected = True
            self.logger.info(f"[KAFKA] KafkaProducer успешно запущен")
        except Exception as e:
            self.connected = False
            self.logger.error(f"[KAFKA] Ошибка инициализации KafkaProducer: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def send(self, data):
        if not self.connected or not self.producer:
            self.logger.error("[KAFKA] Невозможно отправить: KafkaProducer не инициализирован")
            self.error_count += 1
            raise RuntimeError("KafkaProducer не инициализирован")
        
        start_time = time.time()
        self.logger.info(f"[KAFKA] Отправка сообщения: {str(data)[:500]}")
        
        try:
            await self.producer.send_and_wait(self.topic, data)
            elapsed = time.time() - start_time
            self.sent_count += 1
            
            # Периодически логируем статистику
            if time.time() - self.last_stats_time > 60:
                self.logger.info(f"[KAFKA] Статистика: отправлено {self.sent_count}, ошибок {self.error_count}")
                self.last_stats_time = time.time()
                
            self.logger.info(f"[KAFKA] Сообщение успешно отправлено (заняло {elapsed:.3f} сек)")
            return True
        except Exception as e:
            elapsed = time.time() - start_time
            self.error_count += 1
            self.logger.error(f"[KAFKA] Ошибка отправки сообщения (после {elapsed:.3f} сек): {e}")
            self.logger.debug(traceback.format_exc())
            raise

    async def stop(self):
        if self.producer:
            self.logger.info("[KAFKA] Остановка KafkaProducer")
            await self.producer.stop()
            self.producer = None
            self.connected = False
            self.logger.info(f"[KAFKA] KafkaProducer остановлен. Итого: отправлено {self.sent_count}, ошибок {self.error_count}")
        else:
            self.logger.warning("[KAFKA] Попытка остановить неинициализированный KafkaProducer") 