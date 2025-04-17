from aiokafka import AIOKafkaProducer
import asyncio
import json
import logging

class KafkaProducerAsync:
    def __init__(self, kafka_config):
        self.bootstrap_servers = kafka_config['bootstrap_servers']
        self.topic = kafka_config['topic']
        self.producer = None
        self.logger = logging.getLogger("parser.kafka_producer")

    async def start(self):
        self.logger.info("[KAFKA] Запуск KafkaProducerAsync")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        await self.producer.start()

    async def stop(self):
        self.logger.info("[KAFKA] Остановка KafkaProducerAsync")
        if self.producer:
            await self.producer.stop()

    async def send(self, data):
        self.logger.info(f"[KAFKA] Отправка сообщения: {data}")
        try:
            if self.producer:
                await self.producer.send_and_wait(self.topic, data)
                self.logger.info("[KAFKA] Сообщение успешно отправлено")
        except Exception as e:
            self.logger.error(f"[KAFKA] Ошибка при отправке сообщения: {e}")
            raise 