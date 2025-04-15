from aiokafka import AIOKafkaProducer
import asyncio
import json

class KafkaProducerAsync:
    def __init__(self, kafka_config):
        self.bootstrap_servers = kafka_config['bootstrap_servers']
        self.topic = kafka_config['topic']
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send(self, data):
        if self.producer:
            await self.producer.send_and_wait(self.topic, data) 