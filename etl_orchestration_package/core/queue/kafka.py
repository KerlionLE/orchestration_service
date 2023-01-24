import asyncio
import json
from typing import Dict, Any, Union

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from loguru import logger

from .queue import Queue


class KafkaQueue(Queue):
    def create_producer(self, **producer_config):
        loop = asyncio.get_event_loop()
        return AIOKafkaProducer(loop=loop, **producer_config)

    def create_consumer(self, **consumer_config):
        loop = asyncio.get_event_loop()
        topics = consumer_config.pop('topics', list())
        return AIOKafkaConsumer(*topics, loop=loop, **consumer_config)

    async def consume_data(self, consumer_id: Union[str, int]):
        consumer = self.get_consumer(consumer_id)

        if not consumer:
            # logger.error(f'Producer with producer_id {producer_id} does not exist!')
            raise ValueError(f'Consumer with consumer_id {consumer_id} does not exist!')

        async with consumer as c:
            data = await c.getmany(timeout_ms=1000, max_records=10)  # TODO: Вынести в конфиг
            for _, messages in data.items():
                for msg in messages:
                    yield msg

    async def send_message(self, producer_id: Union[str, int], message: Union[Dict[str, Any], str]):
        producer = self.get_producer(producer_id)

        if not producer:
            # logger.error(f'Producer with producer_id {producer_id} does not exist!')
            raise ValueError(f'Producer with producer_id {producer_id} does not exist!')

        json_message = json.dumps(message, default=str).encode('utf-8')

        await producer.send(value=json_message)

