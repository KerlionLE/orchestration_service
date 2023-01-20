import asyncio

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from .queue import Queue


class KafkaQueue(Queue):
    def create_producer(self, **producer_config):
        loop = asyncio.get_event_loop()
        return AIOKafkaProducer(loop=loop, **producer_config)

    def create_consumer(self, **consumer_config):
        loop = asyncio.get_event_loop()
        topics = consumer_config.pop('topics', list())
        return AIOKafkaConsumer(*topics, loop=loop, **consumer_config)

    async def consume_data(self, consumer_id):
        async for msg in self.get_consumer(consumer_id):
            yield msg
