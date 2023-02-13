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
            logger.error(f'Producer with consumer_id {consumer_id} does not exist!')

        logger.info('TRY TO CONSUME DATA...')
        data = await consumer.getmany(timeout_ms=1000, max_records=10)  # TODO: Вынести в конфиг

        if not data:
            logger.info('EMPTY QUEUE...')
        else:
            logger.info('FOUND SOME MESSAGES!')
            logger.debug(f'DATA: {data}')

        for _, messages in data.items():
            for msg in messages:
                yield msg

    async def send_message(self, producer_id: Union[str, int], message: Union[Dict[str, Any], str], **send_config):
        producer = self.get_producer(producer_id)

        if not producer:
            logger.error(f'Producer with producer_id {producer_id} does not exist!')

        json_message = json.dumps(message, default=str).encode('utf-8')

        logger.info(f'SENDING DATA TO KAFKA TOPIC ({send_config.get("topic_name")})...')
        logger.debug(f'DATA: {json_message}')
        await producer.send_and_wait(
            topic=send_config.get('topic_name'),
            value=json_message
        )

    async def start(self):
        super(KafkaQueue, self).start()

        producers = self.get_producers()
        consumers = self.get_consumers()

        for producer in producers:
            await producer.start()

        for consumer in consumers:
            await consumer.start()

    async def stop(self):
        producers = self.get_producers()
        consumers = self.get_consumers()

        for producer in producers:
            await producer.stop()

        for consumer in consumers:
            await consumer.stop()

    async def commit(self, consumer_id):
        consumer = self.get_consumer(consumer_id)

        await consumer.commit()
