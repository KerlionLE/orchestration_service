import asyncio

import uvicorn
from fastapi import FastAPI

from core.metabase import create_metabase
from core.queue import create_queue, get_queue
from rest_api.crud import crud_router

from core.orchestration.functions import orchestration_process

from settings import DB_TYPE, DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

create_metabase(
    metabase_id='default',
    metabase_type=DB_TYPE,
    host=DB_HOST,
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

create_queue(
    queue_id='default',
    queue_type='kafka',  # TODO
    consumers_configs=[
        {
            'consumer_id': 'finished_tasks',
            'topics': ['etl_finished_tasks'],
            'bootstrap_servers': [
                's001cd-mq-kfk01.dev002.local:9092',
                's001cd-mq-kfk02.dev002.local:9092',
                's001cd-mq-kfk03.dev002.local:9092'
            ],
            'group_id': 'orchestration_test_group_2',
            'enable_auto_commit': False
        }
    ],
    producers_configs=[]
)

# app = FastAPI()
# app.include_router(crud_router)


# @app.on_event("startup")
# async def startup_event():
#     queue = get_queue()
#
#     producers = queue.get_producers()
#     consumers = queue.get_consumers()
#
#     for producer in producers:
#         await producer.start()
#
#     for consumer in consumers:
#         await consumer.start()
#
#
# @app.on_event("shutdown")
# async def shutdown_event():
#     queue = get_queue()
#
#     producers = queue.get_producers()
#     consumers = queue.get_consumers()
#
#     for producer in producers:
#         await producer.stop()
#
#     for consumer in consumers:
#         await consumer.stop()


if __name__ == "__main__":
    # uvicorn.run(app, host="127.0.0.1", port=8000)
    orchestration_process()
    print('qq')
