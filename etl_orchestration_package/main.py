from multiprocessing import Process

import uvicorn
from fastapi import FastAPI

from core.metabase import create_metabase
from core.queue import create_queue
from rest_api.crud import crud_router

from core.orchestration.functions import orchestration_process

from settings import DB_TYPE, DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

metabase_interface = create_metabase(
    metabase_id='default',
    metabase_type=DB_TYPE,
    host=DB_HOST,
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

queue_interface = create_queue(
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
            'group_id': 'orchestration_test_group_1',
            'enable_auto_commit': False
        }
    ],
    producers_configs=[
        {
            'producer_id': 'default',
            'bootstrap_servers': [
                's001cd-mq-kfk01.dev002.local:9092',
                's001cd-mq-kfk02.dev002.local:9092',
                's001cd-mq-kfk03.dev002.local:9092'
            ]
        }
    ]
)

app = FastAPI()
app.include_router(crud_router)

PROCESS_DICT = {}


@app.on_event("startup")
def startup_event():
    p = Process(
        target=orchestration_process,
        kwargs={
            'metabase_interface': metabase_interface,
            'queue_interface': queue_interface
        }
    )
    p.start()

    PROCESS_DICT[p.pid] = p


@app.on_event("shutdown")
def shutdown_event():
    for _, process in PROCESS_DICT.items():
        process.kill()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
    orchestration_process()
