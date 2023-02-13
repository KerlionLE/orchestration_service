from multiprocessing import Process

from fastapi import FastAPI

from . import settings as config

from .core.queue import create_queue
from .rest_api.crud import crud_router
from .core.metabase import create_metabase
from .core.orchestration import orchestration_process

app = FastAPI()
app.include_router(crud_router)

PROCESS_DICT = {}


@app.on_event("startup")
def startup_event():
    create_metabase(
        metabase_id='default',
        metabase_type=config.DB_TYPE,
        host=config.DB_HOST,
        port=config.DB_PORT,
        username=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME
    )

    create_queue(
        queue_id='default',
        queue_type=config.QUEUE_TYPE,
        consumers_configs=[
            {
                'consumer_id': 'default',
                'topics': [config.FINISHED_TASKS_TOPIC],
                'bootstrap_servers': config.QUEUE_SERVERS.split(','),
                'group_id': config.QUEUE_GROUP,
                'enable_auto_commit': False,
                'auto_offset_reset': config.AUTO_OFFSET_RESET
            }
        ],
        producers_configs=[
            {
                'producer_id': 'default',
                'bootstrap_servers': config.QUEUE_SERVERS.split(',')
            }
        ]
    )

    p = Process(
        target=orchestration_process,
        kwargs={'dry_run': config.DRY_RUN}
    )
    p.start()

    PROCESS_DICT[p.pid] = p


@app.on_event("shutdown")
def shutdown_event():
    for _, process in PROCESS_DICT.items():
        process.join()

# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8000)
