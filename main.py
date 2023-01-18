import asyncio
import logging

import uvicorn
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

from endpoints.crud import crud_router
from settings import KAFKA_HOST

app = FastAPI()

app.include_router(crud_router)

logger = logging.getLogger(__name__)
# loop = asyncio.get_event_loop()
# aioproducer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_HOST)


# @app.on_event("startup")
# async def startup_event():
#     logger.info("Starting up...")
#     await aioproducer.start()
#
#
# @app.on_event("shutdown")
# async def shutdown_event():
#     await aioproducer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

