import os

import uvicorn
from fastapi import FastAPI

from .core.metabase import create_metabase
from .rest_api.crud import crud_router
from .settings import DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

create_metabase(
    metabase_id='default',
    metabase_type=DB_TYPE,
    host=DB_HOST,
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
)

app = FastAPI()
app.include_router(crud_router)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
