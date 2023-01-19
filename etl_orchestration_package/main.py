import os

import uvicorn
from fastapi import FastAPI

from core.metabase import create_metabase
from rest_api.crud import crud_router

create_metabase(
    metabase_id='default',
    metabase_type=os.getenv('DB_TYPE', 'pg'),
    host=os.getenv('DB_HOST', 's001cd-db-dev01.dev002.local'),
    port=os.getenv('DB_PORT', 5432),
    username=os.getenv('DB_USER', 'a001_orchestration_tech_user'),
    password=os.getenv('DB_PASSWORD', 'usrofa001_orchestration_tech_user'),
    database=os.getenv('DB_NAME', 'orchestration'),
)

app = FastAPI()
app.include_router(crud_router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
