from typing import Dict, Any, Union, List, Optional

from loguru import logger
from pydantic import BaseModel

from sqlalchemy.orm import DeclarativeMeta

try:
    from ..core.metabase import utils as db_tools
except ModuleNotFoundError:
    from ..core.metabase import utils as db_tools


@logger.catch(reraise=True)
async def insert_data(sa_model: DeclarativeMeta, data: Union[BaseModel, Dict[str, Any]]) -> int:
    exclude_fields = ('id', 'create_ts')

    if isinstance(data, BaseModel):
        data = data.dict(exclude=set(exclude_fields))

    return await db_tools.create_model(sa_model, data, exclude_fields, commit=True)


@logger.catch(reraise=True)
async def read_one(sa_model: DeclarativeMeta, object_id: int) -> Optional[Dict[str, Any]]:
    return await db_tools.read_model_by_id(sa_model, object_id)


@logger.catch(reraise=True)
async def read_all(sa_model: DeclarativeMeta) -> List[Dict[str, Any]]:
    return await db_tools.read_all_models(sa_model)


@logger.catch(reraise=True)
async def update_data(sa_model: DeclarativeMeta, object_id: int, update_values: Union[BaseModel, Dict[str, Any]]):
    if isinstance(update_values, BaseModel):
        update_values = update_values.dict()

    upd_data = {k: v for k, v in update_values.items() if v is not None}

    return await db_tools.update_model_by_id(sa_model, object_id, upd_data, commit=True)


@logger.catch(reraise=True)
async def delete_data(sa_model: DeclarativeMeta, object_id: int):
    return await db_tools.delete_model_by_id(sa_model, object_id, commit=True)
