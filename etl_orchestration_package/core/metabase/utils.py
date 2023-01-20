import traceback
from datetime import datetime
from typing import Dict, Any, Union, List

from loguru import logger
from pydantic import BaseModel
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeMeta, Query, defer


async def execute_query(async_session: AsyncSession, query: Query) -> CursorResult:
    result = await async_session.execute(query)
    try:
        await async_session.commit()
    except Exception as e:
        traceback.format_exc()
        # await async_session.rollback()
        raise e

    return result


@logger.catch(reraise=True)
async def insert_data(
        async_session: AsyncSession,
        sa_model: DeclarativeMeta,
        data: Union[BaseModel, Dict[str, Any]],
) -> int:
    exclude_fields = ('id', 'create_ts')

    if isinstance(data, BaseModel):
        data = data.dict(exclude=set(exclude_fields))

    insert_statement = insert(sa_model).values(**data).options(*[defer(col) for col in exclude_fields])

    result = await execute_query(async_session, insert_statement)

    inserted_id, *_ = result.inserted_primary_key

    return inserted_id


@logger.catch(reraise=True)
async def read_one(
        async_session: AsyncSession,
        sa_model: DeclarativeMeta,
        object_id: int,
) -> Dict[str, Any]:
    select_query = select(
        sa_model,
    ).where(
        sa_model.id == object_id,
    )

    query_result = await execute_query(async_session, select_query)

    instance = query_result.scalars().first()

    return instance.to_dict()


@logger.catch(reraise=True)
async def read_all(
        async_session: AsyncSession,
        sa_model: DeclarativeMeta,
) -> List[Dict[str, Any]]:
    select_query = select(
        sa_model,
    )

    query_result = await execute_query(async_session, select_query)
    result = [item.to_dict() for item in query_result.scalars()]

    return result


@logger.catch(reraise=True)
async def update_data():
    pass


@logger.catch(reraise=True)
async def delete_data(
        async_session: AsyncSession,
        sa_model: DeclarativeMeta,
        object_id: int,
):
    delete_statement = delete(sa_model).where(
        sa_model.id == object_id,
    )

    result = await execute_query(async_session, delete_statement)

    return
