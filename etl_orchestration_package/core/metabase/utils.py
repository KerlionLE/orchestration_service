import traceback
from typing import Dict, Any, Union

from loguru import logger
from pydantic import BaseModel
from sqlalchemy import literal_column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeMeta


@logger.catch(reraise=True)
async def insert_data(
        async_session: AsyncSession,
        sa_model: DeclarativeMeta,
        data: Union[BaseModel, Dict[str, Any]],
) -> int:
    if isinstance(data, BaseModel):
        data = data.dict()

    insert_statement = insert(sa_model).values(**data)
    print(insert_statement)
    try:
        result = await async_session.execute(insert_statement)
        await async_session.commit()
    except Exception as e:
        traceback.format_exc()
        raise e

    print(dir(result))
    # print(result.scalar())

    # result_1 = result.fetchone()[0]
    inserted_id, *_ = result.inserted_primary_key

    return inserted_id
