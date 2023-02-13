import traceback
from typing import Union

from sqlalchemy.orm import Query

from sqlalchemy import select, update, delete
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from loguru import logger

from . import models
from . import get_metabase


# ------------------------------------------------------------------------------------

async def execute_query(async_session: AsyncSession, query: Query, autocommit=False) -> CursorResult:
    result = await async_session.execute(query)
    if autocommit:
        await commit(async_session)

    return result


# ------------------------------------------------------------------------------------

async def commit(async_session: AsyncSession = None):
    if async_session is None:
        async_session = await get_metabase().get_current_session()

    try:
        await async_session.commit()
        logger.debug("ALL CHANGES SUCCESSFULLY COMMIT!!!")
    except Exception as e:
        traceback.format_exc()
        raise e


# ------------------------------------------------------------------------------------


def metabase_select_wrapper(read_one=False):
    def decorator(select_func):
        async def wrapper(*args, **kwargs) -> Union[dict, list]:
            commit_flag = kwargs.pop('commit', False)
            query = select_func(*args, **kwargs)

            async_session = await get_metabase().get_current_session()
            query_result = await execute_query(async_session, query, autocommit=commit_flag)

            if read_one:
                result_data = query_result.scalars().first()
                if result_data:
                    return result_data.to_dict()

                logger.warning(f"EMPTY RESULT FOR QUERY: \n'{query}', \nARGS: '{args}', \nKWARGS: '{kwargs}'")
                return

            return [item.to_dict() for item in query_result.scalars()]

        return wrapper

    return decorator


# ------------------------------------------------------------------------------------

def metabase_insert_wrapper(insert_function):
    async def wrapper(*args, **kwargs):
        commit_flag = kwargs.pop('commit', False)
        query = insert_function(*args, **kwargs)

        async_session = await get_metabase().get_current_session()
        query_result = await execute_query(async_session, query, autocommit=commit_flag)

        inserted_id, *_ = query_result.inserted_primary_key
        return inserted_id

    return wrapper


# ------------------------------------------------------------------------------------

def metabase_update_wrapper(update_function):
    async def wrapper(*args, **kwargs):
        commit_flag = kwargs.pop('commit', False)
        query = update_function(*args, **kwargs)

        async_session = await get_metabase().get_current_session()
        _ = await execute_query(async_session, query, autocommit=commit_flag)

        return

    return wrapper


# ------------------------------------------------------------------------------------

def metabase_delete_wrapper(delete_function):
    async def wrapper(*args, **kwargs):
        commit_flag = kwargs.pop('commit', False)
        query = delete_function(*args, **kwargs)

        async_session = await get_metabase().get_current_session()
        _ = await execute_query(async_session, query, autocommit=commit_flag)

        return

    return wrapper


# ------------------------------------------------------------------------------------

@metabase_select_wrapper(read_one=True)
def read_model_by_id(model, _id):
    return select(model).where(model.id == _id)


@metabase_select_wrapper()
def read_models_by_ids_list(model, ids_list):
    return select(model).where(model.id.in_(ids_list))


@metabase_select_wrapper()
def read_models_by_filter(model, filter_dict):
    def _read_command_generator(_command, filter_field, filter_value):
        return _command.where(model.__dict__[filter_field] == filter_value)

    command = select(model)
    for field, value in filter_dict.items():
        command = _read_command_generator(command, field, value)

    return command


@metabase_select_wrapper()
def read_all_models(model):
    return select(model)


@metabase_update_wrapper
def update_model_by_id(model, _id, update_data):
    return update(model).where(model.id == _id).values(**update_data)


@metabase_update_wrapper
def update_model_field_value(model, _id, field, value):
    return update(model).where(model.id == _id).values(**{field: value})


@metabase_update_wrapper
def update_model_by_id(model, _id, data):
    return update(model).where(model.id == _id).values(**data)


@metabase_delete_wrapper
def delete_model_by_id(model, _id):
    return delete(model).where(model.id == _id)


@metabase_insert_wrapper
def create_model(model, data, exclude_fields=None):
    logger.debug(f"CREATE NEW '{model}' OBJECT!")
    return get_metabase().insert(model, data, exclude_fields)


# MODELS UTILS
# -------------------------------------------------------------------------
@metabase_select_wrapper(read_one=True)
def get_status(status):
    return select(models.TaskRunStatus).where(models.TaskRunStatus.status == status)


async def get_status_id(status):
    status_data = await get_status(status)
    if status_data:
        return status_data.get('id')
    raise Exception('UNKNOWN STATUS!')
