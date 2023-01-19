from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ServiceInfo, CreatedObjectResponse

# from core.metabase import get_metabase
from core.metabase import get_metabase
from core.metabase.models import Service
from core.metabase.utils import insert_data

crud_router = APIRouter(prefix="/api/v1")


@crud_router.post('/services')
async def create_service(service_info: ServiceInfo):
    async_session = await get_metabase().get_db().__anext__()
    service_id = await insert_data(async_session, Service, service_info)

    return CreatedObjectResponse(object_id=service_id)

# @crud_router.post('/process')
# async def create_process(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
#
#
# @crud_router.post('/task')
# async def create_task(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
#
#
# @crud_router.post('/task_run_status')
# async def create_task_run_status(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
#
#
# @crud_router.post('/task_run')
# async def create_task_run(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
#
#
# @crud_router.post('/chain')
# async def create_chain(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
#
#
# @crud_router.post('/graph')
# async def create_chain(
#     db_session: AsyncSession = Depends(get_db),
# ):
#     pass
