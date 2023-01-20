from fastapi import APIRouter

from .models import ServiceInfo, CreatedObjectResponse, ProcessInfo, TaskInfo, TaskRunStatusInfo, TaskRunInfo, ChainInfo

# from core.metabase import get_metabase
# from core.metabase.models import Service
# from core.metabase.utils import insert_data
from ..core.metabase import get_metabase
from ..core.metabase.models import Service, Process, Task, TaskRunStatus, TaskRun, Chain
from ..core.metabase.utils import insert_data, read_one, read_all
from ..settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

crud_router = APIRouter(prefix="/api/v1")


# =====================================================================================================================
# CREATE ENDPOINTS
# =====================================================================================================================


SQLALCHEMY_DATABASE_URL = URL.create(
    drivername='postgresql+asyncpg',
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

# METADATA = MetaData(schema=SCHEMA_NAME)

# engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)
#
#
# AsyncSessionFactory = sessionmaker(
#     engine,
#     autoflush=False,
#     expire_on_commit=False,
#     class_=AsyncSession
# )
#
#
# async def get_db() -> AsyncGenerator:
#     async with AsyncSessionFactory() as session:
#         logger.debug(f"ASYNC Pool: {engine.pool.status()}")
#         yield session


@crud_router.post('/services', status_code=201)
async def create_service(
        service_info: ServiceInfo,
        # async_session: AsyncSession = Depends(get_db),
) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    service_id = await insert_data(async_session, Service, service_info)

    return CreatedObjectResponse(object_id=service_id)


@crud_router.post('/processes', status_code=201)
async def create_process(process_info: ProcessInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    process_id = await insert_data(async_session, Process, process_info)

    return CreatedObjectResponse(object_id=process_id)


@crud_router.post('/tasks', status_code=201)
async def create_task(task_info: TaskInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    task_id = await insert_data(async_session, Task, task_info)

    return CreatedObjectResponse(object_id=task_id)


@crud_router.post('/task-run-statuses', status_code=201)
async def create_task_run_status(task_run_status_info: TaskRunStatusInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    task_run_status_id = await insert_data(async_session, TaskRunStatus, task_run_status_info)

    return CreatedObjectResponse(object_id=task_run_status_id)


@crud_router.post('/task-runs', status_code=201)
async def create_task_run(task_run_info: TaskRunInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    task_run_id = await insert_data(async_session, TaskRun, task_run_info)

    return CreatedObjectResponse(object_id=task_run_id)


@crud_router.post('/chains', status_code=201)
async def create_chain(chain_info: ChainInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    chain_id = await insert_data(async_session, Chain, chain_info)

    return CreatedObjectResponse(object_id=chain_id)


# =====================================================================================================================
# READ ENDPOINTS
# =====================================================================================================================


@crud_router.get('/services/{service_id}')
async def read_service_by_id(service_id: int) -> ServiceInfo:
    async_session = await get_metabase().get_db().__anext__()
    service_info = await read_one(async_session, Service, service_id)

    return ServiceInfo(**service_info)


@crud_router.get('/services')
async def read_all_services() -> List[ServiceInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_services = await read_all(async_session, Service)

    return [ServiceInfo(**service) for service in all_services]


@crud_router.get('/processes/{process_id}')
async def read_process_by_id(process_id: int) -> ProcessInfo:
    async_session = await get_metabase().get_db().__anext__()
    service_info = await read_one(async_session, Process, process_id)

    return ProcessInfo(**service_info)


# =====================================================================================================================
# UPDATE ENDPOINTS
# =====================================================================================================================


@crud_router.patch('/services/{service_id}')
async def update_service():
    async_session = await get_metabase().get_db().__anext__()
    pass


# =====================================================================================================================
# DELETE ENDPOINTS
# =====================================================================================================================


@crud_router.delete('/services/{service_id}')
async def delete_service(service_id: int):
    async_session = await get_metabase().get_db().__anext__()
    pass

