from typing import List

from fastapi import APIRouter, HTTPException, status

from .models import ServiceInfo, CreatedObjectResponse, ProcessInfo, TaskInfo, TaskRunStatusInfo, TaskRunInfo, \
    ChainInfo, TaskUpdateValue, TaskRunUpdateValue, GraphInfo, GraphRunInfo, GraphRunUpdateValue

from core.metabase import get_metabase
from core.metabase.models import Service, Process, Task, TaskRunStatus, TaskRun, Chain, Graph, GraphRun
from core.metabase.utils import insert_data, read_one, read_all, update_data, delete_data

crud_router = APIRouter(prefix="/api/v1")


# =====================================================================================================================
# CREATE ENDPOINTS
# =====================================================================================================================


@crud_router.post('/services', status_code=201)
async def create_service(service_info: ServiceInfo) -> CreatedObjectResponse:
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


@crud_router.post('/graphs', status_code=status.HTTP_201_CREATED)
async def create_graph(graph_info: GraphInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    graph_id = await insert_data(async_session, Graph, graph_info)

    return CreatedObjectResponse(object_id=graph_id)


@crud_router.post('/graph-runs', status_code=status.HTTP_201_CREATED)
async def create_graph(graph_run_info: GraphRunInfo) -> CreatedObjectResponse:
    async_session = await get_metabase().get_db().__anext__()
    graph_run_id = await insert_data(async_session, GraphRun, graph_run_info)

    return CreatedObjectResponse(object_id=graph_run_id)


# =====================================================================================================================
# READ ENDPOINTS
# =====================================================================================================================


@crud_router.get('/services/{service_id}')
async def read_service_by_id(service_id: int) -> ServiceInfo:
    async_session = await get_metabase().get_db().__anext__()
    service_info = await read_one(async_session, Service, service_id)

    if service_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return ServiceInfo(**service_info)


@crud_router.get('/services')
async def read_all_services() -> List[ServiceInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_services = await read_all(async_session, Service)

    return [ServiceInfo(**service) for service in all_services]


@crud_router.get('/processes/{process_id}')
async def read_process_by_id(process_id: int) -> ProcessInfo:
    async_session = await get_metabase().get_db().__anext__()
    process_info = await read_one(async_session, Process, process_id)

    if process_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return ProcessInfo(**process_info)


@crud_router.get('/processes')
async def read_all_processes() -> List[ProcessInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_processes = await read_all(async_session, Process)

    return [ProcessInfo(**process) for process in all_processes]


@crud_router.get('/tasks/{task_id}')
async def read_task_by_id(task_id: int) -> TaskInfo:
    async_session = await get_metabase().get_db().__anext__()
    task_info = await read_one(async_session, Task, task_id)

    if task_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return TaskInfo(**task_info)


@crud_router.get('/tasks')
async def read_all_tasks() -> List[TaskInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_tasks = await read_all(async_session, Task)

    return [TaskInfo(**task) for task in all_tasks]


@crud_router.get('/task-statuses')
async def read_all_task_statuses() -> List[TaskRunStatusInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_statuses = await read_all(async_session, TaskRunStatus)

    return [TaskRunStatusInfo(**status) for status in all_statuses]


@crud_router.get('/task-runs/{task_run_id}')
async def read_task_run_by_id(task_run_id: int) -> TaskRunInfo:
    async_session = await get_metabase().get_db().__anext__()
    task_run_info = await read_one(async_session, TaskRun, task_run_id)

    if task_run_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return TaskRunInfo(**task_run_info)


@crud_router.get('/task-runs')
async def read_all_task_runs() -> List[TaskRunInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_task_runs = await read_all(async_session, TaskRun)

    return [TaskRunInfo(**task_run) for task_run in all_task_runs]


@crud_router.get('/chains/{chain_id}')
async def read_chain_by_id(chain_id: int) -> ChainInfo:
    async_session = await get_metabase().get_db().__anext__()
    chain_info = await read_one(async_session, Chain, chain_id)

    if chain_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return ChainInfo(**chain_info)


@crud_router.get('/chains')
async def read_all_chains() -> List[ChainInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_chains = await read_all(async_session, Chain)

    return [ChainInfo(**chain) for chain in all_chains]


@crud_router.get('/graphs/{graph_id}')
async def read_graph_by_id(graph_id: int) -> GraphInfo:
    async_session = await get_metabase().get_db().__anext__()
    graph_info = await read_one(async_session, Graph, graph_id)

    if graph_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return GraphInfo(**graph_info)


@crud_router.get('/graphs')
async def read_all_graphs() -> List[GraphInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_chains = await read_all(async_session, Graph)

    return [GraphInfo(**chain) for chain in all_chains]


@crud_router.get('/graph-runs/{graph_run_id}')
async def read_graph_run_by_id(graph_run_id: int) -> GraphRunInfo:
    async_session = await get_metabase().get_db().__anext__()
    graph_info = await read_one(async_session, GraphRun, graph_run_id)

    if graph_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return GraphRunInfo(**graph_info)


@crud_router.get('/graph-runs')
async def read_all_graph_runs() -> List[GraphRunInfo]:
    async_session = await get_metabase().get_db().__anext__()
    all_chains = await read_all(async_session, GraphRun)

    return [GraphRunInfo(**chain) for chain in all_chains]


# =====================================================================================================================
# UPDATE ENDPOINTS
# =====================================================================================================================


@crud_router.put('/tasks/{task_id}')
async def update_task(task_id: int, update_value: TaskUpdateValue):
    async_session = await get_metabase().get_db().__anext__()
    await update_data(async_session, Task, task_id, update_value)

    return dict(result='success')


@crud_router.put('/task-runs/{task_run_id}')
async def update_task_run(task_run_id: int, update_value: TaskRunUpdateValue):
    async_session = await get_metabase().get_db().__anext__()
    await update_data(async_session, TaskRun, task_run_id, update_value)

    return dict(result='success')


@crud_router.put('/graph-runs/{graph_run_id}')
async def update_graph_run(graph_run_id: int, update_value: GraphRunUpdateValue):
    async_session = await get_metabase().get_db().__anext__()
    await update_data(async_session, GraphRun, graph_run_id, update_value)

    return dict(result='success')

# =====================================================================================================================
# DELETE ENDPOINTS
# =====================================================================================================================


@crud_router.delete('/tasks/{task_id}')
async def delete_task(task_id: int):
    async_session = await get_metabase().get_db().__anext__()
    await delete_data(async_session, Task, task_id)

    return dict(result='success')


@crud_router.delete('/task-runs/{task_run_id}')
async def delete_task_run(task_run_id: int):
    async_session = await get_metabase().get_db().__anext__()
    await delete_data(async_session, TaskRun, task_run_id)

    return dict(result='success')


@crud_router.delete('/chains/{chain_id}')
async def delete_chain(chain_id: int):
    async_session = await get_metabase().get_db().__anext__()
    await delete_data(async_session, Chain, chain_id)

    return dict(result='success')


@crud_router.delete('/graphs/{graph_id}')
async def delete_graph(graph_id: int):
    async_session = await get_metabase().get_db().__anext__()
    await delete_data(async_session, Graph, graph_id)

    return dict(result='success')


@crud_router.delete('/graph-runs/{graph_run_id}')
async def delete_graph_run(graph_run_id: int):
    async_session = await get_metabase().get_db().__anext__()
    await delete_data(async_session, GraphRun, graph_run_id)

    return dict(result='success')
