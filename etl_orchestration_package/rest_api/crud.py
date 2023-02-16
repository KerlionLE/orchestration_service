from typing import List

from fastapi import APIRouter, HTTPException, status

from . import utils as api_tools
from . import models as api_models

from etl_orchestration_package.core.metabase import models as db_models

crud_router = APIRouter(prefix="/api/v1")


# =====================================================================================================================
# CREATE ENDPOINTS
# =====================================================================================================================


@crud_router.post('/services', status_code=201)
async def create_service(service_info: api_models.ServiceInfo) -> api_models.CreatedObjectResponse:
    service_id = await api_tools.insert_data(db_models.Service, service_info)

    return api_models.CreatedObjectResponse(object_id=service_id)


@crud_router.post('/processes', status_code=201)
async def create_process(process_info: api_models.ProcessInfo) -> api_models.CreatedObjectResponse:
    process_id = await api_tools.insert_data(db_models.Process, process_info)

    return api_models.CreatedObjectResponse(object_id=process_id)


@crud_router.post('/tasks', status_code=201)
async def create_task(task_info: api_models.TaskInfo) -> api_models.CreatedObjectResponse:
    task_id = await api_tools.insert_data(db_models.Task, task_info)

    return api_models.CreatedObjectResponse(object_id=task_id)


@crud_router.post('/task-run-statuses', status_code=201)
async def create_task_run_status(
        task_run_status_info: api_models.TaskRunStatusInfo) -> api_models.CreatedObjectResponse:
    task_run_status_id = await api_tools.insert_data(db_models.TaskRunStatus, task_run_status_info)

    return api_models.CreatedObjectResponse(object_id=task_run_status_id)


@crud_router.post('/task-runs', status_code=201)
async def create_task_run(task_run_info: api_models.TaskRunInfo) -> api_models.CreatedObjectResponse:
    task_run_id = await api_tools.insert_data(db_models.TaskRun, task_run_info)

    return api_models.CreatedObjectResponse(object_id=task_run_id)


@crud_router.post('/chains', status_code=201)
async def create_chain(chain_info: api_models.ChainInfo) -> api_models.CreatedObjectResponse:
    chain_id = await api_tools.insert_data(db_models.Chain, chain_info)

    return api_models.CreatedObjectResponse(object_id=chain_id)


@crud_router.post('/graphs', status_code=status.HTTP_201_CREATED)
async def create_graph(graph_info: api_models.GraphInfo) -> api_models.CreatedObjectResponse:
    graph_id = await api_tools.insert_data(db_models.Graph, graph_info)

    return api_models.CreatedObjectResponse(object_id=graph_id)


@crud_router.post('/graph-runs', status_code=status.HTTP_201_CREATED)
async def create_graph(graph_run_info: api_models.GraphRunInfo) -> api_models.CreatedObjectResponse:
    graph_run_id = await api_tools.insert_data(db_models.GraphRun, graph_run_info)

    return api_models.CreatedObjectResponse(object_id=graph_run_id)


# =====================================================================================================================
# READ ENDPOINTS
# =====================================================================================================================


@crud_router.get('/services/{service_id}')
async def read_service_by_id(service_id: int) -> api_models.ServiceInfo:
    service_info = await api_tools.read_one(db_models.Service, service_id)

    if service_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.ServiceInfo(**service_info)


@crud_router.get('/services')
async def read_all_services() -> List[api_models.ServiceInfo]:
    all_services = await api_tools.read_all(db_models.Service)

    return [api_models.ServiceInfo(**service) for service in all_services]


@crud_router.get('/processes/{process_id}')
async def read_process_by_id(process_id: int) -> api_models.ProcessInfo:
    process_info = await api_tools.read_one(db_models.Process, process_id)

    if process_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.ProcessInfo(**process_info)


@crud_router.get('/processes')
async def read_all_processes() -> List[api_models.ProcessInfo]:
    all_processes = await api_tools.read_all(db_models.Process)

    return [api_models.ProcessInfo(**process) for process in all_processes]


@crud_router.get('/tasks/{task_id}')
async def read_task_by_id(task_id: int) -> api_models.TaskInfo:
    task_info = await api_tools.read_one(db_models.Task, task_id)

    if task_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.TaskInfo(**task_info)


@crud_router.get('/tasks')
async def read_all_tasks() -> List[api_models.TaskInfo]:
    all_tasks = await api_tools.read_all(db_models.Task)

    return [api_models.TaskInfo(**task) for task in all_tasks]


@crud_router.get('/task-statuses')
async def read_all_task_statuses() -> List[api_models.TaskRunStatusInfo]:
    all_statuses = await api_tools.read_all(db_models.TaskRunStatus)

    return [api_models.TaskRunStatusInfo(**_status) for _status in all_statuses]


@crud_router.get('/task-runs/{task_run_id}')
async def read_task_run_by_id(task_run_id: int) -> api_models.TaskRunInfo:
    task_run_info = await api_tools.read_one(db_models.TaskRun, task_run_id)

    if task_run_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.TaskRunInfo(**task_run_info)


@crud_router.get('/task-runs')
async def read_all_task_runs() -> List[api_models.TaskRunInfo]:
    all_task_runs = await api_tools.read_all(db_models.TaskRun)

    return [api_models.TaskRunInfo(**task_run) for task_run in all_task_runs]


@crud_router.get('/chains/{chain_id}')
async def read_chain_by_id(chain_id: int) -> api_models.ChainInfo:
    chain_info = await api_tools.read_one(db_models.Chain, chain_id)

    if chain_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.ChainInfo(**chain_info)


@crud_router.get('/chains')
async def read_all_chains() -> List[api_models.ChainInfo]:
    all_chains = await api_tools.read_all(db_models.Chain)

    return [api_models.ChainInfo(**chain) for chain in all_chains]


@crud_router.get('/graphs/{graph_id}')
async def read_graph_by_id(graph_id: int) -> api_models.GraphInfo:
    graph_info = await api_tools.read_one(db_models.Graph, graph_id)

    if graph_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.GraphInfo(**graph_info)


@crud_router.get('/graphs')
async def read_all_graphs() -> List[api_models.GraphInfo]:
    all_chains = await api_tools.read_all(db_models.Graph)

    return [api_models.GraphInfo(**chain) for chain in all_chains]


@crud_router.get('/graph-runs/{graph_run_id}')
async def read_graph_run_by_id(graph_run_id: int) -> api_models.GraphRunInfo:
    graph_info = await api_tools.read_one(db_models.GraphRun, graph_run_id)

    if graph_info is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return api_models.GraphRunInfo(**graph_info)


@crud_router.get('/graph-runs')
async def read_all_graph_runs() -> List[api_models.GraphRunInfo]:
    all_chains = await api_tools.read_all(db_models.GraphRun)

    return [api_models.GraphRunInfo(**chain) for chain in all_chains]


# =====================================================================================================================
# UPDATE ENDPOINTS
# =====================================================================================================================


@crud_router.put('/tasks/{task_id}')
async def update_task(task_id: int, update_value: api_models.TaskUpdateValue):
    await api_tools.update_data(db_models.Task, task_id, update_value)

    return dict(result='success')


@crud_router.put('/task-runs/{task_run_id}')
async def update_task_run(task_run_id: int, update_value: api_models.TaskRunUpdateValue):
    await api_tools.update_data(db_models.TaskRun, task_run_id, update_value)

    return dict(result='success')


@crud_router.put('/graph-runs/{graph_run_id}')
async def update_graph_run(graph_run_id: int, update_value: api_models.GraphRunUpdateValue):
    await api_tools.update_data(db_models.GraphRun, graph_run_id, update_value)

    return dict(result='success')


# =====================================================================================================================
# DELETE ENDPOINTS
# =====================================================================================================================


@crud_router.delete('/tasks/{task_id}')
async def delete_task(task_id: int):
    await api_tools.delete_data(db_models.Task, task_id)

    return dict(result='success')


@crud_router.delete('/task-runs/{task_run_id}')
async def delete_task_run(task_run_id: int):
    await api_tools.delete_data(db_models.TaskRun, task_run_id)

    return dict(result='success')


@crud_router.delete('/chains/{chain_id}')
async def delete_chain(chain_id: int):
    await api_tools.delete_data(db_models.Chain, chain_id)

    return dict(result='success')


@crud_router.delete('/graphs/{graph_id}')
async def delete_graph(graph_id: int):
    await api_tools.delete_data(db_models.Graph, graph_id)

    return dict(result='success')


@crud_router.delete('/graph-runs/{graph_run_id}')
async def delete_graph_run(graph_run_id: int):
    await api_tools.delete_data(db_models.GraphRun, graph_run_id)

    return dict(result='success')
