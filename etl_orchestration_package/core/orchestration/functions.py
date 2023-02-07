import json
import asyncio

from collections import defaultdict

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from ..metabase import get_metabase, add_metabase
from ..metabase.models import Chain, GraphChain, GraphRun, TaskRun, GraphRunTaskRun, \
    Task, Process, Service, Graph, TaskRunStatus
from ..metabase.utils import execute_query

from ..queue import get_queue, add_queue


# @metabase_select_wrapper
# def _get_model_by_id(model, _id):
#     return select(model).where(model.id == _id)


# async def get_model_by_id(model, _id):
#     lst = await _get_model_by_id(model, _id)
#     return lst[0]


# @metabase_select_wrapper
# def _get_status_id_by_value(status):
#     return select(TaskRunStatus).where(TaskRunStatus.status == status)


# async def get_status_id_by_value(status):
#     lst = await _get_status_id_by_value(status)
#     if lst:
#         return lst[0].get('id')
#     else:
#         # logging.error("UNKNOWN STATUS!!!")
#         raise Exception("UNKNOWN STATUS!!!")  # TODO: Убрать потом


# @metabase_select_wrapper
# def get_chains_by_prv_task_id(previous_task_id):
#     return select(Chain).where(Chain.previous_task_id == previous_task_id)
#
#
# @metabase_select_wrapper
# def get_chains_by_ids(chain_ids):
#     return select(Chain).where(Chain.id.in_(chain_ids))
#
#
# @metabase_select_wrapper
# def get_chains_by_graph_id(graph_id):
#     return select(GraphChain).where(GraphChain.graph_id == graph_id)
#
#
# @metabase_select_wrapper
# def get_graph_by_chain_id(chain_id):
#     return select(GraphChain).where(GraphChain.chain_id == chain_id)
#
#
# @metabase_select_wrapper
# def get_graph_run_by_graph_id(graph_id, status_id):
#     return select(GraphRun).where(GraphRun.graph_id == graph_id).where(GraphRun.status_id == status_id)
#
#
# @metabase_select_wrapper
# def get_graph_run_task_run_by_graph_run_id(graph_run_id):
#     return select(GraphRunTaskRun).where(GraphRunTaskRun.graph_run_id == graph_run_id)
#
#
# @metabase_insert_wrapper
# def create_graph_run(graph_id, status_id=2):
#     return insert(GraphRun).values(graph_id=graph_id, status_id=status_id, config={}, result={})
#
#
# @metabase_insert_wrapper
# def create_task_run(task_id, status_id=2, config=None, result=None):
#     return insert(TaskRun).values(
#         task_id=task_id, status_id=status_id,
#         config=config or dict(),
#         result=result or dict()
#     )
#
#
# @metabase_insert_wrapper
# def create_graph_run_task_run(graph_run_id, task_run_id):
#     return insert(GraphRunTaskRun).values(graph_run_id=graph_run_id, task_run_id=task_run_id)


# --------------------------------------------------------------------------------------------------------------------

async def handle_newest_task(finished_task_id, task_run_result):
    """
    Функция генерации нового GraphRun
    """
    finished_tasks = [{
        'task_id': finished_task_id,
        'status': 'SUCCEED',  # TODO: Убрать в константы или подумать еще...
        'result': task_run_result
    }]

    # 1. Получаем список всех Graph, в которых фигурирует завершенный task
    graph_ids = await _get_all_graph_id_by_task_id(finished_task_id)

    graph_runs_dict = dict()
    for graph_id in graph_ids:
        # 2. Проверяем существует ли GraphRun'ы с таким graph_id и статусом RUNNING
        running_graph_runs = await read_models_by_filter(
            model=GraphRun,
            filter_dict={
                'graph_id': graph_id,
                'status_id': 1  # TODO: status_id?
            }
        )

        # 2.1 Если существуют
        # (это случай когда для запуска новой задачи (TaskRun) нужно дождаться N задач
        # автоматически зарегистрированных в таблице TaskRun - [1, 2, ..., N] -> 3):
        for graph_run in running_graph_runs:
            # 2.1.1 Получаем список всех TaskRun в найденном GraphRun
            graph_run_task_run_list = await read_models_by_filter(
                model=GraphRunTaskRun,
                filter_dict={'graph_run_id': graph_run.get('id')}
            )

            # 2.1.2 Проверяем не завершен ли полученный task в найденном GraphRun
            new_graph_run_flag = await _check_for_new_graph_run(graph_run_task_run_list, finished_tasks)
            if not new_graph_run_flag:
                # 2.1.3 Если не завершен, то обновляем статус найденного task_run
                graph_run_id, next_prev_task_runs_dict = await _get_and_update_running_graph_run(
                    graph_run_id=graph_run.get('id'),
                    finished_tasks=finished_tasks
                )
                graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

            if new_graph_run_flag:
                # 2.1.4 Если завершен, то создаем и инициализируем новый GraphRun
                graph_run_id, next_prev_task_runs_dict = await _create_and_initialize_new_graph_run(
                    graph_id=graph_id,
                    finished_tasks=finished_tasks
                )
                graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

        # 2.2 Если НЕ существует, то создаем и инициализируем новый GraphRun:
        if not running_graph_runs:
            graph_run_id, next_prev_task_runs_dict = await _create_and_initialize_new_graph_run(
                graph_id=graph_id,
                finished_tasks=finished_tasks
            )
            graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

    return graph_runs_dict


async def handle_finished_task(graph_run_id, finished_task_run_id, task_run_result, task_run_status):
    task_run = await get_model_by_id(TaskRun, finished_task_run_id)

    finished_tasks = [{
        'task_id': task_run.get('task_id'),
        'status': task_run_status,
        'result': task_run_result
    }]

    graph_run_id, next_prev_task_runs_dict = await _get_and_update_running_graph_run(
        graph_run_id,
        finished_tasks=finished_tasks
    )

    return {graph_run_id: next_prev_task_runs_dict}


# --------------------------------------------------------------------------------------------------------------------

async def create_and_initialize_new_graph_run(graph_id, status_id=1, finished_tasks=None):
    # 3.1. Создаем GraphRun со статусом RUNNING (c указанием полученного graph_id)
    graph_run_id = await create_graph_run(graph_id=graph_id, status_id=status_id)

    # 3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
    gc_list = await get_chains_by_graph_id(graph_id=graph_id)
    chains = await get_chains_by_ids([gc.get('chain_id') for gc in gc_list])

    # 3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = _create_next_prev_tasks_dict(chains)

    # 3.4. Создаем TaskRun'ы со статусом CREATED (параллельно создается словарь -> {task_id: TaskRun})
    #      (по всем next_task_id, previous_task_id, кроме того который пришел )
    task_runs_dict = await _create_task_runs(next_prev_tasks_dict, finished_tasks)

    # 3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

    # 3.7. Создать GraphRunTaskRun записи с использованием сгенерированного словаря
    ids_set = set()
    for _n_task_run_id, _p_task_run_ids in next_prev_task_runs_dict.items():
        ids_set.add(_n_task_run_id)
        for _p_task_run_id in _p_task_run_ids:
            ids_set.add(_p_task_run_id)

    _ = await _create_graph_run_task_run(graph_run_id, ids_set)

    return graph_run_id, next_prev_task_runs_dict


# --------------------------------------------------------------------------------------------------------------------

async def get_message_for_service(graph_run_id, next_task_run_id, *previous_task_run_ids):
    graph_run = await get_model_by_id(model=GraphRun, _id=graph_run_id)
    graph = await get_model_by_id(model=Graph, _id=graph_run.get('graph_id'))

    next_task_run = await get_model_by_id(model=TaskRun, _id=next_task_run_id)
    next_task = await get_model_by_id(model=Task, _id=next_task_run.get('task_id'))

    process = await get_model_by_id(model=Process, _id=next_task.get('process_id'))
    service = await get_model_by_id(model=Service, _id=process.get('service_id'))

    next_task_config_template = next_task.get('config_template')
    next_task_run_config = next_task_run.get('config', dict())
    for previous_task_run_id in previous_task_run_ids:
        previous_task_run = await get_model_by_id(model=TaskRun, _id=previous_task_run_id)

        previous_task_run_result = previous_task_run.get('result', dict())
        for key, default_value in next_task_config_template.items():
            next_task_run_config[key] = previous_task_run_result.get(key, default_value)

    return service.get('name'), {
        "metadata": {
            "graphrun_id": graph_run_id,
            "taskrun_id": next_task_run_id,
            "process_uid": process.get('uid'),
            "service_name": service.get('name'),
            "graph_name": graph.get('name')
        },
        "config": next_task_run_config,
        "result": dict()
    }
