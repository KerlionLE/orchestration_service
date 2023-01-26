# def create_task_run_config(task_id, **config):
#     task = objects.Task.get(id=task_id)
#     config_template = task.config_template
#
#     return config_template.format(**config)
#
#
# def create_task_run(task_id, **config):
#     if task_id is not None:
#         return TaskRun(
#             task=task_id,
#             status=None,
#             config=create_task_run_config(task_id, **config),
#             result=None
#         )
#
#
# def get_next_task_runs(previous_task_run=None):
#     previous_task_id = None
#     previous_task_run_result = dict()
#
#     if previous_task_run is not None:
#         previous_task_id = previous_task_run.task.id
#         previous_task_run_result = previous_task_run.result
#
#     chains = objects.Chain.filter(prev_task=previous_task_id)
#
#     return [
#         create_task_run(
#             task_id=chain.next_task,
#             **previous_task_run_result
#         ) for chain in chains
#     ]  # в списке могут быть None

# class Chain:
#     def __init__(self, _id, previous_task_id, next_task_id):
#         self.id = _id
#         self.previous_task_id = previous_task_id
#         self.next_task_id = next_task_id


# if __name__ == '__main__':
#     data = [
#         (1, 1, 2),
#         (2, 1, 3),
#         (3, 1, 4),
#         (4, 5, 3),
#         (5, 6, 7),
#         (6, 7, 8),
#         (7, 7, 9),
#         (8, 8, 10),
#         (9, 9, 10)
#     ]
#
#     chains = [Chain(_id, prv, nxt) for _id, prv, nxt in data]
#
#     dct = dict()
#     for c in chains:
#         if c.next_task_id not in dct:
#             dct[c.next_task_id] = [c.previous_task_id]
#         else:
#             dct[c.next_task_id].append(c.previous_task_id)
#
#     print('qq')

# def create_new_graph(task_id):
#     """
#     НУЖНО ПОСТОРИТЬ СЛОВАРЬ ЗАВИСИМОСТЕЙ С ИСПОЛЬЗОВААНИЕМ TASKRUN_ID И С НИМ РАБОТАТЬ
#
#
#     Строим граф тасков по task_id
#     1. Получаем все Chain у которых previous_task = task_id
#     2. Получаем Graph (один!) (из GraphChain) у которых chain = chain_id
#     3. Проверяем существует ли GraphRun с таким graph_id и статусом RUNNING
#         Если НЕ существует:
#             3.1. Создаем GraphRun со статусом CREATED (c указанием полученного graph_id)
#             3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
#             3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
#             3.4. Создаем TaskRun'ы со статусом CREATED (параллельно создается словарь -> {task_id: TaskRun})
#                  (по всем next_task_id, previous_task_id, кроме того который пришел )
#             3.5. Создаем TaskRun с пришедшем task_id и статусом SUCCEED
#             3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей по task_run_id
#             3.7. Создать GraphRunTaskRun записи с исспользованием сгенерированного словаря
#         Если существует (это случай когда для запуска новой задачи нужно дождаться N задач не зарегистрированных [1, 2] -> 3):
#             3.1. По graph_run_id получаем из GraphRunTaskRun все task_run_id
#             3.2. Строим словарь {task_id: task_run_id}
#                  3.2.1 У TaskRun c пришедшем task_id меняем статус на SUCCEED (из CREATED (!))
#                  3.2.2 Если он уже в статусе SUCCEED нужно создавать новый GraphRun
#             3.3. Получаем все Chain (из GraphChain) у которых graph = graph_id
#             3.4. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
#             3.5. Из словарей зависимостей (п. 3.4, 3.2) строим новый словарь зависимостей по task_run_id
#     4. Формируем новые задачи на выполнение
#       4.1 Итерируемся по словарю зависимостей {next_task_run_id: [previous_task_run_id1, previous_task_run_id2, ...]}
#           Если все задачи в списке previous в статусе SUCCEED
#               -> Меняем статус TaskRun с task_run_id==next_task_run_id на QUEUED и отправляем задачу в топик кафки
#     """
#     pass


# Пример сообщений
# msg = {
#     'metadata': {
#         'task_id': 1
#     },
#     'config': {},
#     'result': {}
# }
#
# msg = {
#     'metadata': {
#         'graphrun_id': 1,
#         'taskrun_id': 1
#     },
#     'config': {},
#     'result': {}
# }

# ---------------------------------------------------------------------

import json
import asyncio

from collections import defaultdict

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from ..metabase import get_metabase
from ..metabase.models import Chain, GraphChain, GraphRun, TaskRun, GraphRunTaskRun
from ..metabase.utils import execute_query

from ..queue import get_queue


def metabase_select_wrapper(select_func):
    async def wrapper(*args, **kwargs):
        async_session = await get_metabase().get_db().__anext__()

        select_query = select_func(*args, **kwargs)

        query_result = await execute_query(async_session, select_query)
        return [item.to_dict() for item in query_result.scalars()]

    return wrapper


def metabase_insert_wrapper(insert_function):
    async def wrapper(*args, **kwargs):
        async_session = await get_metabase().get_db().__anext__()

        select_query = insert_function(*args, **kwargs)

        query_result = await execute_query(async_session, select_query)
        inserted_id, *_ = query_result.inserted_primary_key
        return inserted_id

    return wrapper


def metabase_update_wrapper(update_function):
    async def wrapper(*args, **kwargs):
        async_session = await get_metabase().get_db().__anext__()

        select_query = update_function(*args, **kwargs)

        _ = await execute_query(async_session, select_query)

        return

    return wrapper


@metabase_select_wrapper
def get_model_by_id(model, _id):
    return select(model).where(model.id == _id)


@metabase_select_wrapper
def get_chains_by_prv_task_id(previous_task_id):
    return select(Chain).where(Chain.previous_task_id == previous_task_id)


@metabase_select_wrapper
def get_chains_by_ids(chain_ids):
    return select(Chain).where(Chain.id.in_(chain_ids))


@metabase_select_wrapper
def get_chains_by_graph_id(graph_id):
    return select(GraphChain).where(GraphChain.graph_id == graph_id)


@metabase_select_wrapper
def get_graph_by_chain_id(chain_id):
    return select(GraphChain).where(GraphChain.chain_id == chain_id)


@metabase_select_wrapper
def get_graph_run_by_graph_id(graph_id, status_id):
    return select(GraphRun).where(GraphRun.graph_id == graph_id).where(GraphRun.status_id == status_id)


@metabase_select_wrapper
def get_graph_run_task_run_by_graph_run_id(graph_run_id):
    return select(GraphRunTaskRun).where(GraphRunTaskRun.graph_run_id == graph_run_id)


@metabase_insert_wrapper
def create_graph_run(graph_id, status_id=2):
    return insert(GraphRun).values(graph_id=graph_id, status_id=status_id, config={}, result={})


@metabase_insert_wrapper
def create_task_run(task_id, status_id=2):
    return insert(TaskRun).values(task_id=task_id, status_id=status_id, config={}, result={})


@metabase_insert_wrapper
def create_graph_run_task_run(graph_run_id, task_run_id):
    return insert(GraphRunTaskRun).values(graph_run_id=graph_run_id, task_run_id=task_run_id)


@metabase_update_wrapper
def update_model_field_value(model, _id, field, value):
    return update(model).where(model.id == _id).values(**{field: value})


# --------------------------------------------------------------------------------------------------------------------

async def handle_newest_task(finished_task_id):
    pass


async def handle_finished_task(fininished_task_run_id):
    pass


async def _get_all_graph_id_by_task_id(task_id):
    graph_ids = set()

    # 1. Получаем все Chain у которых previous_task = task_id
    chains = await get_chains_by_prv_task_id(task_id)

    # 2. Получаем все Graph (из GraphChain) у которых chain = chain_id
    for chain in chains:
        graph_chains = await get_graph_by_chain_id(chain.get('id'))
        for gc in graph_chains:
            graph_ids.add(gc.get('graph_id'))

    return graph_ids


def _create_next_prev_tasks_dict(chains):
    dct = defaultdict(list)
    for c in chains:
        dct[c.get('next_task_id')].append(c.get('previous_task_id'))

    return dct


async def _create_task_runs(next_prev_tasks_dict, finished_tasks=None):
    dct2 = dict()
    _finished_tasks = finished_tasks or list()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        n_task_run_id = await create_task_run(task_id=n_task_id)
        dct2[n_task_id] = n_task_run_id
        for p_task_id in p_task_ids:
            if p_task_id not in dct2:
                status = 4 if p_task_id in _finished_tasks else 2
                p_task_run_id = await create_task_run(task_id=p_task_id, status_id=status)
                dct2[p_task_id] = p_task_run_id

    return dct2


async def _get_task_run_dict(graph_run_task_run_list):
    dct = dict()

    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        dct[tr.get('task_id')] = tr.get('id')

    return dct


async def _get_task_runs_from_graph_run(graph_run_task_run_list):
    for data in graph_run_task_run_list:
        task_runs = await get_model_by_id(model=TaskRun, _id=data.get('task_run_id'))
        for tr in task_runs:
            yield tr


async def _update_task_runs(graph_run_task_run_list, finished_tasks):
    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        if tr.get('task_id') in finished_tasks:
            if tr.get('status_id') == 2:
                _ = await update_model_field_value(model=TaskRun, _id=tr.get('id'), field='status_id', value=4)
            # elif tr.get('status_id') == 4:
            # graph_run_id, next_prev_task_runs_dict = await _create_and_initialize_new_graph_run(graph_id=_id)


async def _check_for_new_graph_run(graph_run_task_run_list, finished_tasks):
    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        if tr.get('task_id') in finished_tasks:
            if tr.get('status_id') == 4:
                return True
    return False


def _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict):
    dct3 = dict()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        dct3[task_runs_dict.get(n_task_id)] = [task_runs_dict.get(p_task_id) for p_task_id in p_task_ids]

    return dct3


def _create_graph_run_task_run(graph_run_id, task_run_ids):
    ids_list = list()
    for tr_id in task_run_ids:
        _id = await create_graph_run_task_run(
            graph_run_id=graph_run_id,
            task_run_id=tr_id
        )
        ids_list.append(_id)
    return ids_list


async def _create_and_initialize_new_graph_run(graph_id, status_id=1):
    # 3.1. Создаем GraphRun со статусом RUNNING (c указанием полученного graph_id)
    graph_run_id = await create_graph_run(graph_id=graph_id, status_id=status_id)

    # 3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
    gc_list = await get_chains_by_graph_id(graph_id=graph_id)
    chains = await get_chains_by_ids([gc.get('chain_id') for gc in gc_list])

    # 3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = _create_next_prev_tasks_dict(chains)

    # 3.4. Создаем TaskRun'ы со статусом CREATED (параллельно создается словарь -> {task_id: TaskRun})
    #      (по всем next_task_id, previous_task_id, кроме того который пришел )
    task_runs_dict = await _create_task_runs(next_prev_tasks_dict)

    # 3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

    # 3.7. Создать GraphRunTaskRun записи с использованием сгенерированного словаря
    ids_set = set()
    for _n_task_run_id, _p_task_run_ids in next_prev_task_runs_dict.items():
        ids_set.add(_n_task_run_id)
        for _p_task_run_id in _p_task_run_ids:
            ids_set.add(_p_task_run_id)

    _ = _create_graph_run_task_run(graph_run_id, ids_set)

    return graph_run_id, next_prev_task_runs_dict


async def _get_and_update_running_graph_run(graph_run_id, graph_id, finished_tasks=None):
    _finished_tasks = finished_tasks or list()

    graph_run_task_run_list = await get_graph_run_task_run_by_graph_run_id(graph_run_id=graph_run_id)
    new_graph_run_flag = await _check_for_new_graph_run(graph_run_task_run_list, finished_tasks)
    if new_graph_run_flag:
        return await _create_and_initialize_new_graph_run(graph_id=graph_id)

    _ = await _update_task_runs(graph_run_task_run_list, finished_tasks)
    task_runs_dict = await _get_task_run_dict(graph_run_task_run_list)

    # 3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
    gc_list = await get_chains_by_graph_id(graph_id=graph_id)
    chains = await get_chains_by_ids([gc.get('chain_id') for gc in gc_list])

    # 3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = _create_next_prev_tasks_dict(chains)

    # 3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

    return graph_id, next_prev_task_runs_dict


# --------------------------------------------------------------------------------------------------------------------

async def orchestration():
    queue_interface = get_queue()

    async for msg in queue_interface.consume_data('finished_tasks'):
        data = json.loads(msg.value)
        metadata = data.get('metadata')
        if not metadata:
            raise Exception("BLANK METADATA!!!")

        # finished_task_id = metadata.get('task_id')
        finished_task_id = 5
        if finished_task_id is not None:
            gc_ids = await _get_all_graph_id_by_task_id(finished_task_id)

            for _id in gc_ids:
                # 3. Проверяем существует ли GraphRun с таким graph_id и статусом RUNNING
                running_graph_runs = await get_graph_run_by_graph_id(graph_id=_id, status_id=1)
                # Если существует
                # (это случай когда для запуска новой задачи нужно дождаться N задач
                # автоматически зарегистрированных в таблице TaskRun - [1, 2, ..., N] -> 3):
                graph_runs_dict = dict()
                for graph_run in running_graph_runs:
                    graph_run_id, next_prev_task_runs_dict = await _get_and_update_running_graph_run(
                        graph_run_id=graph_run.get('id'), graph_id=_id, finished_tasks=[finished_task_id]
                    )
                    graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

                # Если НЕ существует:
                if not graph_runs_dict:
                    graph_run_id, next_prev_task_runs_dict = await _create_and_initialize_new_graph_run(graph_id=_id)
                    graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

        else:
            raise NotImplementedError

        print('qq')


def orchestration_process():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(orchestration())
    loop.close()
