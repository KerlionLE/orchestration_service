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
#             3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей оп task_run_id
#         Если существует:
#             3.1. Получаем из GraphRunTaskRun все task_run_id и
#             3.2. Строим словарь зависимостей {task_id: task_run_id} -> {}
#     4.
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

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from ..metabase import get_metabase
from ..metabase.models import Chain, GraphChain, GraphRun, TaskRun
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


@metabase_insert_wrapper
def create_graph_run(graph_id, status_id=2):
    return insert(GraphRun).values(graph_id=graph_id, status_id=status_id, config={}, result={})


@metabase_insert_wrapper
def create_task_run(task_id, status_id=2):
    return insert(TaskRun).values(task_id=task_id, status_id=status_id, config={}, result={})


async def orchestration():
    queue_interface = get_queue()

    async for msg in queue_interface.consume_data('finished_tasks'):
        data = json.loads(msg.value)
        metadata = data.get('metadata')
        if not metadata:
            raise Exception("BLANK METADATA!!!")

        finished_task_id = metadata.get('task_id')
        if finished_task_id is not None:
            gc_ids = set()

            chains = await get_chains_by_prv_task_id(finished_task_id)
            for chain in chains:
                graph_chains = await get_graph_by_chain_id(chain.get('id'))
                for gc in graph_chains:
                    gc_ids.add(gc.get('graph_id'))

            for _id in gc_ids:
                gr_list = await get_graph_run_by_graph_id(graph_id=_id, status_id=1)
                if gr_list:
                    for gr in gr_list:
                        raise NotImplementedError
                else:
                    _ = await create_graph_run(graph_id=_id)
                    gc_list = await get_chains_by_graph_id(graph_id=_id)

                    chains = await get_chains_by_ids([gc.get('chain_id') for gc in gc_list])

                    dct = dict()
                    for c in chains:
                        next_task_id = c.get('next_task_id')
                        previous_task_id = c.get('previous_task_id')
                        if next_task_id not in dct:
                            dct[next_task_id] = [previous_task_id]
                        else:
                            dct[next_task_id].append(previous_task_id)

                    dct2 = dict()
                    for n_task_id, p_task_ids in dct.items():
                        n_task_run_id = await create_task_run(task_id=n_task_id)
                        dct2[n_task_id] = n_task_run_id
                        for p_task_id in p_task_ids:
                            if p_task_id not in dct2:
                                if p_task_id == finished_task_id:
                                    p_task_run_id = await create_task_run(task_id=p_task_id, status_id=4)
                                else:
                                    p_task_run_id = await create_task_run(task_id=p_task_id)

                                dct2[p_task_id] = p_task_run_id

                    dct3 = dict()
                    for n_task_id, p_task_ids in dct.items():
                        dct3[dct2.get(n_task_id)] = [dct2.get(p_task_id) for p_task_id in p_task_ids]

        else:
            raise NotImplementedError

        print('qq')


def orchestration_process():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(orchestration())
    loop.close()
