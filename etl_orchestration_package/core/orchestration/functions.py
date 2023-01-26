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
def _get_model_by_id(model, _id):
    return select(model).where(model.id == _id)


async def get_model_by_id(model, _id):
    lst = await _get_model_by_id(model, _id)
    return lst[0]


@metabase_select_wrapper
def _get_status_id_by_value(status):
    return select(TaskRunStatus).where(TaskRunStatus.status == status)


async def get_status_id_by_value(status):
    lst = await _get_status_id_by_value(status)
    if lst:
        return lst[0].get('id')
    else:
        # logging.error("UNKNOWN STATUS!!!")
        raise Exception("UNKNOWN STATUS!!!")  # TODO: Убрать потом


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
def create_task_run(task_id, status_id=2, config=None, result=None):
    return insert(TaskRun).values(
        task_id=task_id, status_id=status_id,
        config=config or dict(),
        result=result or dict()
    )


@metabase_insert_wrapper
def create_graph_run_task_run(graph_run_id, task_run_id):
    return insert(GraphRunTaskRun).values(graph_run_id=graph_run_id, task_run_id=task_run_id)


@metabase_update_wrapper
def update_model_field_value(model, _id, field, value):
    return update(model).where(model.id == _id).values(**{field: value})


# --------------------------------------------------------------------------------------------------------------------

async def handle_newest_task(finished_task_id, task_run_result):
    graph_ids = await _get_all_graph_id_by_task_id(finished_task_id)

    graph_runs_dict = dict()
    for graph_id in graph_ids:
        # 3. Проверяем существует ли GraphRun с таким graph_id и статусом RUNNING
        running_graph_runs = await get_graph_run_by_graph_id(graph_id=graph_id, status_id=1)
        # Если существует
        # (это случай когда для запуска новой задачи нужно дождаться N задач
        # автоматически зарегистрированных в таблице TaskRun - [1, 2, ..., N] -> 3):
        finished_tasks = [{
            'task_id': finished_task_id,
            'status': 'SUCCEED',  # TODO: Убрать в константы или подумать еще...
            'result': task_run_result
        }]

        new_graph_run_flag = False
        for graph_run in running_graph_runs:
            graph_run_task_run_list = await get_graph_run_task_run_by_graph_run_id(graph_run_id=graph_run.get('id'))

            new_graph_run_flag = await _check_for_new_graph_run(graph_run_task_run_list, finished_tasks)
            if not new_graph_run_flag:
                graph_run_id, next_prev_task_runs_dict = await _get_and_update_running_graph_run(
                    graph_run_id=graph_run.get('id'),
                    finished_tasks=finished_tasks
                )
                graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

        if new_graph_run_flag:
            graph_run_id, next_prev_task_runs_dict = await _create_and_initialize_new_graph_run(
                graph_id=graph_id,
                finished_tasks=finished_tasks
            )
            graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

        # Если НЕ существует:
        if not graph_runs_dict:
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
    finished_tasks_data = {
        task_data.get('task_id'): {
            'result': task_data.get('result'),
            'status': task_data.get('status'),
        } for task_data in _finished_tasks
    }

    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        n_task_run_id = await create_task_run(task_id=n_task_id)
        dct2[n_task_id] = n_task_run_id
        for p_task_id in p_task_ids:
            if p_task_id not in dct2:
                result = dict()
                status = 2
                if p_task_id in finished_tasks_data:
                    result = finished_tasks_data[p_task_id].get('result')
                    status = await get_status_id_by_value(finished_tasks_data[p_task_id].get('status'))

                p_task_run_id = await create_task_run(
                    task_id=p_task_id,
                    status_id=status,
                    result=result
                )

                dct2[p_task_id] = p_task_run_id

    return dct2


async def _get_task_run_dict(graph_run_task_run_list):
    dct = dict()

    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        dct[tr.get('task_id')] = tr.get('id')

    return dct


async def _get_task_runs_from_graph_run(graph_run_task_run_list):
    for data in graph_run_task_run_list:
        task_run = await get_model_by_id(model=TaskRun, _id=data.get('task_run_id'))
        yield task_run


async def _update_task_runs(graph_run_task_run_list, finished_tasks=None):
    _finished_tasks = finished_tasks or list()
    finished_tasks_data = {
        task_data.get('task_id'): {
            'result': task_data.get('result'),
            'status': task_data.get('status'),
        } for task_data in _finished_tasks
    }
    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        if tr.get('task_id') in finished_tasks_data:
            result = finished_tasks_data[tr.get('task_id')].get('result')
            status = await get_status_id_by_value(
                finished_tasks_data[tr.get('task_id')].get('status')
            )

            _ = await update_model_field_value(
                model=TaskRun, _id=tr.get('id'),
                field='status_id', value=status
            )

            _ = await update_model_field_value(
                model=TaskRun, _id=tr.get('id'),
                field='result', value=result
            )


async def _check_for_new_graph_run(graph_run_task_run_list, finished_tasks):
    finished_task_ids = [task_data.get('task_id') for task_data in finished_tasks]
    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        if tr.get('task_id') in finished_task_ids:
            if tr.get('status_id') == 4:
                return True
    return False


def _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict):
    dct3 = dict()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        dct3[task_runs_dict.get(n_task_id)] = [task_runs_dict.get(p_task_id) for p_task_id in p_task_ids]

    return dct3


async def _create_graph_run_task_run(graph_run_id, task_run_ids):
    ids_list = list()
    for tr_id in task_run_ids:
        _id = await create_graph_run_task_run(
            graph_run_id=graph_run_id,
            task_run_id=tr_id
        )
        ids_list.append(_id)
    return ids_list


async def _create_and_initialize_new_graph_run(graph_id, status_id=1, finished_tasks=None):
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


async def _get_and_update_running_graph_run(graph_run_id, finished_tasks=None):
    _finished_tasks = finished_tasks or list()
    graph_run_task_run_list = await get_graph_run_task_run_by_graph_run_id(graph_run_id=graph_run_id)

    _ = await _update_task_runs(graph_run_task_run_list, _finished_tasks)
    task_runs_dict = await _get_task_run_dict(graph_run_task_run_list)

    # 3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
    graph_run = await get_model_by_id(GraphRun, graph_run_id)
    gc_list = await get_chains_by_graph_id(graph_id=graph_run.get('graph_id'))
    chains = await get_chains_by_ids([gc.get('chain_id') for gc in gc_list])

    # 3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = _create_next_prev_tasks_dict(chains)

    # 3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

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


async def orchestration():
    queue_interface = get_queue()
    await queue_interface.start()

    while True:
        # TODO: Прикрутить GracefulKill (выход из бесконечного цикла)
        # TODO: Прикрутить логи!!!!
        # TODO: Нужно коммитить консьюмера!!!

        print('RUNNING INFINITE LOOP')
        graph_runs_dict = dict()
        async for msg in queue_interface.consume_data('finished_tasks'):
            data = json.loads(msg.value)
            metadata = data.get('metadata')
            if not metadata:
                raise Exception("BLANK METADATA!!!")

            finished_task_id = metadata.get('task_id')
            if finished_task_id is not None:
                taskrun_result = data.get('result')
                graph_runs_dict.update(
                    await handle_newest_task(
                        finished_task_id=finished_task_id,
                        task_run_result=taskrun_result
                    )
                )
            else:
                graphrun_id = metadata.get('graphrun_id')
                finished_taskrun_id = metadata.get('taskrun_id')
                taskrun_result = data.get('result')
                taskrun_status = data.get('status')
                graph_runs_dict.update(
                    await handle_finished_task(
                        graph_run_id=graphrun_id,
                        finished_task_run_id=finished_taskrun_id,
                        task_run_result=taskrun_result,
                        task_run_status=taskrun_status
                    )
                )

        for graph_run_id, next_prev_task_runs_dict in graph_runs_dict.items():
            graph_run_failed_flag = False
            graph_run_finished_flag = True
            for next_task_run_id, prev_task_run_ids in next_prev_task_runs_dict.items():
                next_task_run = await get_model_by_id(model=TaskRun, _id=next_task_run_id)
                if next_task_run.get('status_id') in (4, 5):
                    if next_task_run.get('status_id') == 5:
                        graph_run_failed_flag = True
                    continue

                graph_run_finished_flag = False
                prev_task_runs_success_flag = True
                for prev_task_run_id in prev_task_run_ids:
                    task_run = await get_model_by_id(model=TaskRun, _id=prev_task_run_id)
                    if task_run.get('status_id') != 4:
                        if task_run.get('status_id') == 5:
                            _ = await update_model_field_value(
                                model=TaskRun,
                                _id=next_task_run_id,
                                field='status_id',
                                value=5
                            )
                        prev_task_runs_success_flag = False
                        break

                if prev_task_runs_success_flag:
                    service_name, message_for_service = await get_message_for_service(
                        graph_run_id, next_task_run_id, *prev_task_run_ids
                    )

                    await queue_interface.send_message(
                        producer_id='default',
                        message=message_for_service,
                        topic_name=service_name  # TODO: Пока service_name == topic_name
                    )

                    _ = await update_model_field_value(
                        model=TaskRun,
                        _id=next_task_run_id,
                        field='config',
                        value=message_for_service.get('config')
                    )

                    _ = await update_model_field_value(
                        model=TaskRun,
                        _id=next_task_run_id,
                        field='status_id',
                        value=1
                    )

            if graph_run_finished_flag:
                _ = await update_model_field_value(
                    model=GraphRun,
                    _id=graph_run_id,
                    field='status_id',
                    value=5 if graph_run_failed_flag else 4
                )

    await queue_interface.stop()


def orchestration_process(metabase_interface=None, queue_interface=None):
    if metabase_interface is not None:
        add_metabase('default', metabase_interface)

    if queue_interface is not None:
        add_queue('default', queue_interface)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(orchestration())
    loop.close()
