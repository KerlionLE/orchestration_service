from loguru import logger

from . import utils as orc_tools

from ..metabase import utils as db_tools
from ..metabase import models as db_models


async def handle_newest_task(task_id, task_run_result, task_run_status):
    """
    Функция генерации нового GraphRun по task_id
    """

    logger.info(f"START HANDLE NEWEST TASK WITH ID: {task_id}")

    finished_tasks = {
        task_id: {
            'status': task_run_status,
            'result': task_run_result
        }
    }

    # 1. Получаем список всех graph_id, в которых фигурирует завершенный task
    graph_ids = await get_all_graph_id_by_task_id(task_id)

    if not graph_ids:
        logger.warning(f"UNKNOWN TASK! (task_id={task_id})")

    graph_runs_dict = dict()
    for graph_id in graph_ids:
        # 2. Проверяем существует ли GraphRun'ы с таким graph_id и статусом RUNNING
        running_graph_runs = await db_tools.read_models_by_filter(
            model=db_models.GraphRun,
            filter_dict={
                'graph_id': graph_id,
                'status_id': db_tools.get_status_id('RUNNING')  # TODO: убрать в константы
            }
        )

        create_graph_run_flag = True
        for graph_run in running_graph_runs:
            # 2.1 Если существуют
            # (это случай когда для запуска новой задачи (TaskRun) нужно дождаться N задач
            # автоматически зарегистрированных в таблице TaskRun - [1, 2, ..., N] -> 3):

            # 2.1.1 Получаем список всех TaskRun в найденном GraphRun
            graph_run_task_run_list = await db_tools.read_models_by_filter(
                model=db_models.GraphRunTaskRun,
                filter_dict={'graph_run_id': graph_run.get('id')}
            )

            task_run_list = await db_tools.read_models_by_ids_list(
                model=db_models.TaskRun,
                ids_list=[data.get('task_run_id') for data in graph_run_task_run_list]
            )

            # 2.1.2 Проверяем не завершен ли полученный TaskRun в найденном GraphRun
            new_graph_run_flag = await orc_tools.check_for_new_graph_run(
                task_run_list,
                finished_task_ids=[_task_id for _task_id in finished_tasks]
            )

            if not new_graph_run_flag:
                # 2.1.2.1 Если НЕ завершен, то
                logger.info(f"FOUND RUNNING GRAPHRUN (graph_run_id={graph_run.get('id')}) "
                            f"WITH HANDLED TASK (task_id={task_id}). "
                            f"UPDATE TASKRUN STATUS...")

                # 2.1.2.2 Формируем словарь вида {task_run_id: task_id}
                task_runs_dict = await orc_tools.get_task_run_dict(task_run_list)

                # 2.1.2.3 Обновляем статус найденного task_run
                _ = await orc_tools.update_task_runs(
                    task_run_dict=task_runs_dict,
                    tasks_data=finished_tasks
                )

                # 2.1.2.4 Получаем словарь связей следующих и предыдущих TaskRun
                next_prev_task_runs_dict = await get_graph(
                    graph_run_id=graph_run.get('id'), task_runs_dict=task_runs_dict
                )

                graph_runs_dict.update({graph_run.get('id'): next_prev_task_runs_dict})

                # Если хотя бы один из GraphRun нуждался в этом Task для продолжения работы
                # новый GraphRun создавать не нужно
                create_graph_run_flag = False

        if create_graph_run_flag:
            # 2.2 Если таких НЕ существует или все GraphRun завершены, то
            logger.info(f"CAN\'T FIND ANY RUNNING GRAPHRUNS WITH QUEUED HANDLED TASK (task_id={task_id}). "
                        f"CREATE NEW GRAPHRUN...")

            # 2.2.1 Создаем новый GraphRun
            graph_run_id, next_prev_task_runs_dict = await create_new_graph(graph_id)
            graph_runs_dict.update({graph_run_id: next_prev_task_runs_dict})

            # 2.2.2 Получаем список всех только что созданных TaskRun
            graph_run_task_run_list = await db_tools.read_models_by_filter(
                model=db_models.GraphRunTaskRun,
                filter_dict={'graph_run_id': graph_run_id}
            )

            task_run_list = await db_tools.read_models_by_ids_list(
                model=db_models.TaskRun,
                ids_list=[data.get('task_run_id') for data in graph_run_task_run_list]
            )

            # 2.2.3 Формируем словарь вида {task_run_id: task_id}
            task_runs_dict = await orc_tools.get_task_run_dict(task_run_list)

            # 2.2.4 Обновляем статус завешенного task_run
            _ = await orc_tools.update_task_runs(
                task_run_dict=task_runs_dict,
                tasks_data=finished_tasks
            )

            logger.info(f"CREATED NEW GRAPHRUN (graph_run_id={graph_run_id}).")

    return graph_runs_dict


async def handle_finished_task(graph_run_id, finished_task_run_id, task_run_result, task_run_status):
    """
    Функция обновления состояний TaskRun существующего GraphRun
    """

    # 1. Обновляем статус найденного task_run
    _ = await orc_tools.update_task_run(
        task_run_id=finished_task_run_id,
        status=task_run_status,
        result=task_run_result
    )

    # ---------------------------------------------------------------

    # 2. Получаем список всех TaskRun в найденном GraphRun
    graph_run_task_run_list = await db_tools.read_models_by_filter(
        model=db_models.GraphRunTaskRun,
        filter_dict={'graph_run_id': graph_run_id}
    )
    task_run_list = await db_tools.read_models_by_ids_list(
        model=db_models.TaskRun,
        ids_list=[data.get('task_run_id') for data in graph_run_task_run_list]
    )

    # 3. Формируем словарь вида {task_run_id: task_id}
    task_runs_dict = await orc_tools.get_task_run_dict(task_run_list)

    # 4. Получаем словарь связей следующих и предыдущих TaskRun
    next_prev_task_runs_dict = await get_graph(
        graph_run_id=graph_run_id, task_runs_dict=task_runs_dict
    )

    return {graph_run_id: next_prev_task_runs_dict}


# --------------------------------------------------------------------------------------------------------------------


async def get_all_graph_id_by_task_id(task_id):
    """
    Функция получения всех graph_id определенных Graph, в которых фигурирует конкретный task_id
    """
    graph_ids = set()

    # 1. Получаем все Chain у которых previous_task = task_id
    chains = await db_tools.read_models_by_filter(model=db_models.Chain, filter_dict={'previous_task_id': task_id})

    # 2. Получаем все Graph (из GraphChain) у которых chain = chain_id
    for chain in chains:
        graph_chains = await db_tools.read_models_by_filter(
            model=db_models.GraphChain,
            filter_dict={'chain_id': chain.get('id')}
        )
        for gc in graph_chains:
            graph_ids.add(gc.get('graph_id'))

    return graph_ids


# --------------------------------------------------------------------------------------------------------------------


async def create_new_graph(graph_id):
    """
    Функция создания нового GraphRun.
    Функция возвращает id нового GraphRun и словарь
    {next_task_run_id: [previous_task_run_id1, previous_task_run_id2, ...]}
    """
    # 1. Создаем новый GraphRun
    graph_run_id = await db_tools.create_model(
        model=db_models.GraphRun, data={
            'graph_id': graph_id,
            'status_id': db_tools.get_status_id('RUNNING'),  # TODO: убрать в константы
            'config': dict(),
            'result': dict()
        }
    )
    # 2. Получаем словарь связей следующих и предыдущих TaskRun нового GraphRun
    next_prev_task_runs_dict = await get_graph(graph_run_id=graph_run_id)

    # 3. Создаем GraphRunTaskRun записи с использованием сгенерированного словаря
    ids_set = set()
    for _n_task_run_id, _p_task_run_ids in next_prev_task_runs_dict.items():
        ids_set.add(_n_task_run_id)
        for _p_task_run_id in _p_task_run_ids:
            ids_set.add(_p_task_run_id)

    for _id in ids_set:
        _ = await db_tools.create_model(
            model=db_models.GraphRunTaskRun,
            data={
                'graph_run_id': graph_run_id,
                'task_run_id': _id
            }
        )

    return graph_run_id, next_prev_task_runs_dict


# --------------------------------------------------------------------------------------------------------------------

async def get_graph(graph_run=None, graph_run_id=None, task_runs_dict=None):
    """
    Функция получения словаря зависимостей
    {next_task_run_id: [previous_task_run_id1, previous_task_run_id2, ...]}
    """

    # 0. Получаем объект GraphRun из МетаБД, если он не был передан
    if graph_run is None:
        if graph_run_id is None:
            raise Exception("'graph_run is None' AND 'graph_run_id is None'")

        graph_run = await db_tools.read_model_by_id(
            model=db_models.GraphRun, _id=graph_run_id
        )

    # 1. Получаем все Chain (из GraphChain) у которых graph = graph_id
    gc_list = await db_tools.read_models_by_filter(
        model=db_models.GraphChain, filter_dict={'graph_id': graph_run.get('graph_id')}
    )
    chains = await db_tools.read_models_by_ids_list(
        model=db_models.Chain, ids_list=[gc.get('chain_id') for gc in gc_list]
    )

    # 2. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = orc_tools.create_next_prev_tasks_dict(chains)

    # 3. Получаем словарь -> {task_id: TaskRun}, если он не был передан ранее
    if task_runs_dict is None:
        task_runs_dict = await orc_tools.create_task_runs(next_prev_tasks_dict)

    # 3. Из словаря зависимостей (п. 2) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = orc_tools.create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

    return next_prev_task_runs_dict


# --------------------------------------------------------------------------------------------------------------------

async def get_message_for_service(graph_run_id, next_task_run_id, *previous_task_run_ids):
    """
    Функция подготовки сообщения для отправки в очередь задач конкретного сервиса.
    """

    # 1. По входным данным собираем необходимые параметры для сообщения из МетаБД
    graph_run = await db_tools.read_model_by_id(model=db_models.GraphRun, _id=graph_run_id)
    graph = await db_tools.read_model_by_id(model=db_models.Graph, _id=graph_run.get('graph_id'))

    next_task_run = await db_tools.read_model_by_id(model=db_models.TaskRun, _id=next_task_run_id)
    next_task = await db_tools.read_model_by_id(model=db_models.Task, _id=next_task_run.get('task_id'))

    process = await db_tools.read_model_by_id(model=db_models.Process, _id=next_task.get('process_id'))
    service = await db_tools.read_model_by_id(model=db_models.Service, _id=process.get('service_id'))

    # 2. Формируем конфиг следующего TaskRun.
    # Заполняем config_template данными из результатов предыдущих TaskRun этого GraphRun
    next_task_config_template = next_task.get('config_template')
    next_task_run_config = next_task_run.get('config', dict())
    for previous_task_run_id in previous_task_run_ids:
        previous_task_run = await db_tools.read_model_by_id(model=db_models.TaskRun, _id=previous_task_run_id)

        previous_task_run_result = previous_task_run.get('result', dict())
        for key, default_value in next_task_config_template.items():
            next_task_run_config[key] = previous_task_run_result.get(key, default_value)

    return service.get('topic_name'), {
        "metadata": {
            "graphrun_id": graph_run_id,
            "taskrun_id": next_task_run_id,
            "process_uid": process.get('uid'),
            "service_name": service.get('name'),
            "graph_name": graph.get('name')
        },
        "config": next_task_run_config,
        "result": dict(),
        "status": "RUNNING"
    }
