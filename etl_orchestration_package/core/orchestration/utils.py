from collections import defaultdict

from ..metabase import utils as db_tools
from ..metabase import models as db_models


async def get_task_run_dict(task_run_list):
    """
    Функция формирования словаря вида {task_run_id: task_id} из списка TaskRun объектов
    """
    return {
        tr.get('task_id'): tr.get('id') for tr in task_run_list
    }


async def check_for_new_graph_run(task_run_list, finished_task_ids):
    """
    Функция проверки необходимости создания нового GraphRun:
        - если хотя бы один TaskRun из переданного списка task_run_list присутствует
          в списке finished_task_ids и статус этого TaskRun в МетаБД == 'SUCCEED'
          необходимо создавать новый GraphRun
        - в противном случае GraphRun создаваться не должен

    """
    for tr in task_run_list:
        if tr.get('task_id') in finished_task_ids:
            if tr.get('status_id') == db_tools.get_status_id('SUCCEED'):  # TODO: Убрать в константы
                return True
    return False


def create_next_prev_tasks_dict(chains):
    """
    Функция формирования словаря вида
    {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    из списка Chain объектов
    """
    dct = defaultdict(list)
    _ = [dct[c.get('next_task_id')].append(c.get('previous_task_id')) for c in chains]
    return dct


def create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict):
    """
    Функция преобразования словаря вида
    {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    в словарь
    {next_task_run_id: [previous_task_run_id1, previous_task_run_id2, ...]}
    """
    dct = dict()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        dct[task_runs_dict.get(n_task_id)] = [task_runs_dict.get(p_task_id) for p_task_id in p_task_ids]

    return dct


async def create_task_runs(next_prev_tasks_dict):
    """
    Функция создания новых TaskRun.
    На вход получает словарь вида
    {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    """

    dct2 = dict()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        dct2[n_task_id] = {
            'task_id': n_task_id,
            'status_id': db_tools.get_status_id("CREATED"),  # TODO: убрать в константы
            'config': dict(),
            'result': dict()
        }
        for p_task_id in p_task_ids:
            dct2[p_task_id] = {
                'task_id': p_task_id,
                'status_id': db_tools.get_status_id("CREATED"),  # TODO: убрать в константы
                'config': dict(),
                'result': dict()
            }

    for task_id, task_run_data in dct2.items():
        dct2[task_id] = await db_tools.create_model(
            model=db_models.TaskRun,
            data=task_run_data
        )

    return dct2


async def update_task_run(task_run_id, status=None, result=None, config=None):
    data = {}

    if status is not None:
        status_id = await db_tools.read_models_by_filter(
            model=db_models.TaskRunStatus,
            filter_dict={
                'status': status
            }
        )
        data.update({'status_id': status_id[0].get('id')})

    if result is not None:
        data.update({'result': result})

    if config is not None:
        data.update({'config': config})

    if data:
        _ = await db_tools.update_model_by_id(
            model=db_models.TaskRun,
            _id=task_run_id,
            data=data
        )


async def update_task_runs(task_run_dict, tasks_data):
    """
    Функция обновления объектов TaskRun в соответствии с переданным списком параметров
    """

    for task_id, task_run_id in task_run_dict.items():
        if task_id in tasks_data:
            await update_task_run(
                task_run_id,
                status=tasks_data.get(task_id, dict()).get('status'),
                result=tasks_data.get(task_id, dict()).get('result')
            )
