from ..metabase.utils import read_models_by_filter, read_model_by_id, \
    read_models_by_ids_list, update_model_field_value


async def _get_all_graph_id_by_task_id(task_id):
    graph_ids = set()

    # 1. Получаем все Chain у которых previous_task = task_id
    chains = await read_models_by_filter(model=Chain, filter_dict={'previous_task_id': task_id})

    # 2. Получаем все Graph (из GraphChain) у которых chain = chain_id
    for chain in chains:
        graph_chains = await read_models_by_filter(model=GraphChain, filter_dict={'chain_id': chain.get('id')})
        for gc in graph_chains:
            graph_ids.add(gc.get('graph_id'))

    return graph_ids


async def _get_and_update_running_graph_run(graph_run_id, finished_tasks=None):
    _finished_tasks = finished_tasks or list()
    graph_run_task_run_list = await read_models_by_filter(
        model=GraphRunTaskRun,
        filter_dict={'graph_run_id': graph_run_id}
    )

    _ = await _update_task_runs(graph_run_task_run_list, _finished_tasks)
    task_runs_dict = await _get_task_run_dict(graph_run_task_run_list)

    # 3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
    graph_run = await read_model_by_id(GraphRun, graph_run_id)
    gc_list = await read_models_by_filter(
        model=GraphChain,
        filter_dict={'graph_id': graph_run.get('graph_id')}
    )
    chains = await read_models_by_ids_list(
        model=Chain,
        ids_list=[gc.get('chain_id') for gc in gc_list]
    )

    # 3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
    next_prev_tasks_dict = _create_next_prev_tasks_dict(chains)

    # 3.6. Из словаря зависимостей (п. 3.3) и созданных TaskRun строим новый словарь зависимостей
    # по task_run_id
    next_prev_task_runs_dict = _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict)

    return graph_run_id, next_prev_task_runs_dict


def _create_next_prev_tasks_dict(chains):
    dct = defaultdict(list)
    for c in chains:
        dct[c.get('next_task_id')].append(c.get('previous_task_id'))

    return dct


def _create_next_prev_task_runs_dict(next_prev_tasks_dict, task_runs_dict):
    dct3 = dict()
    for n_task_id, p_task_ids in next_prev_tasks_dict.items():
        dct3[task_runs_dict.get(n_task_id)] = [task_runs_dict.get(p_task_id) for p_task_id in p_task_ids]

    return dct3


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
            status = await read_models_by_filter(
                model=TaskRunStatus,
                filter_dict={'status': finished_tasks_data[tr.get('task_id')].get('status')}
            )

            _ = await update_model_field_value(model=TaskRun, _id=tr.get('id'), field='status_id', value=status[0])
            _ = await update_model_field_value(model=TaskRun, _id=tr.get('id'), field='result', value=result)


async def _get_task_run_dict(graph_run_task_run_list):
    return {
        tr.get('task_id'): tr.get('id')
        async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list)
    }


async def _check_for_new_graph_run(graph_run_task_run_list, finished_tasks):
    finished_task_ids = [task_data.get('task_id') for task_data in finished_tasks]
    async for tr in _get_task_runs_from_graph_run(graph_run_task_run_list):
        if tr.get('task_id') in finished_task_ids and tr.get('status_id') == 4:  # TODO: status_id?
            return True
    return False


async def _get_task_runs_from_graph_run(graph_run_task_run_list):
    for data in graph_run_task_run_list:
        yield await read_model_by_id(model=TaskRun, _id=data.get('task_run_id'))


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


async def _create_graph_run_task_run(graph_run_id, task_run_ids):
    ids_list = list()
    for tr_id in task_run_ids:
        _id = await create_graph_run_task_run(
            graph_run_id=graph_run_id,
            task_run_id=tr_id
        )
        ids_list.append(_id)
    return ids_list
