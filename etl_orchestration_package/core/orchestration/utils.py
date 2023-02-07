from ..metabase.utils import read_models_by_filter


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
