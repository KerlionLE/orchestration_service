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

def create_graph():
    """
    Строим граф тасков по task_id
    0. Получаем какой-то task_id
    1. Получаем все Chain у которых previous_task = task_id
    2. Получаем все Graph (из GraphChain) у которых chain = chain_id
    3. Получаем все Chain (из GraphChain) у которых graph = graph_id
    4. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}

    """
    pass

# ---------------------------------------------------------------------

# from ..metabase import get_metabase
from ..queue import get_queue

async def orchestration():
    # db = get_metabase()

    queue_interface = get_queue()

    etl_tasks_consumer = queue_interface.get_consumer(consumer_id='etl_tasks_consumer')

    # 1. Получаем сообщение из "Завершенные задачи"
    # 2. Сохраняем/обновляем задачу (TaskRun) у себя метабазе
    #   2.1 Важным этапом тут является изменение статуса TaskRun на SUCCEED/FAILED
    # 3. Формируем задачи (!) на выполнение
    #   3.1 Выбираем все последние SUCCEED TaskRun
    #   3.2

    async for task_msg in etl_tasks_consumer.consume_data():
        task_run = get_or_create_task_run(**task_msg)
