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

def create_new_graph(task_id):
    """
    НУЖНО ПОСТОРИТЬ СЛОВАРЬ ЗАВИСИМОСТЕЙ С ИСПОЛЬЗОВААНИЕМ TASKRUN_ID И С НИМ РАБОТАТЬ


    Строим граф тасков по task_id
    0. Создаем TaskRun с указанным task_id и статусом SUCCEED
    1. Получаем все Chain у которых previous_task = task_id
    2. Получаем Graph (один!) (из GraphChain) у которых chain = chain_id
    3. Проверяем существует ли GraphRun с таким graph_id и статусом RUNNING
        Если существует:
            3.1. Получаем из GraphRunTaskRun все task_run_id
            3.2. Строим словарь зависимостей (???)
        Если НЕ существует:
            3.1. Создаем GraphRun со статусом CREATED (c указанием полученного graph_id)
            3.2. Получаем все Chain (из GraphChain) у которых graph = graph_id
            3.3. Строим словарь зависимостей -> {next_task_id: [previous_task_id1, previous_task_id2, ...]}
            3.4. Создаем TaskRun'ы со статусом CREATED (по всем next_task_id у )
            3.5. Получаем все TaskRun'ы у которых
    4.
    """
    pass


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
