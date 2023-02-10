import json
import asyncio

from loguru import logger

from .functions import handle_newest_task, handle_finished_task, get_message_for_service

from ..metabase import get_metabase, add_metabase
from ..queue import get_queue, add_queue

from ..metabase import utils as db_tools
from ..metabase import models as db_models


def orchestration_process(metabase_interface=None, queue_interface=None):
    if metabase_interface is not None:
        add_metabase('default', metabase_interface)

    if queue_interface is not None:
        add_queue('default', queue_interface)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(orchestration())
    loop.close()


async def orchestration():
    queue_interface = get_queue()
    await queue_interface.start()

    logger.info('RUNNING INFINITE LOOP')
    while True:
        # TODO: Маппинг статуса и status_id
        # TODO: Прикрутить GracefulKill (выход из бесконечного цикла)
        # TODO: Прикрутить логи!!!!
        # TODO: Нужно коммитить консьюмера!!!

        graph_runs_dict = dict()
        # 1. Слушаем топик сообщений от сервисов
        async for msg in queue_interface.consume_data('default'):
            data = json.loads(msg.value)

            logger.debug(f'MESSAGE: {data}')

            # 1.1 Получаем метаданные задачи (task)
            metadata = data.get('metadata')
            if not metadata:
                logger.error(f"CAN\'T FIND METADATA IN MESSAGE: {data}")
                continue

            # 1.2 Пробуем получить 'task_run_id' из метаданных
            finished_task_run_id = metadata.get('taskrun_id')
            if finished_task_run_id is not None:
                # 1.2.1 Если 'task_run_id' присутствует в метаданных это означает,
                #       что это задача была запущена с помощью сервиса оркестрации
                #       и все ее метаданные нужно только обновить
                graph_run_dict = await handle_finished_task(
                    graph_run_id=metadata.get('graphrun_id'),
                    finished_task_run_id=finished_task_run_id,
                    task_run_result=data.get('result'),
                    task_run_status=data.get('status')
                )

            else:
                # 1.2.2 Если 'task_run_id' отсутствует в метаданных это означает,
                #       что это задача новая и нам необходимо
                #       сгенерировать новые графы (GraphRun), и зарегистрировать
                #       задачу в метабазе
                graph_run_dict = await handle_newest_task(
                    task_id=metadata.get('task_id'),
                    task_run_result=data.get('result')
                )

            # 1.3 Результатом шага 1.2 является словарь вида
            # {
            #     graph_run_id1: {
            #         next_task_run_id1: [
            #             prev_task_run_id1,
            #             prev_task_run_id2,
            #             ...
            #         ],
            #         next_task_run_id2: [
            #             prev_task_run_id3,
            #             prev_task_run_id4,
            #             ...
            #         ],
            #     }
            # }
            # Так как сообщений из очереди может приехать сразу несколько
            # мы формируем один большой словарь таких графов с целью их
            # последовательного обновления и формирования новых задач в рамках этих графов
            graph_runs_dict.update(graph_run_dict)

        # 2. По сформированным DAG (?) формируем и отправляем сообщения в соответствующие топики
        for graph_run_id, next_prev_task_runs_dict in graph_runs_dict.items():

            graph_run_failed_flag = False
            graph_run_finished_flag = True

            for next_task_run_id, prev_task_run_ids in next_prev_task_runs_dict.items():
                # 2.1 Определяем состояние GraphRun
                next_task_run = await db_tools.read_model_by_id(
                    model=db_models.TaskRun,
                    _id=next_task_run_id
                )

                # Если следующий по графу TaskRun уже SUCCEED или FAILED, то
                # пометь сам GraphRun как FAILED, если TaskRun FAILED и переходи к следующей
                # части графа
                if next_task_run.get('status_id') in (4, 5):
                    if next_task_run.get('status_id') == 5:
                        graph_run_failed_flag = True
                    continue

                # Если хотя бы один из следующих по графу Taskrun'ов не оказался в
                # одном из статусов SUCCEED или FAILED, то GraphRun еще не считается
                # завершенным
                graph_run_finished_flag = False

                prev_task_runs_success_flag = True

                # Если один из предыдущих по графу TaskRun'ов оказался в статусе FAILED,
                # то пометь следующий по графу TaskRun тоже как FAILED
                for prev_task_run_id in prev_task_run_ids:
                    task_run = await db_tools.read_model_by_id(model=db_models.TaskRun, _id=prev_task_run_id)
                    if task_run.get('status_id') != 4:
                        if task_run.get('status_id') == 5:
                            _ = await db_tools.update_model_field_value(
                                model=db_models.TaskRun,
                                _id=next_task_run_id,
                                field='status_id',
                                value=5
                            )
                        # Если хотя бы один из предыдущих по графу TaskRun'ов не находится в
                        # статусе SUCCEED, то новое сообщение формироваться не должно
                        prev_task_runs_success_flag = False
                        break

                # 2.2 Если все предыдущие по графу TaskRun'ы находятся в статусе SUCCEED
                # необходимо формировать и отправлять сообщение для следующего TaskRun
                if prev_task_runs_success_flag:
                    # 2.2.1 Формируем сообщение
                    topic_name, message_for_service = await get_message_for_service(
                        graph_run_id, next_task_run_id, *prev_task_run_ids
                    )

                    # 2.2.2 Отправляем сообщение
                    await queue_interface.send_message(
                        producer_id='default',
                        message=message_for_service,
                        topic_name=topic_name
                    )

                    # 2.2.3 Обновляем значение параметров следующего TaskRun
                    _ = await db_tools.update_model_field_value(
                        model=db_models.TaskRun,
                        _id=next_task_run_id,
                        field='config',
                        value=message_for_service.get('config')
                    )

                    _ = await db_tools.update_model_field_value(
                        model=db_models.TaskRun,
                        _id=next_task_run_id,
                        field='status_id',
                        value=1  # TODO: status_id?
                    )

            # Обновляем статус GraphRun, если это необходимо
            if graph_run_finished_flag:
                _ = await db_tools.update_model_field_value(
                    model=db_models.GraphRun,
                    _id=graph_run_id,
                    field='status_id',
                    value=5 if graph_run_failed_flag else 4
                )



    await queue_interface.stop()
