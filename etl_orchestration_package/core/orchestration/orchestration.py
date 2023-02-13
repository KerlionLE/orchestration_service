import json
import asyncio

from loguru import logger

from .functions import handle_newest_task, handle_finished_task, get_message_for_service

from ..metabase import get_metabase, add_metabase
from ..queue import get_queue, add_queue

from ..metabase import utils as db_tools
from ..metabase import models as db_models

from etl_orchestration_package.utils.killer import GracefulKiller


def orchestration_process(metabase_interface=None, queue_interface=None, **config):
    if metabase_interface is not None:
        add_metabase('default', metabase_interface)

    if queue_interface is not None:
        add_queue('default', queue_interface)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(orchestration(**config))
    loop.close()


async def orchestration(**config):

    killer = GracefulKiller()

    queue_interface = get_queue()
    await queue_interface.start()

    logger.info('RUNNING INFINITE LOOP')
    while True:
        if config.get('dry_run', False):
            logger.warning("SERVICE WORKING IN DRY RUN MODE!!!")

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
                    task_run_status=data.get('status', 'SUCCEED')  # TODO: Костыль для тестирования
                )

            else:
                # 1.2.2 Если 'task_run_id' отсутствует в метаданных это означает,
                #       что это задача новая и нам необходимо
                #       сгенерировать новые графы (GraphRun), и зарегистрировать
                #       задачу в метабазе
                graph_run_dict = await handle_newest_task(
                    task_id=metadata.get('task_id'),
                    task_run_result=data.get('result'),
                    task_run_status=data.get('status', 'SUCCEED')  # TODO: Костыль для тестирования
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
            succeed_status_id = await db_tools.get_status_id("SUCCEED")
            failed_status_id = await db_tools.get_status_id("FAILED")
            running_status_id = await db_tools.get_status_id('RUNNING')

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
                if next_task_run.get('status_id') in (succeed_status_id, failed_status_id):
                    if next_task_run.get('status_id') == failed_status_id:
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
                    if task_run.get('status_id') != succeed_status_id:
                        if task_run.get('status_id') == failed_status_id:
                            await db_tools.update_model_by_id(
                                model=db_models.TaskRun,
                                _id=next_task_run_id,
                                data={'status_id': failed_status_id}
                            )

                        # Если хотя бы один из предыдущих по графу TaskRun'ов не находится в
                        # статусе SUCCEED, то новое сообщение формироваться не должно
                        prev_task_runs_success_flag = False
                        break

                # 2.2 Если все предыдущие по графу TaskRun'ы находятся в статусе SUCCEED
                # необходимо формировать и отправлять сообщение для следующего TaskRun
                if prev_task_runs_success_flag:
                    # 2.2.1 Формируем сообщение
                    logger.info(f"CREATE NEW MESSAGE FOR TASK QUEUE...")
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
                    logger.info(f"UPDATE TASKRUN (task_run_id={next_task_run_id}) STATUS TO 'RUNNING'...")
                    await db_tools.update_model_by_id(
                        model=db_models.TaskRun,
                        _id=next_task_run_id,
                        data={
                            'config': message_for_service.get('config'),
                            'status_id': running_status_id
                        }
                    )

            # Обновляем статус GraphRun, если это необходимо
            if graph_run_finished_flag:
                status = failed_status_id if graph_run_failed_flag else succeed_status_id
                logger.info(f"UPDATE GRAPHRUN (graph_run_id={graph_run_id}) STATUS...")
                await db_tools.update_model_by_id(
                    model=db_models.GraphRun,
                    _id=graph_run_id,
                    data={
                        'status_id': status
                    }
                )

        if not config.get('dry_run', False) and graph_runs_dict:
            # Commit полученных из очереди сообщений
            await queue_interface.commit(consumer_id='default')

            # Commit всех изменений в МетаБД
            await db_tools.commit()

        graph_runs_dict.clear()

        if killer.kill_now:
            break

    await queue_interface.stop()

    logger.info(f"STOP!")
