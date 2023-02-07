import json
import asyncio

from .functions import handle_newest_task, handle_finished_task

from ..metabase import get_metabase, add_metabase
from ..queue import get_queue, add_queue

from ..metabase.utils import read_model_by_id, update_model_field_value


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

    while True:
        # TODO: Прикрутить GracefulKill (выход из бесконечного цикла)
        # TODO: Прикрутить логи!!!!
        # TODO: Нужно коммитить консьюмера!!!

        print('RUNNING INFINITE LOOP')
        graph_runs_dict = dict()

        # 1. Слушаем топик сообщений от сервисов
        async for msg in queue_interface.consume_data('finished_tasks'):
            data = json.loads(msg.value)

            # 1.1 Получаем метаданные задачи (task)
            metadata = data.get('metadata')
            if not metadata:
                raise Exception("BLANK METADATA!!!")

            # 1.2 Пробуем получить 'task_id' из метаданных
            finished_task_id = metadata.get('task_id')
            if finished_task_id is not None:
                # 1.2.1 Если 'task_id' присутствует в метаданных это означает,
                #       что это задача новая и нам необходимо
                #       сгенерировать новые графы (GraphRun), и зарегистрировать
                #       задачу в метабазе
                graph_run_dict = await handle_newest_task(
                    finished_task_id=finished_task_id,
                    task_run_result=data.get('result')
                )

            else:
                # 1.2.2 Если 'task_id' отсутствует в метаданных это означает,
                #       что это задача была запущена с помощью сервиса оркестрации
                #       и все ее метаданные нужно только обновить
                graph_run_dict = await handle_finished_task(
                    graph_run_id=metadata.get('graphrun_id'),
                    finished_task_run_id=metadata.get('taskrun_id'),
                    task_run_result=data.get('result'),
                    task_run_status=data.get('status')
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

        for graph_run_id, next_prev_task_runs_dict in graph_runs_dict.items():
            graph_run_failed_flag = False
            graph_run_finished_flag = True
            for next_task_run_id, prev_task_run_ids in next_prev_task_runs_dict.items():
                next_task_run = await read_model_by_id(model=TaskRun, _id=next_task_run_id)
                if next_task_run.get('status_id') in (4, 5):
                    if next_task_run.get('status_id') == 5:
                        graph_run_failed_flag = True
                    continue

                graph_run_finished_flag = False
                prev_task_runs_success_flag = True
                for prev_task_run_id in prev_task_run_ids:
                    task_run = await read_model_by_id(model=TaskRun, _id=prev_task_run_id)
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
