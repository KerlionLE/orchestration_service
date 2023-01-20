1. Kafka
	- Создать топик для завершенных задач (etl_finished_tasks)
	- Создать топик для задач отправляемых в Airflow (airflow_tasks)
2. PG
	- Создать базу в DEV PG
		- dbname: orchestration
		- user: a001_orchestration_tech_user
		- pass: usrofa001_orchestration_tech_user
3. OrchestrationService
	- Создать модели:
		- Service (Сервисы которые выполняют таски - airflow/nifi/...):
			- id (int/sequence)
			- name (varchar) - PK
		- Process (Процесы которые выполняют таски - <dag_name>/<pg_id>/...):
			- id (int/sequence)
			- service (int) - FK на Service
			- uid (varchar) 
		- Task (Задачи которые будут выполняться):
			- id (int/sequence)
			- process (int) - FK на Process
			- config_template (varchar)
		- TaskRun (Конкретный запуск какого-то таска):
			- id (int/sequence)
			- task (int) - FK на Task
			- status (varchar/int если нормализуем)
			- config (varchar)
			- result (varchar)
			- created_at (timestamp)
			- updatet_at (timestamp)
		- Chain (Звено цепочки задач):
			- id (int/sequence)
			- previous_task (int) - FK на Task
			- next_task (int) - FK на Task нужно будет добавить проверку что previous и next не могут быть одинаковыми
			- что если зависим от нескольких previous_task?
		- Graph
			- id
			- created_dt
			- updated_dt
		- Graph_Chain
			- id (PK)
			- chain (PK)

	- Реализция бизнес-логики приложения:
		- Функциональность получения сообщений из источника
			- Kafka
		- Функциональность обновления статуса запущенного таска
		- Функциональность формировния задач на выполнение
		- Функциональность отправки задач на выполнение
			- Kafka
	
	- Реализация интерфейса взаимодействия с сервисом:
		- REST API (CRUD всех моделей)
		- endpoints на запуск и останов полного процесса работы сервиса (получение->обработка->формирование->отправка)
		- endpoint на ручной запуск отдельной цепочки

4. OrchestrationServiceClient
	- Реализация методов взимдействия с сервисом оркестрации:
		- REST API (CRUD всех моделей)
		- endpoints на запуск и останов полного процесса работы сервиса (получение->обработка->формирование->отправка)
		- endpoint на ручной запуск отдельной цепочки

5. NiFi
	- Создать поток для загрузки данных в STG (синтетика). Последний процесс потока должен отправлять в очередь сообщение о завершении задачи.

6. Airflow
	- Создать даг который будет слушать топики kafka и тригерить другие даги в соответсвии с теми сообщениеми который получил
	- Создать даг для переклди данных с STG на ODS


---




6 -> 7 -> [8, 9]



1 -> 2
[1, 5] -> 3
1 -> 4

chain
_id prv nxt
  1   1   2
  2   1   3
  3   1   4
  4   5   3
  5   6   7
  6   7   8
  7   7   9

graph (integration)
_id name
  1  ...

graph_chain
graph chain
    1     1
    1     2
    1     3
    1     4
    2     1 (ошибка - нужна проверка)

graph_run
_id graph_id created_dt updated_dt status
  1       ..        ...        ...    ... Нужен демон, который будет ставить статусы FAILED для незавершившихся run

GraphRunTaskRun
graph_run_id task_run_id
           1           1
           1           2



4 -> 6
