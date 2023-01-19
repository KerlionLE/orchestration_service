import os

KAFKA_HOST = os.getenv('KAFKA_SERVERS', '127.0.0.1:9092')
FINISHED_TASKS_TOPIC = os.getenv('FINISHED_TASKS_TOPIC', 'etl_finished_tasks')
AIRFLOW_TASKS_TOPIC = os.getenv('AIRFLOW_TASKS_TOPIC', 'airflow_tasks')
