import os

DB_TYPE = os.getenv('DB_TYPE', 'pg')
DB_HOST = os.getenv('DB_HOST', 's001cd-db-dev01.dev002.local')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_USER = os.getenv('DB_USER', 'a001_orchestration_tech_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'usrofa001_orchestration_tech_user')
DB_NAME = os.getenv('DB_NAME', 'orchestration')

KAFKA_HOST = os.getenv('KAFKA_SERVERS', '127.0.0.1:9092')
FINISHED_TASKS_TOPIC = os.getenv('FINISHED_TASKS_TOPIC', 'etl_finished_tasks')
AIRFLOW_TASKS_TOPIC = os.getenv('AIRFLOW_TASKS_TOPIC', 'airflow_tasks')
