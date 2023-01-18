import os

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_USER = os.getenv('DB_USER', 'test_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'test_user')
DB_NAME = os.getenv('DB_NAME', 'orchestration')
SCHEMA_NAME = 'ukd_orchestration_service'

KAFKA_HOST = os.getenv('KAFKA_SERVERS', '127.0.0.1:9092')
FINISHED_TASKS_TOPIC = os.getenv('FINISHED_TASKS_TOPIC', 'etl_finished_tasks')
AIRFLOW_TASKS_TOPIC = os.getenv('AIRFLOW_TASKS_TOPIC', 'airflow_tasks')
