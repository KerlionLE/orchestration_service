import os

# DB_CONFIG
# ----------------------------------------------------------------------------

DB_TYPE = os.getenv('DB_TYPE', 'pg')
DB_HOST = os.getenv('DB_HOST', 's001cd-db-dev01.dev002.local')
DB_PORT = os.getenv('DB_PORT', 5432)
DB_USER = os.getenv('DB_USER', 'a001_orchestration_tech_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'usrofa001_orchestration_tech_user')
DB_NAME = os.getenv('DB_NAME', 'orchestration')

# QUEUE_CONFIG
# ----------------------------------------------------------------------------

QUEUE_TYPE = os.getenv('QUEUE_TYPE', 'kafka')
QUEUE_SERVERS = os.getenv(
    'QUEUE_SERVERS',
    's001cd-ad-kfk01.dev002.local:9092,s001cd-ad-kfk01.dev002.local:9092,s001cd-ad-kfk01.dev002.local:9092'
)
FINISHED_TASKS_TOPIC = os.getenv('FINISHED_TASKS_TOPIC', 'ukd_etl_tasks')
QUEUE_GROUP = os.getenv('QUEUE_GROUP', 'orchestration_service_group')

AUTO_OFFSET_RESET = os.getenv('AUTO_OFFSET_RESET', 'earliest')

# ORCHESTRATION_CONFIG
# ----------------------------------------------------------------------------

DRY_RUN = os.getenv('DRY_RUN', True)
