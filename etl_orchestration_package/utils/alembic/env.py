import asyncio
import os
import sys

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

parent_dir = os.path.abspath(os.path.join(os.getcwd()))
sys.path.append(parent_dir)

from core.metabase import create_metabase, get_metabase
from core.metabase.models import Base

create_metabase(
    metabase_id='default',
    metabase_type=os.getenv('DB_TYPE', 'pg'),
    host=os.getenv('DB_HOST', 's001cd-db-dev01.dev002.local'),
    port=os.getenv('DB_PORT', 5432),
    username=os.getenv('DB_USER', 'a001_orchestration_tech_user'),
    password=os.getenv('DB_PASSWORD', 'usrofa001_orchestration_tech_user'),
    database=os.getenv('DB_NAME', 'orchestration')
)



target_metadata = Base.metadata


def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        version_table_schema=target_metadata.schema
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_async_engine(get_metabase().get_sqlalchemy_db_url())

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)


asyncio.run(run_migrations_online())
