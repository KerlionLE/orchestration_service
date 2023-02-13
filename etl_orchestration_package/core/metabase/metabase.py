from asyncio import current_task

from typing import AsyncGenerator

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_scoped_session

from sqlalchemy.orm import defer
from sqlalchemy.dialects.postgresql import insert


class Metabase:
    def __init__(self, **db_config):
        self.driver_name = None
        self.username = db_config.get('username', 'test_user')
        self.password = db_config.get('password', 'test_user')
        self.host = db_config.get('host', '127.0.0.1')
        self.port = db_config.get('port', 5432)
        self.database = db_config.get('db_name', 'orchestration')

        self.sessions = dict()

    def get_sqlalchemy_db_url(self, **db_config):
        return URL.create(
            drivername=self.driver_name,
            username=db_config.get('user', self.username),
            password=db_config.get('password', self.password),
            host=db_config.get('host', self.host),
            port=db_config.get('port', self.port),
            database=db_config.get('db_name', self.database)
        )

    def get_engine(self, **db_config):
        return create_async_engine(
            self.get_sqlalchemy_db_url(**db_config),
            # echo=True
        )

    def get_session_factory(self, **db_config):
        return sessionmaker(
            self.get_engine(**db_config),
            autoflush=False,
            expire_on_commit=False,
            class_=AsyncSession
        )

    async def get_db(self, **db_config) -> AsyncGenerator:
        factory = self.get_session_factory(**db_config)
        yield async_scoped_session(factory, scopefunc=current_task)

    async def get_current_session(self):
        current_session = self.sessions.get('current')
        if current_session is None:
            current_session = await self.get_db().__anext__()
            self.sessions['current'] = current_session
        return current_session

    def insert(self, *args, **kwargs):
        pass


class PGMetabase(Metabase):
    def __init__(self, **db_config):
        super(PGMetabase, self).__init__(**db_config)

        self.driver_name = 'postgresql+asyncpg'

    def insert(self, model, data, exclude_fields=None):
        if exclude_fields is None:
            return insert(model).values(**data)
        return insert(model).values(**data).options(*[defer(col) for col in exclude_fields])
