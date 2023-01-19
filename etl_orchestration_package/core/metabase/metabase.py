from typing import AsyncGenerator

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


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
            echo=True
        )

    def get_session_factory(self, session_id=0, **db_config):
        if session_id not in self.sessions:
            self.sessions[session_id] = sessionmaker(
                self.get_engine(**db_config),
                autoflush=False,
                expire_on_commit=False,
                class_=AsyncSession
            )
        return self.sessions.get(session_id)

    async def get_db(self, **db_config) -> AsyncGenerator:
        factory = self.get_session_factory(**db_config)
        async with factory() as session:
            yield session


class PGMetabase(Metabase):
    def __init__(self, **db_config):
        super(PGMetabase, self).__init__(**db_config)

        self.driver_name = 'postgresql+asyncpg'
