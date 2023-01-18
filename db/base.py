import logging
from typing import AsyncGenerator

from sqlalchemy import MetaData
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, SCHEMA_NAME

SQLALCHEMY_DATABASE_URL = URL.create(
    drivername='postgresql+asyncpg',
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

METADATA = MetaData(schema=SCHEMA_NAME)

engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=True)


# expire_on_commit=False will prevent attributes from being expired
# after commit.
AsyncSessionFactory = sessionmaker(
    engine,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession
)


Base = declarative_base()

logger = logging.getLogger(__name__)


# Dependency
async def get_db() -> AsyncGenerator:
    async with AsyncSessionFactory() as session:
        logger.debug(f"ASYNC Pool: {engine.pool.status()}")
        yield session
