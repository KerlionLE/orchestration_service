from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import BIGINT, TIMESTAMP, VARCHAR, SMALLINT, DATE, JSONB
from sqlalchemy.orm import relationship, validates

from settings import SCHEMA_NAME
from .base import Base


class SchemaBase:
    """
    Общий класс для определения схемы
    """
    __table_args__ = {'schema': SCHEMA_NAME}


class Service(Base, SchemaBase):
    __tablename__ = 'service'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    name = Column(VARCHAR(64), unique=True, nullable=False)

    processes = relationship('Process', back_populates='service')


class Process(Base, SchemaBase):
    __tablename__ = 'process'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    service_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.service.id'))
    uid = Column(VARCHAR(128), unique=True, nullable=False)

    service = relationship('Service', back_populates='processes')
    tasks = relationship('Task', back_populates='process')


class Task(Base, SchemaBase):
    __tablename__ = 'task'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    process_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.process.id'))
    config_template = Column(JSONB, nullable=False)

    process = relationship('Process', back_populates='tasks')
    task_runs = relationship('TaskRun', back_populates='task')


class TaskRunStatus(Base, SchemaBase):
    __tablename__ = 'task_run_status'

    id = Column(SMALLINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    status = Column(VARCHAR(64), nullable=True, unique=True)

    task_run_status = relationship('TaskRun', back_populates='status')


class TaskRun(Base, SchemaBase):
    __tablename__ = 'task_run'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    task_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.task.id'))
    status_id = Column(SMALLINT, ForeignKey(f'{SCHEMA_NAME}.task_run_status.id'))
    config = Column(JSONB, nullable=False)
    result = Column(VARCHAR(256), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.now)
    updated_at = Column(TIMESTAMP, nullable=False, default=datetime.now, onupdate=datetime.now)

    task = relationship('Task', back_populates='task_run')
    status = relationship('TaskRunStatus', back_populates='task_run_status')


class Chain(Base, SchemaBase):
    __tablename__ = 'chain'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    previous_task_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.task.id'))
    next_task_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.task.id'))

    previous_task = relationship('Task')
    next_task = relationship('Task')

    @validates('previous_task', 'next_task')
    def validate(self, key, value):
        if key == 'next_task':
            assert self.previous_task != value
        return value


class Graph(Base, SchemaBase):
    __tablename__ = 'graph'
    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    updated_ts = Column(DATE, nullable=False, onupdate=datetime.now)


class GraphChain(Base, SchemaBase):
    __tablename__ = 'graph_chain'

    id = Column(BIGINT, primary_key=True)
    create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    graph_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.graph.id'))
    chain_id = Column(BIGINT, ForeignKey(f'{SCHEMA_NAME}.chain.id'))
