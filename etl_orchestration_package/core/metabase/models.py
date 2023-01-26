import os

from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, ForeignKey, text
from sqlalchemy.dialects.postgresql import BIGINT, TIMESTAMP, VARCHAR, SMALLINT, DATE, JSONB
from sqlalchemy.orm import relationship, validates

Base = declarative_base()


class SchemaBase:
    """
    Общий класс для определения схемы
    """
    __table_args__ = {'schema': os.getenv('SCHEMA_NAME', 'public')}

    def to_dict(self):
        return self.__dict__


class Service(Base, SchemaBase):
    __tablename__ = 'service'

    id = Column(
        BIGINT,
        # Identity(start=1, increment=1, cycle=True),
        autoincrement=True,
        primary_key=True,
    )
    create_ts = Column(
        TIMESTAMP,
        server_default=text('now()'),
    )
    name = Column(VARCHAR(64), unique=True, nullable=False)

    processes = relationship(
        'Process',
        back_populates='service',
        cascade="all, delete",
        passive_deletes=True,
    )


class Process(Base, SchemaBase):
    __tablename__ = 'process'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    create_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    service_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.service.id', ondelete="CASCADE"),
    )
    # TODO: Должен быть уникальным только в рамках конкретного сервиса. Убрать 'unique=True'
    uid = Column(VARCHAR(128), unique=True, nullable=False)

    service = relationship('Service', back_populates='processes')
    tasks = relationship('Task', back_populates='process')


class Task(Base, SchemaBase):
    __tablename__ = 'task'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    create_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    process_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.process.id', ondelete="CASCADE"),
    )
    config_template = Column(JSONB, nullable=False)

    process = relationship('Process', back_populates='tasks')
    task_runs = relationship('TaskRun', back_populates='task')


class TaskRunStatus(Base, SchemaBase):
    __tablename__ = 'task_run_status'

    id = Column(SMALLINT, primary_key=True, autoincrement=True)
    create_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    status = Column(VARCHAR(64), nullable=True, unique=True)

    task_run_status = relationship('TaskRun', back_populates='status')


class TaskRun(Base, SchemaBase):
    __tablename__ = 'task_run'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    # create_ts = Column(TIMESTAMP, nullable=False, default=datetime.now)
    task_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task.id', ondelete="CASCADE"),
    )
    status_id = Column(
        SMALLINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task_run_status.id', ondelete="CASCADE"),
    )
    config = Column(JSONB, nullable=False)
    result = Column(JSONB, nullable=False)
    created_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    updated_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
        server_onupdate=text('now()'),
    )

    task = relationship('Task', back_populates='task_runs')
    status = relationship('TaskRunStatus', back_populates='task_run_status')


class Chain(Base, SchemaBase):
    __tablename__ = 'chain'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    create_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    previous_task_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task.id', ondelete="CASCADE"),
    )
    next_task_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task.id', ondelete="CASCADE"),
    )

    previous_task = relationship('Task', foreign_keys=[previous_task_id])
    next_task = relationship('Task', foreign_keys=[next_task_id])

    @validates('previous_task', 'next_task')
    def validate(self, key, value):
        if key == 'next_task':
            assert self.previous_task != value
        return value


class Graph(Base, SchemaBase):
    __tablename__ = 'graph'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    created_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    updated_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
        server_onupdate=text('now()'),
    )
    name = Column(VARCHAR(128), nullable=False, unique=True)


class GraphChain(Base, SchemaBase):
    __tablename__ = 'graph_chain'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    created_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    graph_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.graph.id', ondelete="CASCADE"),
    )
    chain_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.chain.id', ondelete="CASCADE"),
    )


class GraphRun(Base, SchemaBase):
    __tablename__ = 'graph_run'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    created_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    graph_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.graph.id', ondelete="CASCADE"),
    )
    status_id = Column(
        SMALLINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task_run_status.id', ondelete="CASCADE"),
    )
    config = Column(JSONB, nullable=False)
    result = Column(JSONB, nullable=False)
    updated_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
        server_onupdate=text('now()'),
    )


class GraphRunTaskRun(Base, SchemaBase):
    __tablename__ = 'graph_run_task_run'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    created_ts = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text('now()'),
    )
    graph_run_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.graph_run.id', ondelete="CASCADE"),
    )
    task_run_id = Column(
        BIGINT,
        ForeignKey(f'{SchemaBase.__table_args__.get("schema")}.task_run.id', ondelete="CASCADE"),
    )
