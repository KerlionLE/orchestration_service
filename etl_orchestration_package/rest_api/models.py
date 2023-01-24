from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional

from pydantic import BaseModel


class ServiceInfo(BaseModel):
    id: Optional[int]
    create_ts: Optional[datetime]
    name: str


class ProcessInfo(BaseModel):
    id: Optional[int]
    service_id: int
    uid: str


class TaskInfo(BaseModel):
    id: Optional[int]
    process_id: int
    config_template: Dict[str, Any]


class TaskRunStatus(str, Enum):
    created: str = 'CREATED'
    failed: str = 'FAILED'
    finished: str = 'FINISHED'


class TaskRunStatusInfo(BaseModel):
    id: Optional[int]
    status: TaskRunStatus


class TaskRunInfo(BaseModel):
    id: Optional[int]
    task_id: int
    status_id: int
    config: Dict[str, Any]
    result: str


class ChainInfo(BaseModel):
    id: Optional[int]
    previous_task_id: int
    next_task_id: int


class GraphInfo(BaseModel):
    id: Optional[int]
    create_ts: Optional[datetime]
    name: str


class GraphChainInfo(BaseModel):
    id: Optional[int]
    graph_id: int
    chain_id: int


class GraphRunInfo(BaseModel):
    id: Optional[int]
    created_ts: Optional[datetime]
    updated_ts: Optional[datetime]
    graph_id: int
    status_id: int
    config: Dict[str, Any]
    result: Dict[str, Any]


class TaskUpdateValue(BaseModel):
    config_template: Dict[str, Any]


class TaskRunUpdateValue(BaseModel):
    status_id: Optional[int]
    config: Optional[Dict[str, Any]]
    result: Optional[str]


class GraphRunUpdateValue(BaseModel):
    status_id: Optional[int]
    config: Optional[Dict[str, Any]]
    result: Optional[Dict[str, Any]]


# ======================================================================================================================


class CreatedObjectResponse(BaseModel):
    object_id: int
