from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional

from pydantic import BaseModel


class ServiceInfo(BaseModel):
    name: str


class ProcessInfo(BaseModel):
    service_id: int
    uid: str


class TaskInfo(BaseModel):
    process_id: int
    config_template: Dict[str, Any]


class TaskRunStatus(str, Enum):
    created: str = 'CREATED'
    failed: str = 'FAILED'
    finished: str = 'FINISHED'


class TaskRunStatusInfo(BaseModel):
    status: TaskRunStatus


class TaskRunInfo(BaseModel):
    task_id: int
    status_id: int
    config: Dict[str, Any]
    result: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class ChainInfo(BaseModel):
    previous_task_id: int
    next_task_id: int


class CreatedObjectResponse(BaseModel):
    object_id: int
