from datetime import datetime
import json
from typing import Any, Dict, Generic, TypeVar, List, Optional, Protocol, Type

import attr

from liquorice.core import exceptions
from liquorice.core.const import TaskStatus
from liquorice.core.database import QueuedTask


@attr.s
class Toolbox(Protocol):
    ...


@attr.s
class Job(Protocol):
    @staticmethod
    def name() -> str:
        raise NotImplementedError

    async def start(self, toolbox: Toolbox) -> Any:
        return await self.run(toolbox)

    async def run(self, toolbox: Toolbox) -> Any:
        raise NotImplementedError


GenericJob = TypeVar('GenericJob', bound=Job)


@attr.s
class JobRegistry:
    toolbox: Toolbox = attr.ib()
    _jobs: Dict[str, Type[Job]] = attr.ib(default=attr.Factory(dict))

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    def job(self, cls: Type[Job]) -> None:
        name = cls.name()
        if name in self._jobs:
            raise exceptions.DuplicateTaskError(name)
        self._jobs[name] = cls
        return cls

    def get(self, name: str) -> Optional[Type[Job]]:
        return self._jobs.get(name, None)


@attr.s
class Schedule:
    due_at: Optional[datetime] = attr.ib(default=None)
    after_tasks: List['Task'] = attr.ib(default=attr.Factory(list))

    @classmethod
    def now(cls) -> 'Schedule':
        return cls(due_at=datetime.utcnow())

    @classmethod
    def after(cls, task: 'Task') -> 'Schedule':
        return cls(after_tasks=[task])


@attr.s
class Task(Generic[GenericJob]):
    job: GenericJob = attr.ib()
    schedule: Schedule = attr.ib(default=Schedule.now())
    status: TaskStatus = attr.ib(default=TaskStatus.NEW)
    id: int = attr.ib(default=None)
    result: Any = attr.ib(default=None)

    @classmethod
    def from_queued_task(
        cls, queued_task: QueuedTask, job_cls: GenericJob,
    ) -> 'Task':
        return cls(
            id=queued_task.id,
            job=job_cls(**queued_task.data),
            schedule=Schedule(due_at=queued_task.due_at),
            status=queued_task.status,
            result=json.loads(queued_task.result),
        )

    async def save(self) -> QueuedTask:
        if self.id:
            queued_task = await QueuedTask.get(self.id)
            queued_task.status = self.status
            queued_task.result = self.result
            await queued_task.apply()
        else:
            queued_task = await QueuedTask.create(
                job=self.job.name(),
                data=attr.asdict(self.job),
                due_at=self.schedule.due_at,
                # after_tasks=self.schedule.after,
                status=self.status,
                result=json.dumps(self.result),
            )
            self.id = queued_task.id
        return queued_task
