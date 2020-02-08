from datetime import datetime
from typing import Any, Dict, Generic, TypeVar, List, Optional, Protocol, Type

import attr

from liquorice.core.const import TaskStatus
from liquorice.core.db import QueuedTask


@attr.s
class Toolbox(Protocol):
    ...


@attr.s
class Job(Protocol):
    @staticmethod
    def name() -> str:
        pass

    async def run(self, toolbox: Toolbox) -> Any:
        pass


GenericJob = TypeVar('GenericJob', bound=Job)


@attr.s
class JobRegistry:
    _jobs: Dict[str, Type[Job]] = attr.ib(default=attr.Factory(dict))

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    def job(self, cls: Type[Job]) -> None:
        self._jobs[cls.name()] = cls
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

    queued_task: QueuedTask = attr.ib(default=None)

    @property
    def id(self) -> Optional[int]:
        if self.queued():
            return self.queued_task.id
        return None

    @property
    def queued(self) -> bool:
        return self.queued_task is not None


@attr.s
class Scheduler:
    async def schedule(self, task: Task) -> QueuedTask:
        return await QueuedTask.create(
            job=task.job.name(),
            data=attr.asdict(task.job),
            due_at=task.schedule.due_at,
            # after_tasks=task.schedule.after,
            status=task.status
        )
