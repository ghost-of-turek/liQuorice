from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

import attr

from liquorice.core.const import TaskStatus
from liquorice.core.db import QueuedTask


@attr.s
class Toolbox(Protocol):
    ...


@attr.s
class Job(Protocol):
    def name(self) -> str:
        pass

    async def run(self, toolbox: Toolbox) -> Any:
        pass


@attr.s
class Registry:
    _toolbox: Toolbox = attr.ib(default=attr.Factory(Toolbox))
    _jobs: Dict[str, Job] = attr.ib(default=attr.Factory(dict))

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    def job(self, job: Job) -> None:
        self._jobs[job.name(), job]

    def get(self, name: str) -> Optional[Job]:
        return self._jobs.get(name, default=None)


@attr.s
class Schedule:
    due_at: Optional[datetime] = attr.ib(default=None)
    after: Optional[List['Task']] = attr.ib(default=attr.Factory(list))

    @classmethod
    def now(cls) -> 'Schedule':
        return cls(due_at=datetime.utcnow())

    @classmethod
    def after(cls, task: 'Task') -> 'Schedule':
        return cls(after=[task])


@attr.s
class Task:
    job: Job = attr.ib()
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
            data={},  # fixme: this
            due_at=task.schedule.due_at,
            # after=task.schedule.after,
            status=task.status
        )
