import attr
from datetime import datetime
from typing import List, Optional

from liquorice.core.jobs import Job


@attr.s(frozen=True)
class Timetable:
    due_at: Optional[datetime] = attr.ib(default=None)
    after: Optional[List['Task']] = attr.ib(default=attr.Factory(list))

    @classmethod
    def now(cls) -> 'Timetable':
        return cls(due_at=datetime.utcnow())

    @classmethod
    def after(cls, task: 'Task') -> 'Timetable':
        return cls(after=[task])


@attr.s(frozen=True)
class Task:
    job: Job = attr.ib()
    timetable: Timetable = attr.ib(default=Timetable.now())
