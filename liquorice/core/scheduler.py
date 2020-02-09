import attr

from liquorice.core.database import QueuedTask
from liquorice.core.tasks import Task


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
