from typing import Optional, Tuple
from logging import Logger

import attr

from liquorice.core.const import TaskStatus
from liquorice.core.db import QueuedTask
from liquorice.core.tasks import Job, JobRegistry


@attr.s
class Puller:
    job_registry: JobRegistry = attr.ib()
    logger: Logger = attr.ib()

    async def pull(self) -> Optional[Tuple[int, Job]]:
        task = await QueuedTask.query.where(
            QueuedTask.status == TaskStatus.NEW,
        ).gino.first()

        if task is None:
            return None

        self.logger.info(f'Processing task {task.id}.')

        job_cls = self.job_registry.get(task.job)
        if job_cls is None:
            self.logger.warning(
                f'Error while processing task {task.id}: '
                f'job `{task.job.name()}` is not in the registry.')
            return None

        return task.id, job_cls(**task.data)
