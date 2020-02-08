import asyncio
from logging import Logger
from typing import List

import attr

from liquorice.core.tasks import Job, Toolbox


@attr.s
class Dispatcher:
    toolbox: Toolbox = attr.ib()
    logger: Logger = attr.ib()
    _handles: List[asyncio.Task] = attr.ib(default=attr.Factory(list))

    async def dispatch(self, task_id: int, job: Job) -> None:
        self._handles.append(asyncio.create_task(job.run(self.toolbox)))
        self.logger.info(
            f'Job `{job.name()}` for task {task_id} scheduled successfully.'
        )

    async def flush(self) -> None:
        await asyncio.gather(*self._handles)
