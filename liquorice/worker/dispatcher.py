import asyncio
from logging import Logger

import attr

from liquorice.core.tasks import Job, Toolbox


@attr.s
class Dispatcher:
    toolbox: Toolbox = attr.ib()
    logger: Logger = attr.ib()

    async def dispatch(self, job: Job):
        task = asyncio.create_task(job.run(self.toolbox))
        self.logger.info(
            f'Job `{job.name()}` scheduled successfully.'
        )
        return task
