import asyncio
from logging import Logger
import threading

import attr

from liquorice.core.tasks import JobRegistry, Toolbox
from liquorice.worker.puller import Puller
from liquorice.worker.dispatcher import Dispatcher


@attr.s
class Runner:
    name: str = attr.ib()
    job_registry: JobRegistry = attr.ib()
    toolbox: Toolbox = attr.ib()
    logger: Logger = attr.ib()
    stop_event: threading.Event = attr.ib()

    _puller: Puller = attr.ib(default=None)
    _dispatcher: Dispatcher = attr.ib(default=None)

    def __attrs_post_init__(self):
        self._puller = Puller(
            job_registry=self.job_registry,
            logger=self.logger,
        )
        self._dispatcher = Dispatcher(
            toolbox=self.toolbox,
            logger=self.logger,
        )

    async def run(self):
        while not self.stop_event.is_set():
            task_id, job = await self._puller.pull()
            if job is None:
                self.logger.info(
                    f'Nothing to do, sleeping for 5s...',
                )
                await asyncio.sleep(5)
            else:
                self.logger.info(
                    f'Job `{job.name()}` will be scheduled '
                    f'to run for task {task_id} '
                    f'on worker {self.name}.'
                )
                await self._dispatcher.dispatch(task_id, job)
                await asyncio.sleep(0.5)

        await self._dispatcher.flush()
