import asyncio

from liquorice.core import Job, Toolbox
from liquorice.worker.threading import BaseThread


class WorkerThread(BaseThread):
    def __init__(self, toolbox: Toolbox, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._toolbox = toolbox

    @property
    def ident(self):
        return f'liquorice.worker'

    def schedule(self, job: Job):
        return self.loop.create_task(self._schedule(job))

    async def _schedule(self, job: Job):
        return self.loop.create_task(job.run(self._toolbox))

    async def _setup(self):
        await super()._setup()
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _run(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(1)

    async def _teardown(self):
        self._logger.info(f'Worker thread {self.name} shut down successfully.')
        await super()._teardown()
