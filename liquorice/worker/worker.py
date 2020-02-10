import asyncio

from liquorice.core import Job, Toolbox
from liquorice.worker.threading import BaseThread


class WorkerThread(BaseThread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tasks = []

    @property
    def ident(self) -> str:
        return 'liquorice.worker'

    def schedule(self, job: Job, toolbox: Toolbox) -> asyncio.Future:
        future = asyncio.Future()
        self.loop.create_task(self._schedule(job, toolbox, future))
        return future

    async def _schedule(
        self, job: Job, toolbox: Toolbox, future: asyncio.Future,
    ) -> None:
        result = await job.start(toolbox)
        self.processed_tasks += 1
        future.set_result(result)

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)

        await asyncio.gather(
            *[await task for task in self._tasks],
        )

    async def _teardown(self) -> None:
        self._logger.info(f'Worker thread {self.name} shut down successfully.')
        await super()._teardown()
