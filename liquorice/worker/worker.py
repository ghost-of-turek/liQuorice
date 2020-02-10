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

    async def schedule(self, job: Job, toolbox: Toolbox) -> asyncio.Future:
        future = asyncio.Future()
        future.set_result(self.loop.create_task(job.start(toolbox)))
        self._tasks.append(future)
        return await future

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(1)

        await asyncio.gather(
            *[await self._finalize(task) for task in self._tasks],
        )

    async def _finalize(self, task: asyncio.Future) -> None:
        self.processed_tasks += 1
        return await task

    async def _teardown(self) -> None:
        self._logger.info(f'Worker thread {self.name} shut down successfully.')
        await super()._teardown()
