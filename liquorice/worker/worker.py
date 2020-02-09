import asyncio
from typing import Optional

from liquorice.core import Job, Toolbox
from liquorice.worker.threading import BaseThread


class WorkerThread(BaseThread):
    def __init__(self, toolbox: Toolbox, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._toolbox = toolbox

        self._done_tasks = []
        self._pending_tasks = []

    @property
    def ident(self) -> str:
        return f'liquorice.worker'

    def schedule(self, job: Job) -> None:
        self.loop.create_task(self._schedule(job))

    async def _schedule(self, job: Job) -> None:
        self._pending_tasks.append(
            self.loop.create_task(job.run(self._toolbox))
        )

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(1)
        while self._pending_tasks:
            done, pending = await asyncio.wait(
                self._pending_tasks, return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                self._pending_tasks.remove(task)
                self._done_tasks.append(task)

    async def _teardown(self) -> None:
        self._logger.info(f'Worker thread {self.name} shut down successfully.')
        await super()._teardown()

    async def _get_done_task(self) -> Optional[asyncio.Task]:
        return self._done_tasks.pop() if self._done_tasks else None
