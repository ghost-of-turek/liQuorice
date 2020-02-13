import asyncio

from liquorice.core import Job, Toolbox
from liquorice.core.exceptions import JobError
from liquorice.worker.threading import BaseThread


class WorkerThread(BaseThread):
    @property
    def name(self) -> str:
        return 'liquorice.worker'

    def schedule(self, job: Job, toolbox: Toolbox) -> asyncio.Future:
        future = asyncio.Future()
        self._running_tasks.append(
            self.loop.create_task(self._schedule(job, toolbox, future)),
        )
        return future

    async def _schedule(
        self, job: Job, toolbox: Toolbox, future: asyncio.Future,
    ) -> None:
        try:
            result = await job.start(toolbox)
        except Exception as e:
            future.set_result(JobError(e))
        else:
            future.set_result(result)
            self.successful_tasks += 1
        finally:
            self.processed_tasks += 1

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            if self._running_tasks:
                (done, _) = await asyncio.wait(
                    self._running_tasks,
                    timeout=0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    await task
                    self._running_tasks.remove(task)
