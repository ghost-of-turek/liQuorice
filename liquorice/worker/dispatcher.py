import asyncio
from typing import List, Protocol

import attr

from liquorice.core.database import db, QueuedTask
from liquorice.core.tasks import JobRegistry, Task, TaskStatus
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


@attr.s
class WorkerSelector(Protocol):
    workers: List[WorkerThread] = attr.ib()

    def select(self, task: Task) -> WorkerThread:
        raise NotImplementedError


@attr.s
class RoundRobinSelector(WorkerSelector):
    _index: int = attr.ib(default=0)

    def select(self, task: Task) -> WorkerThread:
        worker = self.workers[self._index]
        self._index = (self._index + 1) % len(self.workers)
        return worker


class DispatcherThread(BaseThread):
    def __init__(
        self, job_registry: JobRegistry,
        worker_selector: WorkerSelector, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self._worker_selector = worker_selector
        self._job_registry = job_registry
        self._running_tasks: List[asyncio.Task] = []

    @property
    def ident(self) -> str:
        return 'liquorice.dispatcher'

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Dispatcher thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            task = await self._pull_task()
            if task is not None:
                self._running_tasks.append(task)

            if self._running_tasks:
                (done, pending) = await asyncio.wait(
                    self._running_tasks,
                    timeout=0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    await task
                self._running_tasks = list(pending)

        await asyncio.gather(*self._running_tasks)

    async def _pull_task(self) -> asyncio.Task:
        async with db.transaction():
            queued_task = await QueuedTask.pull()

        if queued_task is None:
            return None

        self._logger.info(f'Processing task {queued_task.id}.')

        job_cls = self._job_registry.get(queued_task.job)
        if job_cls is None:
            self._logger.warning(
                f'Error while pulling task {queued_task.id}: '
                f'job `{queued_task.job}` is not in the registry.',
            )
            return None

        task = Task.from_queued_task(queued_task, job_cls)

        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} pulled.',
        )

        return asyncio.create_task(self._process_task(task))

    async def _process_task(self, task: Task) -> None:
        worker_thread = self._worker_selector.select(task)

        future = worker_thread.schedule(
            task.job, self._job_registry.toolbox,
        )
        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} '
            f'scheduled on worker {worker_thread.name}.',
        )

        await future
        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} '
            'completed successfully.',
        )

        task.result = future.result()
        if isinstance(task.result, Exception):
            task.status = TaskStatus.ERROR
        else:
            task.status = TaskStatus.DONE

        self.processed_tasks += 1

        async with db.transaction():
            await task.save()

    async def _teardown(self) -> None:
        self._logger.info(
            f'Dispatcher thread {self.name} shut down successfully.',
        )
        await super()._teardown()
