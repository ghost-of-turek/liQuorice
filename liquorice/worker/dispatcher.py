import asyncio
from collections import defaultdict
from typing import Dict, List, Optional, Protocol, Tuple

import attr

from liquorice.core.database import db, QueuedTask
from liquorice.core.tasks import Job, JobRegistry, Task, TaskStatus
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


@attr.s
class RunningTask:
    task: Task = attr.ib()
    future: asyncio.Future = attr.ib()


@attr.s
class WorkerSelector(Protocol):
    workers: List[WorkerThread] = attr.ib()

    def select(self) -> WorkerThread:
        raise NotImplementedError


@attr.s
class RoundRobinSelector(WorkerSelector):
    _index: int = attr.ib(default=0)

    def select(self) -> WorkerThread:
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
        self._running_tasks: List[RunningTask] = []
        self._jobs: Dict[asyncio.Future, Job] = {}
        self._workers: Dict[asyncio.Future, str] = defaultdict(list)
        self._job_registry = job_registry

    @property
    def ident(self) -> str:
        return 'liquorice.dispatcher'

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Dispatcher thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            task = await self._pull_task()
            if task is None:
                self._logger.info(f'Nothing to do, sleeping for 5s...')
                await asyncio.sleep(10)
            else:
                self._running_tasks.append(task)

            await asyncio.sleep(2)

        await asyncio.gather(*self._running_tasks)

    async def _pull_task(self) -> Optional[Tuple[int, Job]]:
        async with db.transaction():
            queued_task = await QueuedTask.query.where(
                QueuedTask.status == TaskStatus.NEW,
            ).gino.first()

        if queued_task is None:
            return None

        self._logger.info(f'Processing task {queued_task.id}.')

        job_cls = self._job_registry.get(queued_task.job)
        if job_cls is None:
            self._logger.warning(
                f'Error while processing task {queued_task.id}: '
                f'job `{queued_task.job}` is not in the registry.',
            )
            return None

        task = Task.from_queued_task(queued_task, job_cls)

        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} pulled.',
        )

        return self.loop.create_task(self._process_task(task))

    async def _process_task(self, task: Task) -> None:
        worker_thread = self._worker_selector.select()
        future = asyncio.Future()
        future.set_result(await worker_thread.schedule(
            task.job, self._job_registry.toolbox,
        ))
        running_task = RunningTask(task, future)
        self._running_tasks.append(running_task)
        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} '
            f'scheduled on worker {worker_thread.name}.'
        )
        return await self._finalize_task(running_task)

    async def _finalize_task(self, running_task: RunningTask) -> None:
        task, future = attr.astuple(running_task, recurse=False)
        result = await future

        self._running_tasks.remove(running_task)
        self.processed_tasks += 1

        task.result = result
        if isinstance(result, Exception):
            task.status = TaskStatus.ERROR
        else:
            task.status = TaskStatus.DONE

        async with db.transaction():
            await running_task.task.apply()

    async def _teardown(self) -> None:
        self._logger.info(
            f'Dispatcher thread {self.name} shut down successfully.',
        )
        await super()._teardown()
