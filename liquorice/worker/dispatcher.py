import asyncio
from collections import defaultdict
from typing import Dict, List, Optional, Protocol, Tuple

import attr

from liquorice.core.database import db, QueuedTask
from liquorice.core.tasks import Job, JobRegistry, Schedule, Task, TaskStatus
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


@attr.s
class RunningTask:
    task: Task = attr.ib()
    worker_thread: WorkerThread = attr.ib()
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
        return f'liquorice.dispatcher'

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Dispatcher thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            task = await self._pull_task()
            if task is None:
                self._logger.info(
                    f'Nothing to do, sleeping for 5s...',
                )
                await asyncio.sleep(5)
            else:
                await self._handle_task(task)

        await asyncio.gather(*self._workers)

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
                    f'job `{queued_task.job.name()}` is not in the registry.',
                )
                return None

            return Task(
                job=job_cls(**queued_task.data),
                schedule=Schedule(due_at=queued_task.due_at),
                status=queued_task.status,
                queued_task=queued_task,
            )

    async def _handle_task(self, task) -> None:
        worker_thread = self._worker_selector.select()
        future = await worker_thread.schedule(
            task.job, self._job_registry.toolbox,
        )
        self._running_tasks.append(RunningTask(task, worker_thread, future))
        self._logger.info(
            f'Job `{task.job.name()}` for task {task.id} '
            f'scheduled on worker {worker_thread.name}.'
        )

    async def _teardown(self) -> None:
        self._logger.info(
            f'Dispatcher thread {self.name} shut down successfully.',
        )
        await super()._teardown()
