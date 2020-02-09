import asyncio
from collections import defaultdict
from logging import Logger
from typing import Dict, List, Optional, Protocol, Tuple

import attr

from liquorice.core.database import db, QueuedTask
from liquorice.core.tasks import Job, JobRegistry, TaskStatus
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


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


@attr.s
class Puller:
    job_registry: JobRegistry = attr.ib()
    logger: Logger = attr.ib()

    async def pull(self) -> Optional[Tuple[int, Job]]:
        async with db.transaction():
            task = await QueuedTask.query.where(
                QueuedTask.status == TaskStatus.NEW,
            ).gino.first()

            if task is None:
                return None

            self.logger.info(f'Processing task {task.id}.')

            job_cls = self.job_registry.get(task.job)
            if job_cls is None:
                self.logger.warning(
                    f'Error while processing task {task.id}: '
                    f'job `{task.job.name()}` is not in the registry.')
                return None

            return task.id, job_cls(**task.data)


@attr.s
class Dispatcher:
    logger: Logger = attr.ib()
    job_registry: JobRegistry = attr.ib()
    worker_selector: WorkerSelector = attr.ib()

    async def dispatch(
        self, task_id: int, job: Job,
    ) -> Tuple[WorkerThread, asyncio.Task]:
        worker_thread = self.worker_selector.select()
        task = await worker_thread.schedule(job, self.job_registry.toolbox)
        self.logger.info(
            f'Job `{job.name()}` for task {task_id} '
            f'scheduled on worker {worker_thread.name}.'
        )
        return worker_thread, task


class DispatcherThread(BaseThread):
    def __init__(
        self, job_registry: JobRegistry,
        worker_selector: WorkerSelector, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self._jobs: Dict[asyncio.Future, Job] = {}
        self._tasks: Dict[asyncio.Future, str] = defaultdict(list)
        self._puller = Puller(
            job_registry=job_registry,
            logger=self._logger,
        )
        self._dispatcher = Dispatcher(
            job_registry=job_registry,
            worker_selector=worker_selector,
            logger=self._logger,
        )

    @property
    def ident(self) -> str:
        return f'liquorice.dispatcher'

    async def _setup(self) -> None:
        await super()._setup()
        self._logger.info(f'Dispatcher thread {self.name} is up and running.')

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            task_id, job = await self._puller.pull()
            if job is None:
                self._logger.info(
                    f'Nothing to do, sleeping for 5s...',
                )
                await asyncio.sleep(5)
            else:
                self._logger.info(
                    f'Job `{job.name()}` will be scheduled '
                    f'to run for task {task_id} '
                    f'on worker {self.name}.'
                )
                worker_thread, task = await self._dispatcher.dispatch(
                    task_id, job,
                )
                print(task)
                self._jobs[task] = job
                self._tasks[task].append(worker_thread)

        await asyncio.gather(*self._tasks)

    async def _teardown(self) -> None:
        self._logger.info(
            f'Dispatcher thread {self.name} shut down successfully.',
        )
        await super()._teardown()
