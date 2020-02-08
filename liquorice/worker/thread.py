import asyncio
import logging
import threading
from typing import Any, Union
from sqlalchemy.engine.url import URL

import aiolog

from liquorice.core.db import db
from liquorice.core.tasks import JobRegistry, Toolbox
from liquorice.worker.puller import Puller
from liquorice.worker.dispatcher import Dispatcher


class WorkerThread(threading.Thread):
    def __init__(
        self, job_registry: JobRegistry, toolbox: Toolbox,
        dsn: Union[str, URL], id_: Any = None, sleep: int = 5,
        *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.id = threading.get_ident() if id_ is None else id_
        self._start_event = threading.Event()
        self._stop_event = threading.Event()
        self._loop = asyncio.new_event_loop()

        self._sleep = sleep
        self._dsn = dsn
        self._job_registry = job_registry
        self._toolbox = toolbox
        self._prepare_internals()

    @property
    def name(self):
        return f'liquorice-workerthread-{self.id}'

    def run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._setup())
        exit_code = self._loop.run_until_complete(self._run())
        self._loop.run_until_complete(self._teardown())
        return exit_code

    def stop(self):
        self._loop.call_soon(self._stop_event.set)

    async def _run(self):
        while not self._stop_event.is_set():
            task_id, job = await self._puller.pull()
            if job is None:
                self._logger.info(
                    f'Nothing to do, sleeping for {self._sleep} seconds...',
                )
                await asyncio.sleep(self._sleep)
            else:
                self._logger.info(
                    f'Job `{job.name()}` will be scheduled '
                    f'to run for task {task_id}.'
                )
                task = await self._dispatcher.dispatch(job)
                self._tasks.append(task)

        return 69

    def _prepare_internals(self):
        self._logger = logging.getLogger('liquorice.workerthread')
        self._puller = Puller(
            job_registry=self._job_registry,
            logger=self._logger,
        )
        self._dispatcher = Dispatcher(
            toolbox=self._toolbox,
            logger=self._logger,
        )
        self._tasks = []

    async def _setup(self):
        aiolog.start(loop=self._loop)
        await db.set_bind(self._dsn)
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _teardown(self):
        await db.pop_bind().close()
        self._logger.info(
            f'Worker thread {self.name} shut down successfully, '
            'shutting down logger now...',
        )
        await aiolog.stop()
