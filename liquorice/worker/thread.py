import asyncio
import logging
import threading
from typing import Any

import aiolog

from liquorice.core.tasks import JobRegistry, Toolbox
from liquorice.worker.runner import Runner


class WorkerThread(threading.Thread):
    def __init__(
        self, job_registry: JobRegistry, toolbox: Toolbox,
        id_: Any = None, sleep: int = 5, *args, **kwargs,
    ):
        kwargs['daemon'] = True
        super().__init__(*args, **kwargs)

        self.id = threading.get_ident() if id_ is None else id_
        self._start_event = threading.Event()
        self._stop_event = threading.Event()
        self._loop = asyncio.new_event_loop()

        self._sleep = sleep
        self._logger = logging.getLogger('liquorice.workerthread')
        self._runner = self._get_runner(job_registry, toolbox)

    @property
    def name(self):
        return f'liquorice-workerthread-{self.id}'

    def run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._setup())
        self._loop.run_until_complete(self._runner.run())
        self._loop.run_until_complete(self._teardown())

    def stop(self):
        self._loop.call_soon(self._stop_event.set)

    def _get_runner(self, job_registry, toolbox):
        return Runner(
            name=f'{self.name}-runner',
            job_registry=job_registry,
            toolbox=toolbox,
            logger=self._logger,
            stop_event=self._stop_event,
        )

    async def _setup(self):
        aiolog.start(loop=self._loop)
        self._logger.info(f'Worker thread {self.name} is up and running.')

    async def _teardown(self):
        self._logger.info(f'Worker thread {self.name} shut down successfully.')
        await aiolog.stop()
