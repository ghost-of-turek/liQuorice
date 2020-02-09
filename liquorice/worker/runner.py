import asyncio
from typing import List
import threading

import attr

from liquorice.core import JobRegistry, Toolbox
from liquorice.worker.dispatcher import (
    DispatcherThread,
    RoundRobinSelector,
)
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


@attr.s
class Runner:
    dispatchers: int = attr.ib()
    workers: int = attr.ib()
    job_registry: JobRegistry = attr.ib()
    toolbox: Toolbox = attr.ib()

    _stop_event: threading.Event = attr.ib(
        default=attr.Factory(threading.Event),
    )
    _worker_threads: List[WorkerThread] = attr.ib(default=attr.Factory(list))
    _dispatcher_threads: List[DispatcherThread] = attr.ib(
        default=attr.Factory(list),
    )

    def __attrs_post_init__(self):
        for id_ in range(self.workers):
            worker_thread = WorkerThread(
                toolbox=self.toolbox,
                id_=id_,
            )
            self._worker_threads.append(worker_thread)
        for id_ in range(self.dispatchers):
            dispatcher_thread = DispatcherThread(
                job_registry=self.job_registry,
                worker_selector=RoundRobinSelector(self._worker_threads),
                id_=id_,
            )
            self._dispatcher_threads.append(dispatcher_thread)

    async def run(self) -> None:
        for handle in self._all_threads:
            handle.start()
        while not self._stop_event.is_set():
            await asyncio.sleep(1)
        for handle in self._all_threads:
            handle.join()

    def stop(self) -> None:
        self._stop_event.set()
        for handle in self._all_threads:
            handle.stop()

    @property
    def _all_threads(self) -> List[BaseThread]:
        return self._worker_threads + self._dispatcher_threads

    def stop_on_signal(self, signal, frame):
        self.stop()
