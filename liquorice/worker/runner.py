import asyncio
from typing import List, Optional
import threading

import attr

from liquorice.core import db, JobRegistry
from liquorice.worker.dispatcher import (
    DispatcherThread,
    RoundRobinSelector,
)
from liquorice.worker.threading import BaseThread
from liquorice.worker.worker import WorkerThread


@attr.s
class Runner:
    dsn: str = attr.ib()
    job_registry: JobRegistry = attr.ib()
    dispatchers: Optional[int] = attr.ib(default=None)
    workers: Optional[int] = attr.ib(default=None)
    worker_selector_class = attr.ib(default=None)
    worker_threads: List[WorkerThread] = attr.ib(default=attr.Factory(list))
    dispatcher_threads: List[DispatcherThread] = attr.ib(
        default=attr.Factory(list),
    )

    _stop_event: threading.Event = attr.ib(
        default=attr.Factory(threading.Event),
    )

    def __attrs_post_init__(self):
        if self.workers and not self.worker_threads:
            for id_ in range(self.workers):
                self.worker_threads.append(WorkerThread(id_=id_))
        if self.dispatchers and not self.dispatcher_threads:
            for id_ in range(self.dispatchers):
                worker_selector_cls = (
                    self.worker_selector_class or RoundRobinSelector
                )
                self.dispatcher_threads.append(DispatcherThread(

                    job_registry=self.job_registry,
                    worker_selector=worker_selector_cls(self.worker_threads),
                    id_=id_,
                ))

        self.workers = len(self.worker_threads)
        self.dispatchers = len(self.dispatcher_threads)

    async def run(self) -> None:
        async with db.with_bind(self.dsn):
            for handle in self._all_threads:
                handle.start()
            while not self._stop_event.is_set():
                await asyncio.sleep(0.1)
            for handle in self.worker_threads:
                handle.join()

    def stop(self) -> None:
        for handle in self._all_threads:
            handle.stop()
        self._stop_event.set()

    @property
    def _all_threads(self) -> List[BaseThread]:
        return self.dispatcher_threads + self.worker_threads

    def stop_on_signal(self, signal, frame):
        self.stop()
