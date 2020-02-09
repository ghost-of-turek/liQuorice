import asyncio
import time

# import coloredlogs
from liquorice.core import db, MultiLoopPool
from liquorice.worker import (
    DispatcherThread,
    RoundRobinSelector,
    WorkerThread,
)

from common import job_registry, DSN, ExampleToolbox, Printer


async def worker():
    toolbox = ExampleToolbox(printer=Printer())

    async with db.with_bind(DSN, pool_class=MultiLoopPool):
        worker_handles = []
        dispatcher_handles = []

        for id_ in range(10):
            handle = WorkerThread(
                toolbox=toolbox,
                id_=id_,
            )
            handle.start()
            worker_handles.append(handle)

        for id_ in range(1):
            handle = DispatcherThread(
                job_registry=job_registry,
                worker_selector=RoundRobinSelector(worker_handles),
                id_=id_,
            )
            handle.start()
            dispatcher_handles.append(handle)

        handles = worker_handles + dispatcher_handles

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            for handle in handles:
                handle.stop()
        finally:
            for handle in handles:
                handle.join()


asyncio.run(worker())
