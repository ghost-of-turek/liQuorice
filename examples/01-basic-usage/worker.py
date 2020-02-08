import asyncio
import time

import coloredlogs
from liquorice.worker import WorkerThread

from common import job_registry, DSN, ExampleToolbox


async def worker():
    toolbox = ExampleToolbox(
        contacts={
            'nietzsche': 'Friedrich Nietzsche <nietzsche@philo.com>',
            'abyss': 'Mr. Abyss <abyss@scarystuff.org>',
        },
    )

    run_threads(DSN, job_registry, toolbox)


def run_threads(dsn, job_registry, toolbox):
    handles = []
    for id_ in range(1):
        handle = WorkerThread(
            id_=id_,
            job_registry=job_registry,
            toolbox=toolbox,
            dsn=dsn,
        )
        handle.start()
        handles.append(handle)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for handle in handles:
            handle.stop()
    finally:
        for handle in handles:
            handle.join()


coloredlogs.install()
asyncio.run(worker())
