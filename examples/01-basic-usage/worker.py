import asyncio
import time

import coloredlogs
from liquorice.core.db import db, AsyncThreadedPool
from liquorice.worker import WorkerThread

from common import job_registry, DSN, ExampleToolbox


async def worker():
    toolbox = ExampleToolbox(
        contacts={
            'nietzsche': 'Friedrich Nietzsche <nietzsche@philo.com>',
            'abyss': 'Mr. Abyss <abyss@scarystuff.org>',
        },
    )

    async with db.with_bind(DSN, pool_class=AsyncThreadedPool):
        handles = []
        for id_ in range(2):
            handle = WorkerThread(
                id_=id_,
                job_registry=job_registry,
                toolbox=toolbox,
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
