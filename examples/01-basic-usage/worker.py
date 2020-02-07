import asyncio

from liquorice.core.db import db
from liquorice.worker import WorkerThread

from common import DSN, ExampleToolbox


toolbox = ExampleToolbox(contacts={
    'nietzsche': 'Friedrich Nietzsche <nietzsche@philosophers.com>',
    'abyss': 'Mr. Abyss <abyss@concepts.org>',
})


async def worker():
    async with db.with_bind(DSN):
        worker = WorkerThread()
        worker.run()


asyncio.run(worker())
