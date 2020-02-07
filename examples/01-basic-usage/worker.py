import asyncio

from liquorice.core.db import db

from common import DSN, ExampleToolbox


toolbox = ExampleToolbox(contacts={
    'artur': 'Artur Ciesielski',
    'darkness': 'The Darkness',
})


async def worker():
    async with db.with_bind(DSN):
        pass


asyncio.run(worker())
