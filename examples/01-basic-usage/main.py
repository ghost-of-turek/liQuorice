import asyncio

from liquorice.core import db, Task, Scheduler

from common import DSN, PrintMessage


async def main():
    async with db.with_bind(DSN):
        scheduler = Scheduler()
        await asyncio.gather(
            *(scheduler.schedule(task) for task in get_tasks()),
        )


def get_tasks():
    return [
        Task(PrintMessage('<Nietzsche stares at the Abyss awkwardly>')),
        Task(PrintMessage('<The Abyss stares at Nietzche majestically>')),
    ]


asyncio.run(main())
