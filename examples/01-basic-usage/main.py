import asyncio

from liquorice.core import Task, Schedule, Scheduler
from liquorice.core.db import db

from common import DSN, SendMessage


async def main():
    async with db.with_bind(DSN):
        scheduler = Scheduler()
        await asyncio.gather(
            *(scheduler.schedule(task) for task in get_tasks()),
        )


def get_tasks():
    return [
        Task(
            job=SendMessage(
                message='Always look at the bright side of life!',
                to='abyss',
            ),
            schedule=Schedule.now(),
        ),
        # `schedule=Schedule.now()` is the default, so you can omit it
        Task(
            job=SendMessage(
                message='Hello from the dark side!',
                to='nietzsche',
            ),
        ),
        Task(
            job=SendMessage(
                message='<stares at The Abyss in awkward silence>',
                to='abyss',
            ),
        ),
        # `SendMessage` by default sends them to Nietzsche so I can omit this.
        # Actually, let's omit all the boring crud and get to the point.
        Task(SendMessage('<stares at Nietzche majestically>')),
    ]


asyncio.run(main())
