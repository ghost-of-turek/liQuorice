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
                to='darkness',
            ),
            schedule=Schedule.now(),
        ),
        # `schedule=Schedule.now()` is the default, so you can omit it
        Task(
            job=SendMessage(
                message='Hello from the dark side!',
                to='artur',
            ),
        ),
        # `SendMessage` by default sends them to me
        # so I can omit this as well.
        Task(
            job=SendMessage(message='Stares at Artur in awkward silence.'),
        ),
        Task(
            job=SendMessage(
                message='Stares at Darkness in awkward silence.',
                to='darkness',
            ),
        ),
    ]


asyncio.run(main())
