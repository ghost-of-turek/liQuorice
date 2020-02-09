import asyncio
import random

from liquorice.core import db, Task, Scheduler

from common import DSN, PrintMessage


async def main():
    async with db.with_bind(DSN):
        scheduler = Scheduler()
        available_tasks = get_tasks()
        tasks = []

        try:
            while True:
                tasks.append(
                    scheduler.schedule(random.choice(available_tasks)),
                )
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass

        await asyncio.gather(*tasks)


def get_tasks():
    return [
        Task(PrintMessage('<Nietzsche stares at the Abyss awkwardly>')),
        Task(PrintMessage('<The Abyss stares at Nietzche majestically>')),
    ]


asyncio.run(main())
