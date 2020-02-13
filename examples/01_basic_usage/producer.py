import asyncio
import random
import signal
import threading

from liquorice.core import db, Task

from common import DSN, PrintMessage


stop_ev = threading.Event()


async def main():
    async with db.with_bind(DSN):
        await db.gino.create_all()

        while not stop_ev.is_set():
            task = random_task()
            await task.save()
            print(f'Scheduled task {task.id}.')
            await asyncio.sleep(5)

        await db.gino.drop_all()


def random_task() -> Task:
    return random.choice([
        Task(PrintMessage('<Nietzsche stares at the Abyss awkwardly>')),
        Task(PrintMessage('<The Abyss stares at Nietzche majestically>')),
    ])


signal.signal(signal.SIGTERM, lambda s, f: stop_ev.set())
signal.signal(signal.SIGINT, lambda s, f: stop_ev.set())


asyncio.new_event_loop().run_until_complete(main())
