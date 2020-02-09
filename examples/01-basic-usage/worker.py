import asyncio
import signal

from liquorice.core import db, MultiLoopPool
from liquorice.worker import Runner

from common import job_registry, DSN, PrintingRoom, Printer


runner = Runner(
    dispatchers=1,
    workers=5,
    job_registry=job_registry,
    toolbox=PrintingRoom(printer=Printer()),
)
signal.signal(signal.SIGTERM, runner.stop_on_signal)
signal.signal(signal.SIGINT, runner.stop_on_signal)


async def main():
    async with db.with_bind(DSN, pool_class=MultiLoopPool):
        await runner.run()

    for dispatcher_thread in runner._dispatcher_threads:
        tasks = dispatcher_thread._tasks
        tasks_cnt = sum(map(lambda k: len(tasks[k]), tasks))
        print(
            f'Dispatcher thread {dispatcher_thread.name} '
            f'pulled {tasks_cnt} tasks.',
        )

    for worker_thread in runner._worker_threads:
        print(
            f'Worker thread {worker_thread.name} '
            f'ran {len(worker_thread._done_tasks)} tasks.',
        )


asyncio.run(main())
