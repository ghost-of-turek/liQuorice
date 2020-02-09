import asyncio
import signal

from liquorice.core import db, MultiLoopPool
from liquorice.worker import Runner

from common import job_registry, DSN


runner = Runner(
    dispatchers=1,
    workers=5,
    job_registry=job_registry,
)
signal.signal(signal.SIGTERM, runner.stop_on_signal)
signal.signal(signal.SIGINT, runner.stop_on_signal)


async def main():
    async with db.with_bind(DSN, pool_class=MultiLoopPool):
        await runner.run()

    for dispatcher_thread in runner._dispatcher_threads:
        print(
            f'Dispatcher thread {dispatcher_thread.name} '
            f'pulled {len(dispatcher_thread._running_tasks)} tasks.',
        )

    for worker_thread in runner._worker_threads:
        print(
            f'Worker thread {worker_thread.name} '
            f'ran {len(worker_thread._tasks)} tasks.',
        )


asyncio.run(main())
