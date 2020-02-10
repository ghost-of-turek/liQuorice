import asyncio
import signal

from liquorice.core import db
from liquorice.worker import Runner

from common import job_registry, DSN

import coloredlogs


runner = Runner(
    dispatchers=1,
    workers=5,
    job_registry=job_registry,
)
signal.signal(signal.SIGTERM, runner.stop_on_signal)
signal.signal(signal.SIGINT, runner.stop_on_signal)


async def main():
    async with db.with_bind(DSN):
        await runner.run()

    # for dispatcher_thread in runner._dispatcher_threads:
    #     print(
    #         f'Dispatcher thread {dispatcher_thread.name} '
    #         f'pulled {dispatcher_thread.processed_tasks} tasks.',
    #     )

    # for worker_thread in runner._worker_threads:
    #     print(
    #         f'Worker thread {worker_thread.name} '
    #         f'ran {worker_thread.processed_tasks} tasks.',
    #     )


coloredlogs.install()
asyncio.run(main())
