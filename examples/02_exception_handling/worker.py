import asyncio
import signal

from liquorice.worker import Runner

from common import job_registry, DSN


runner = Runner(
    job_registry=job_registry,
    dsn=DSN,
    dispatchers=1,
    workers=1,
)
signal.signal(signal.SIGTERM, runner.stop_on_signal)
signal.signal(signal.SIGINT, runner.stop_on_signal)


async def main():
    await runner.run()

    for dispatcher_thread in runner.dispatcher_threads:
        print(
            f'Dispatcher thread {dispatcher_thread.name} '
            f'pulled {dispatcher_thread.processed_tasks} tasks.',
        )

    for worker_thread in runner.worker_threads:
        print(
            f'Worker thread {worker_thread.name} '
            f'ran {worker_thread.processed_tasks} tasks.',
        )


asyncio.new_event_loop().run_until_complete(main())
