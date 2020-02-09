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


asyncio.run(main())
