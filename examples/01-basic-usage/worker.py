import asyncio
import signal

from liquorice.core import db, MultiLoopPool
from liquorice.worker import Runner

from common import job_registry, DSN, ExampleToolbox, Printer


async def main():
    toolbox = ExampleToolbox(printer=Printer())
    runner = Runner(
        dispatchers=1,
        workers=5,
        job_registry=job_registry,
        toolbox=toolbox,
    )
    signal.signal(signal.SIGINT, lambda s, f: runner.stop())

    async with db.with_bind(DSN, pool_class=MultiLoopPool):
        await runner.run()


asyncio.run(main())
