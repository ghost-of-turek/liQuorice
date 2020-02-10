import asyncio
import random

import attr
from liquorice.core import Job, JobRegistry, Toolbox


DSN = 'postgres://liquorice:liquorice@localhost:5432/liquorice'


class Printer:
    def print(self, string):
        print(string)


@attr.s
class PrintingRoom(Toolbox):
    printer: Printer = attr.ib()


job_registry = JobRegistry(toolbox=PrintingRoom(printer=Printer()))


@job_registry.job
@attr.s
class PrintMessage(Job):
    message: str = attr.ib()

    @staticmethod
    def name() -> str:
        return 'print_message'

    async def run(self, toolbox: PrintingRoom) -> None:
        toolbox.printer.print(f'Message: {self.message}')
        return await self._get_return_value()

    async def _get_return_value(self) -> int:
        await asyncio.sleep(random.random())
        return 42
