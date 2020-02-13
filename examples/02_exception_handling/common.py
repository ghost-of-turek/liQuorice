import random

import attr
from liquorice.core import Job, JobRegistry, Toolbox


DSN = 'postgres://liquorice:liquorice@localhost:5432/liquorice'


class Printer:
    OK = 'ok'
    ERROR = 'error'

    def print(self, message: str):
        print(message)

    @property
    def status(self):
        if random.randrange(1, 5) == 3:
            return Printer.ERROR
        return Printer.OK


class FUBARError(Exception):
    pass


@attr.s
class BasicToolbox(Toolbox):
    printer: Printer = attr.ib(default=attr.Factory(Printer))


job_registry = JobRegistry(toolbox=BasicToolbox())


@job_registry.job
@attr.s
class PrintMessage(Job):
    message: str = attr.ib()

    @staticmethod
    def name() -> str:
        return 'print_message'

    async def run(self, toolbox: BasicToolbox) -> None:
        if toolbox.printer.status == Printer.ERROR:
            raise FUBARError('Everything went FUBAR!')
        toolbox.printer.print(f'Message: {self.message}')
        return 'Operation completed successfully.'
