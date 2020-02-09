import attr
from liquorice.core import Job, JobRegistry, Toolbox


DSN = 'postgres://liquorice:liquorice@localhost:5432/liquorice'


class Printer:
    def print(self, string):
        print(string)


@attr.s
class ExampleToolbox(Toolbox):
    printer: Printer = attr.ib()


job_registry = JobRegistry()


@job_registry.job
@attr.s
class PrintMessage(Job):
    message: str = attr.ib()

    @staticmethod
    def name() -> str:
        return 'print_message'

    async def run(self, toolbox: ExampleToolbox) -> None:
        toolbox.printer.print(f'Message: {self.message}')
