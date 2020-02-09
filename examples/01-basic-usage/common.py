from typing import Dict
import threading

import attr
from liquorice.core.tasks import Job, JobRegistry, Toolbox


DSN = 'postgres://liquorice:liquorice@localhost:5432/liquorice'


@attr.s
class ExampleToolbox(Toolbox):
    contacts: Dict[str, str] = attr.ib()


job_registry = JobRegistry()


@job_registry.job
@attr.s
class SendMessage(Job):
    message: str = attr.ib(default='')
    to: str = attr.ib(default='nietzche')

    @staticmethod
    def name() -> str:
        return 'send_message'

    async def run(self, toolbox: ExampleToolbox) -> None:
        print(f'To: {toolbox.contacts[self.to]}')
        print(f'Message: {self.message}')
        print(f'Ran on [{threading.current_thread().name}]')
