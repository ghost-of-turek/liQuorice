import attr

from typing import Dict

from liquorice.core.tasks import Job, Toolbox


DSN = 'postgres://liquorice:liquorice@localhost:5432/liquorice'


@attr.s
class ExampleToolbox(Toolbox):
    contacts: Dict[str, str] = attr.ib()


@attr.s
class SendMessage(Job):
    message: str = attr.ib(default='')
    to: str = attr.ib(default='artur')

    def name(self) -> str:
        return 'send_message'

    async def run(self, toolbox: ExampleToolbox) -> None:
        print(f'To: {toolbox.contacts[self.to]}\n{self.message}\n')
