from typing import Optional

import attr

from liquorice.core import Task


@attr.s
class Puller:
    def __init__(self, pull_timeout):
        self._pull_timeout = pull_timeout

    async def pull(self) -> Optional[Task]:
        pass
