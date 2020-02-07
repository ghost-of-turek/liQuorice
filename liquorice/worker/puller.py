from typing import Optional

import attr

from liquorice.core import Task


@attr.s
class Puller:
    async def pull(self) -> Optional[Task]:
        pass
