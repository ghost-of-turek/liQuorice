import asyncio
import threading

import attr

from liquorice.laborer.puller import Puller
from liquorice.laborer.worker import Worker


@attr.s
class WorkerThread(threading.Thread):
    _loop: asyncio.AbstractEventLoop = attr.ib(default=asyncio.new_event_loop)
    _puller: Puller = attr.ib(default=Puller)
    _worker: Worker = attr.ib(default=Worker)
