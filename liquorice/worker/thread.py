import asyncio
import threading

import attr

from liquorice.worker.puller import Puller
from liquorice.worker.worker import Worker


@attr.s
class WorkerThread(threading.Thread):
    _loop: asyncio.AbstractEventLoop = attr.ib(
        default=attr.Factory(asyncio.new_event_loop),
    )
    _puller: Puller = attr.ib(default=attr.Factory(Puller))
    _worker: Worker = attr.ib(default=attr.Factory(Worker))
