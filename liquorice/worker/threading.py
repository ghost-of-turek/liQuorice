import asyncio
import logging
import threading
from typing import Any, List

from liquorice.core.exceptions import format_exception


class BaseThread(threading.Thread):
    def __init__(self, id_: Any = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.id = id_ if id_ is not None else threading.get_ident()
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self.processed_tasks: int = 0
        self.successful_tasks: int = 0
        self.error: asyncio.Future = asyncio.Future()

        self._stop_event: threading.Event = threading.Event()
        self._logger: logging.Logger = logging.getLogger(self.ident)
        self._running_tasks: List[asyncio.Task] = []

    def run(self) -> None:
        self.loop.set_exception_handler(self._handle_exception)
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._setup())

        self.loop.run_until_complete(self._run())

        self.loop.run_until_complete(self._teardown())

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def ident(self) -> str:
        return f'{self.name}.{self.id}'

    async def _setup(self) -> None:
        self._logger.info(f'Thread {self.ident} is up and running.')

    async def _run(self) -> None:
        raise NotImplementedError

    async def _teardown(self) -> None:
        timeout = 10
        if self._running_tasks:
            self._logger.info(
                f'Waiting for up to {timeout} seconds before '
                'cancelling outstanding tasks.',
            )
            done, pending = await asyncio.wait(
                self._running_tasks,
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in done:
                await task
            for task in pending:
                task.cancel()
            if pending:
                self._logger.info(
                    f'Cancelling {len(pending)} outstanding tasks.'
                )

        self._logger.info(
            f'Thread {self.ident} shut down successfully.',
        )

    def _handle_exception(
        self, loop: asyncio.AbstractEventLoop, context: dict,
    ) -> None:
        msg = context.get('exception', context['message'])
        if not self.error.done():
            self.error.set_result(msg)
        if isinstance(msg, Exception):
            msg = format_exception(msg)
        self._logger.exception(
            f'Critical exception thrown in {self.ident}: {msg}',
        )
        self.stop()
