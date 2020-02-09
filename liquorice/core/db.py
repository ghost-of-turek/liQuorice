import asyncio

from gino import Gino
from gino.dialects.base import Pool as GinoPool
from gino.dialects.asyncpg import Pool as AsyncpgPool

from liquorice.core.const import TaskStatus


class MultiLoopPool(GinoPool):
    def __init__(self, url, loop, **kwargs):
        self._url = url
        self._kwargs = kwargs
        self._pools = {}

    def __await__(self):
        async def _get_pool():
            await self._get_pool()
            return self

        return _get_pool().__await__()

    @property
    def raw_pool(self):
        return self._pools[asyncio.get_event_loop()]

    async def acquire(self, *, timeout=None):
        pool = await self._get_pool()
        return await pool.acquire(timeout=timeout)

    async def release(self, conn):
        pool = await self._get_pool()
        await pool.release(conn)

    async def close(self):
        pools = list(self._pools.values())
        self._pools.clear()
        for pool in pools:
            await pool.result().close()

    async def _get_pool(self):
        loop = asyncio.get_event_loop()
        rv = self._pools.get(loop)
        if rv is None:
            rv = self._pools[loop] = asyncio.Future()
            rv.set_result(await AsyncpgPool(self._url, loop, **self._kwargs))
        return await rv

    async def close_for_thread(self):
        await (await self._get_pool()).close()


db = Gino()


class QueuedTask(db.Model):
    __tablename__ = 'queued_tasks'

    id = db.Column(db.Integer(), primary_key=True)
    job = db.Column(db.String(length=100))
    data = db.Column(db.JSON(), default={})
    due_at = db.Column(db.DateTime())
    status = db.Column(db.Enum(TaskStatus))
