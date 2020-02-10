import asyncio
import datetime
from typing import Optional

from gino import Gino as BaseGino
from gino.dialects.base import Pool as GinoPool
from gino.dialects.asyncpg import Pool as AsyncpgPool
from sqlalchemy import and_

from liquorice.core.const import TaskStatus


class Gino(BaseGino):
    def set_bind(self, bind, loop=None, **kwargs):
        kwargs['pool_class'] = MultiLoopPool
        return super().set_bind(bind, loop=loop, **kwargs)

    def with_bind(self, bind, loop=None, **kwargs):
        kwargs['pool_class'] = MultiLoopPool
        return super().with_bind(bind, loop=loop, **kwargs)

    async def close_for_current_loop(self):
        await self.bind._pool.close_for_current_loop()


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
            await pool.close()

    async def _get_pool(self):
        loop = asyncio.get_event_loop()
        if loop not in self._pools:
            self._pools[loop] = await AsyncpgPool(
                self._url, loop, **self._kwargs,
            )
        return self._pools[loop]

    async def close_for_current_loop(self):
        await (await self._get_pool()).close()


db = Gino()


class QueuedTask(db.Model):
    __tablename__ = 'queued_tasks'

    id = db.Column(db.Integer(), primary_key=True)
    job = db.Column(db.String(length=100))
    data = db.Column(db.JSON(), default={})
    due_at = db.Column(db.DateTime())
    status = db.Column(db.Enum(TaskStatus))
    result = db.Column(db.JSON(), default='')

    @classmethod
    async def pull(cls) -> Optional['QueuedTask']:
        candidates = cls.select('id').where(
            and_(
                cls.status == TaskStatus.NEW,
                cls.due_at <= datetime.datetime.now(),
            ),
        ).order_by('due_at').limit(1).cte('candidates')
        query = cls.update.values(status=TaskStatus.PROCESSING).where(
            and_(
                cls.id == (
                    cls.select('id').select_from(candidates).limit(1),
                ),
                cls.status == TaskStatus.NEW,
            ),
        ).returning(*cls)
        print(str(query))
        async with db.transaction():
            return await query.gino.one_or_none()

    async def apply(self) -> None:
        await self.update(status=self.status, result=self.result).apply()
