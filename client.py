import asyncio
import contextlib
import datetime
import functools
import logging
import typing as T


# Due to lazy imports in the websockets module we have to import using `the real import path`.
# Ref.: https://websockets.readthedocs.io/en/stable/changelog.html?highlight=mypy#id5
from websockets import (
    exceptions as ws_exceptions,
)
from websockets.client import (
    connect as ws_connect,
)

logging.basicConfig(level=logging.INFO)


class WSListener:
    def __init__(self, uri: str):
        self._uri = uri
        self._evnts: asyncio.Queue[str] = asyncio.Queue()
        self._client = asyncio.create_task(self.client())

    async def client(self):
        try:
            async for ws in ws_connect(uri=self._uri):
                # Might need todo something bether here, for now
                # it seems to work ok.
                with contextlib.suppress(ws_exceptions.ConnectionClosed):
                    while True:
                        await self._evnts.put(await ws.recv())
        except Exception:
            logging.exception("Connection to `%s` was lost, trying to reconnect.", self._uri)

    async def get(self) -> str:
        return await self._evnts.get()

    def get_nowait(self) -> str:
        return self._evnts.get_nowait()


def cache(
    uri: str,
    predicate: T.Callable[[str], bool] = bool,
    min_ttl: datetime.timedelta = datetime.timedelta(seconds=5),
    max_ttl: datetime.timedelta = datetime.timedelta(seconds=60),
):
    @functools.cache
    def invalidatior() -> WSListener:
        return WSListener(uri)

    def outer(fn):

        cached = functools.cache(fn)
        cached.updated_at = datetime.datetime.now()

        def inner(*args, **kw):
            # Clear cache if we have an event from
            # the database that tells us there is
            # some chance.
            now = datetime.datetime.now()

            if not invalidatior()._evnts.empty():
                if predicate(invalidatior()._evnts.get_nowait()):
                    if now > cached.updated_at + min_ttl:
                        logging.info("Clearing cache")
                        cached.cache_clear()
                        cached.updated_at = now

            elif now > max_ttl + cached.updated_at:
                cached.cache_clear()
                cached.updated_at = now

            return cached(*args, **kw)

        inner.cache_clear = cached.cache_clear
        inner.cache_info = cached.cache_info
        inner.invalidatior = invalidatior

        return inner

    return outer
