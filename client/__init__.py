import asyncio
import contextlib
import logging


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
            logging.exception(
                "Connection to `%s` was lost, trying to reconnect.", self._uri
            )

    async def get(self) -> str:
        return await self._evnts.get()

    def get_nowait(self) -> str:
        return self._evnts.get_nowait()
