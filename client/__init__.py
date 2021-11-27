import asyncio
import contextlib
import logging
import queue
import threading
import typing

# Due to lazy imports in the websockets module we have to import using `the real import path`.
# Ref.: https://websockets.readthedocs.io/en/stable/changelog.html?highlight=mypy#id5
from websockets import (
    exceptions as ws_exceptions,
)
from websockets.client import (
    connect as ws_connect,
)

logging.basicConfig(level=logging.INFO)


class AsyncWSListener(asyncio.Queue):
    def __init__(self, uri: str):
        super().__init__()
        self._uri = uri
        self._client = asyncio.create_task(self.client())

    async def client(self):
        try:
            async for ws in ws_connect(uri=self._uri):
                # Might need todo something bether here, for now
                # it seems to work ok.
                with contextlib.suppress(ws_exceptions.ConnectionClosed):
                    while True:
                        await self.put(await ws.recv())
        except Exception:
            logging.exception(
                "Connection to `%s` was lost, trying to reconnect.", self._uri
            )


class ThreadedWSListener:

    threads: typing.Dict[str, threading.Thread] = {}

    def __init__(self, uri: str):
        self._uri = uri
        self._queue: queue.Queue[str] = queue.Queue()

        if self._uri not in ThreadedWSListener.threads:
            ThreadedWSListener.threads[self._uri] = threading.Thread(
                target=ThreadedWSListener._ws,
                args=(self._queue, self._uri),
                daemon=True,
            )
            ThreadedWSListener.threads[self._uri].start()

    @staticmethod
    def _ws(q: queue.Queue, uri: str) -> None:
        try:
            # This is a MUST, we don't want this event loop to be
            # as isolated as we can.
            loop = asyncio.new_event_loop()
            async def run():
                listener = AsyncWSListener(uri)
                while True:
                    q.put(await listener.get())
            loop.run_until_complete(run())
        except Exception:
            logging.exception("Was unable to setup threaded WS Listener")
            raise

    def empty(self) -> bool:
        return self._queue.empty()

    def get_nowait(self) -> str:
        return self._queue.get_nowait()
