import asyncio
import dataclasses
import datetime
import json
import logging
import queue
import threading
import time
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


@dataclasses.dataclass(frozen=True)
class Event:
    operation: typing.Literal["insert", "update", "delete"]
    extra: typing.Optional[typing.Any]
    received_at: datetime.datetime = dataclasses.field(
        default_factory=datetime.datetime.now
    )

    @staticmethod
    def load(payload: str) -> "Event":
        p = json.loads(payload)
        return Event(
            operation=p["operation"],
            extra=p.get("extra"),
        )


class Strategy:
    def __init__(
        self,
        predicate: typing.Callable[[Event], bool],
        min_ttl: datetime.timedelta,
        max_ttl: datetime.timedelta,
    ) -> None:
        self._queue: queue.Queue[Event] = queue.Queue()
        self._predicate = predicate
        self._cleard_at = datetime.datetime.now()
        self._min_ttl = min_ttl
        self._max_ttl = max_ttl

    def any_cache_clear_hit(
        self,
        t_cutuff=0.05,
        n_cutoff=100,
    ) -> bool:
        """
        Walks the queue for 100 elemets or 50ms
        """

        cnt = 0
        enter = time.time()
        hit = False

        while not self._queue.empty():

            if self._predicate(self._queue.get_nowait()):
                hit = True

            cnt += 1
            if cnt > n_cutoff:
                break

            if time.time() - enter > t_cutuff:
                break

        return hit

    def cache_clear(self) -> bool:

        now = datetime.datetime.now()
        dt = now - self._cleard_at

        if dt > self._max_ttl:
            self._cleard_at = now
            return True

        if dt > self._min_ttl:
            if self.any_cache_clear_hit():
                return True

        return False


class Threaded(Strategy):
    def __init__(
        self,
        uri: str,
        predicate: typing.Callable[[Event], bool],
        min_ttl: datetime.timedelta = datetime.timedelta(seconds=5),
        max_ttl: datetime.timedelta = datetime.timedelta(seconds=60),
    ):
        super().__init__(predicate=predicate, min_ttl=min_ttl, max_ttl=max_ttl)

        self._background_listener = threading.Thread(
            target=Threaded._listener,
            args=(uri, self._queue),
            daemon=True,
        )
        self._background_listener.start()

    @staticmethod
    async def __listener(uri: str, q: queue.Queue) -> None:
        async for ws in ws_connect(uri=uri):
            # Might need todo something bether here, for now
            # it seems to work ok.
            async for msg in ws:
                q.put(Event.load(msg))
            # while True:
            #     try:
            #         new = await ws.recv()
            #         q.put(Event.load(new))
            #     except (KeyError, json.decoder.JSONDecodeError):
            #         logging.exception("Was unable to queue: %s", new)
            #     except ws_exceptions.ConnectionClosed:
            #         break

    @staticmethod
    def _listener(uri: str, q: queue.Queue) -> None:
        # This is a MUST, we don't want this event loop are attached
        # to PID's. We do not want any interference from
        loop = asyncio.new_event_loop()
        loop.run_until_complete(Threaded.__listener(uri, q))
