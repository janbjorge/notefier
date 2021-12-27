import abc
import datetime
import logging
import queue
import time
import typing

from . import (
    listeners,
    models,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class Strategy(typing.Protocol):
    @abc.abstractmethod
    def clear(self) -> bool:
        raise NotImplementedError()


class Predicate(listeners.Threaded, Strategy):
    def __init__(
        self,
        uri: str,
        fn: typing.Callable[[models.Event], bool],
    ) -> None:
        super().__init__(uri)
        self._fn = fn

    def clear(self) -> bool:
        try:
            return self._fn(self.queue.get_nowait())
        except queue.Empty:
            return False


class PredicateTTL(listeners.Threaded, Strategy):
    def __init__(
        self,
        uri: str,
        ttl: datetime.timedelta,
        fn: typing.Callable[[models.Event], bool],
    ) -> None:
        super().__init__(uri)
        self._ttl = ttl
        self._fn = fn
        self._cleard_at = datetime.datetime.now()

    def clear(self) -> bool:
        now = datetime.datetime.now()
        # If TTL expired, should have precedence over a
        # predicate hit.
        if now > self._cleard_at + self._ttl:
            return True

        try:
            return self._fn(self.queue.get_nowait())
        except queue.Empty:
            return False
