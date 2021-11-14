import datetime
import functools
import logging
import typing

from client import (
    WSListener,
)


logging.basicConfig(level=logging.INFO)


def cache(
    uri: str,
    predicate: typing.Callable[[str], bool] = bool,
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
