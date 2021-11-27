import datetime
import functools
import logging
import typing

from client import (
    ThreadedWSListener,
)


logging.basicConfig(level=logging.INFO)


def cache(
    uri: str,
    predicate: typing.Callable[[str], bool] = bool,
    min_ttl: datetime.timedelta = datetime.timedelta(seconds=5),
    max_ttl: datetime.timedelta = datetime.timedelta(seconds=60),
):
    # Using cache insted of keeping a local variable,
    # not if this is smart with regards to< performence.
    @functools.cache
    def invalidatior() -> ThreadedWSListener:
        return ThreadedWSListener(uri)

    def outer(fn):

        cached = functools.cache(fn)
        cached.updated_at = datetime.datetime.now()

        def inner(*args, **kw):

            # time.time is 4-5 times quicker, (350ns vs. 50ns), not sure
            # if it's worh it or not.
            now = datetime.datetime.now()

            # Clear cache if we have some event from
            # database
            if not invalidatior().empty():
                change = invalidatior().get_nowait()
                if predicate(change):
                    if now > cached.updated_at + min_ttl:
                        logging.info("Clearing cache - event")
                        cached.cache_clear()
                        cached.updated_at = now

            # Hmmmmmm?
            elif now > max_ttl + cached.updated_at:
                logging.info("Clearing cache - ttl expired")
                cached.cache_clear()
                cached.updated_at = now

            return cached(*args, **kw)

        inner.cache_clear = cached.cache_clear
        inner.cache_info = cached.cache_info
        inner.invalidatior = invalidatior

        return inner

    return outer
