import argparse
import asyncio
import datetime
import functools
import logging
import typing

import asyncpg

from client import (
    WSListener,
)


logging.basicConfig(level=logging.INFO)


async def emit(channel: str, message: str) -> None:
    connection: asyncpg.Connection = await asyncpg.connect()
    try:
        await connection.fetch("SELECT pg_notify($1, $2)", channel, message)
    finally:
        await connection.close()


def cache(
    uri: str,
    predicate: typing.Callable[[str], bool] = bool,
    min_ttl: datetime.timedelta = datetime.timedelta(seconds=5),
    max_ttl: datetime.timedelta = datetime.timedelta(seconds=10),
):
    # Using cache insted of keeping a local variable,
    # not if this is smart with regards to< performence.
    @functools.cache
    def invalidatior() -> WSListener:
        return WSListener(uri)

    def outer(fn):

        cached = functools.cache(fn)
        cached.updated_at = datetime.datetime.now()

        def inner(*args, **kw):

            # time.time is 4-5 times quicker, (350ns vs. 50ns), not sure
            # if it's worh it or not.
            now = datetime.datetime.now()

            # Clear cache if we have some event from
            # database
            if not invalidatior()._evnts.empty():
                if predicate(invalidatior().get_nowait()):
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


def main() -> None:
    parser = argparse.ArgumentParser(prog="Simple PG-notefy emitter")
    parser.add_argument(
        "--channel",
        "-c",
        type=str,
        help="PG-channel for the servre to lisen to.",
        required=True,
    )
    parser.add_argument(
        "--message",
        "-m",
        type=str,
        help="The message to emit onto the channel.",
        required=True,
    )
    opts = parser.parse_args()
    asyncio.run(emit(opts.channel, opts.message))
