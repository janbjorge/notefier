import argparse
import asyncio
import collections
import functools
import json
import logging
import typing

import asyncpg

from core import (
    strategies,
    utils,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def cache(
    strategy: strategies.Strategy,
    maxsize: typing.Optional[int] = 128,
):
    def outer(fn):

        if maxsize is not None and maxsize < 1:
            raise ValueError("The maxsize must be a positive number, greather than 1.")

        # cached = collections.OrderedDict()
        cached = dict()

        @functools.wraps(fn)
        def inner(*args, **kw):
            # Clear cache if we have some event from
            # database
            key = utils.make_key(args, kw)

            if strategy.clear() and key in cached:
                print("clear")
                cached.pop(key, None)

            # if maxsize is not None and len(cached) > maxsize:
            #     logging.info("maxsize")
            #     cached.popitem(last=True)

            # Use cached value if we got one.
            if key not in cached:
                print("miss")
                rv = cached[key] = fn(*args, **kw)
            else:
                # print("hit")
                rv = cached[key]
            return rv
            # if key in cached:
            #     # logging.info("hit")
            #     return cached[key]
            #     # # Move item to top of lru-cache "stack".
            #     # if maxsize is not None:
            #     #     logging.info("lru-rotate")
            #     #     rv = cached.pop(key)
            #     #     cached[key] = rv
            #     # return rv

            # return rv

        return inner

    return outer


async def emit(
    channel: str,
    operation: str,
    extra: typing.Optional[typing.Any],
    repeat: int,
) -> None:
    payload = json.dumps(
        {
            "operation": operation,
            "extra": extra,
        }
    )
    connection: asyncpg.Connection = await asyncpg.connect()
    try:
        for _ in range(repeat):
            await connection.fetch("SELECT pg_notify($1, $2)", channel, payload)
    except Exception:
        logging.exception("Was unable to emit %s to %s", payload, channel)
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="Simple PG-notefy emitter")
    parser.add_argument(
        "--channel",
        "-c",
        type=str,
        help="Destination chanel for the emitted event.",
        required=True,
    )
    parser.add_argument(
        "--operation",
        "-o",
        choices=("insert", "update", "delete"),
        help="The database operation to be emitted onto the channel.",
        required=True,
    )
    parser.add_argument(
        "--extra",
        "-e",
        default=None,
        help="Any other data that you would like to attach to the",
    )
    parser.add_argument(
        "--repeat",
        "-r",
        type=int,
        help="Repeat the given message N times",
        default=1,
    )
    opts = parser.parse_args()
    asyncio.run(
        emit(
            opts.channel,
            opts.operation,
            opts.extra,
            opts.repeat,
        )
    )
