import argparse
import asyncio
import functools
import json
import logging
import typing

import asyncpg

from core import (
    strategies,
)

logging.basicConfig(level=logging.INFO)


def cache(
    strategy: strategies.Strategy,
):
    def outer(fn):

        cached = functools.cache(fn)

        def inner(*args, **kw):

            # Clear cache if we have some event from
            # database
            if strategy.cache_clear():
                cached.cache_clear()

            return cached(*args, **kw)

        inner.cache_clear = cached.cache_clear
        inner.cache_info = cached.cache_info
        inner.strategy = strategy

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
        "--repeat", "-r", type=int, help="Repeat the given message N times", default=1
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
