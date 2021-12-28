import argparse
import asyncio
import collections
import functools
import json
import logging
import queue
import typing

from collections.abc import Mapping

import asyncpg

from core import (
    listeners,
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

        cached = collections.OrderedDict()
        miss = object()

        @functools.wraps(fn)
        def inner(*args, **kw):
            # Clear cache if we have some event from
            # database
            key = utils.make_key(args, kw)

            if strategy.clear():
                cached.pop(key, None)

            # Check if we have a cached value, if not we use `cache_miss`
            # to singal a cache-miss.
            rv = cached.pop(key, miss)

            if rv is miss:
                rv = fn(*args, **kw)
                if maxsize is not None and len(cached) > maxsize:
                    cached.popitem(last=True)

            # cache + LRU-rotation
            cached[key] = rv

            return rv

        return inner

    return outer


class KV(Mapping):
    def __init__(self, uri):
        self._listener = listeners.Threaded(uri)
        self._kv = dict()

    def _update(self):
        while True:

            try:
                e = self._listener.queue.get_nowait()
            except queue.Empty:
                break

            try:
                kv = json.loads(e.payload)
            except json.decoder.JSONDecodeError:
                logging.exception("Unable to load %s", e.payload)
                continue

            try:
                if e.operation == "insert" or e.operation == "update":
                    self._kv[kv["key"]] = kv["value"]
                elif e.operation == "delete":
                    self._kv.pop(kv["key"], None)
            except Exception:
                logging.exception(
                    "Unable to update KV, Illformed payload, must have key/value: %s",
                    kv,
                )

    def __getitem__(self, key):
        self._update()
        return self._kv[key]

    def __iter__(self):
        self._update()
        return self._kv.__iter__()

    def __len__(self) -> int:
        self._update()
        return self._kv.__len__()


async def emit(
    channel: str,
    operation: str,
    payload: typing.Any,
) -> None:
    payload = json.dumps({"operation": operation, "payload": payload})
    connection: asyncpg.Connection = await asyncpg.connect()
    try:
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
        "--payload",
        "-p",
        default=None,
        help="A payload that you would like to attach to the operation.",
    )
    opts = parser.parse_args()
    asyncio.run(emit(opts.channel, opts.operation, opts.payload))
