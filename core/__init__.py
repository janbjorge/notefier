import argparse
import asyncio
import logging

import asyncpg

logging.basicConfig(level=logging.INFO)


async def emit(channel: str, message: str) -> None:
    connection: asyncpg.Connection = await asyncpg.connect()
    try:
        await connection.fetch("SELECT pg_notify($1, $2)", channel, message)
    except Exception:
        logging.exception("Was unable to emit %s to %s", message, channel)
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
        "--message",
        "-m",
        type=str,
        help="The message to be emitted onto the channel.",
        required=True,
    )
    opts = parser.parse_args()
    asyncio.run(emit(opts.channel, opts.message))
