import argparse
import asyncio
import logging
import sys
import typing

import asyncpg

# Due to lazy imports in the websockets module we
# have to import using `the real import path`.
# Ref.: https://websockets.readthedocs.io/en/stable/changelog.html?highlight=mypy#id5
from websockets.client import (
    WebSocketClientProtocol,
)
from websockets import (
    exceptions as ws_exceptions,
    server,
)


# Due to lazy imports in the websockets module we have to import using `the real import path`.
# Ref.: https://websockets.readthedocs.io/en/stable/changelog.html?highlight=mypy#id5
from websockets.client import (
    connect as ws_connect,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class Handler:
    async def send_message(self, ws: WebSocketClientProtocol, message: str) -> None:

        try:
            await ws.send(message)
        except ws_exceptions.ConnectionClosedError:
            self.unregister(ws)
        except Exception:
            logging.exception(
                "Got an exception while sending message to %s, removed from clients set.",
                ws,
            )
            self.unregister(ws)

    def register(self, ws: WebSocketClientProtocol) -> None:
        self.clients.add(ws)
        logging.info(f"{ws.remote_address} connected, connections: {len(self.clients)}")

    def unregister(self, ws: WebSocketClientProtocol) -> None:
        logging.info("......., unregister")
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, message: str) -> None:
        await asyncio.gather(*[self.send_message(c, message) for c in self.clients])

    async def ws_handler(
        self,
        ws: WebSocketClientProtocol,
        path: str,
    ):
        self.register(ws)

        # This just looks wrong on so many levels....
        try:
            await suspend_forever()
        finally:
            self.unregister(ws)

    async def emitter(self):
        while True:
            try:
                await self.broadcast(await self.pg_events.get())
            except Exception:
                logging.exception("Was unable to broadcast.")

    def __init__(
        self,
        pg_channel: str,
    ):

        self.clients: typing.Set[WebSocketClientProtocol] = set()

        self.pg_channel: str = pg_channel
        self.pg_connection: typing.Optional[asyncpg.Connection] = None
        self.pg_events: asyncio.Queue[str] = asyncio.Queue()

        self.setup_task = asyncio.create_task(self.async_init())
        self.emitter_task = asyncio.create_task(self.emitter())

    def put(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ):
        logging.info("channel: %s, payload: %s", channel, payload)
        if channel == self.pg_channel:
            self.pg_events.put_nowait(payload)

    async def async_init(self):

        # Setup pg event listner
        try:
            self.pg_connection = await asyncpg.connect()
        except Exception:
            logging.exception("Was unable to connect to the database.")
            sys.exit(1)

        try:
            await self.pg_connection.add_listener(
                self.pg_channel,
                self.put,
            )
        except Exception:
            logging.exception("Was unablt to setup the pg listener, while ")
            sys.exit(1)


async def suspend_forever() -> None:
    await asyncio.sleep(float("inf"))


async def run(channel: str, host: str, port: int) -> None:
    h = Handler(channel)
    await server.serve(h.ws_handler, host=host, port=port)
    await suspend_forever()


def main() -> None:
    parser = argparse.ArgumentParser(prog="PGCache")
    parser.add_argument(
        "--channel",
        type=str,
        help="PG-channel for the servre to lisen to.",
        required=True,
    )
    parser.add_argument(
        "--host",
        type=str,
        help="The network interfaces the server is bound to.",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="TCP port the server listens on.",
        default=4000,
    )

    opts = parser.parse_args()
    asyncio.run(run(channel=opts.channel, host=opts.host, port=opts.port))
