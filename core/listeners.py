import asyncio
import json
import logging
import queue
import threading
import typing

# Due to lazy imports in the websockets module we have to import using `the real import path`.
# Ref.: https://websockets.readthedocs.io/en/stable/changelog.html?highlight=mypy#id5
from websockets.client import (
    connect as ws_connect,
)

from . import (
    models,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class Threaded:

    _BACKGROUND_LISTENER: typing.Optional[threading.Thread] = None
    _QUEUES: typing.List[queue.Queue[models.Event]] = []

    def __init__(self, uri: str):

        if Threaded._BACKGROUND_LISTENER is None:
            Threaded._BACKGROUND_LISTENER = threading.Thread(
                target=Threaded._listener,
                args=(
                    uri,
                    Threaded._QUEUES,
                ),
                daemon=True,
            )
            Threaded._BACKGROUND_LISTENER.start()

        self.queue: queue.Queue[models.Event] = queue.Queue()
        Threaded._QUEUES.append(self.queue)

    @staticmethod
    def _listener(uri: str, qs: typing.List[queue.Queue[models.Event]]) -> None:
        async def __listener(
            uri: str, qs: typing.List[queue.Queue[models.Event]]
        ) -> None:
            while True:
                try:
                    async for ws in ws_connect(uri=uri):
                        # Might need todo something bether here, for now
                        # it seems to work ok.
                        async for msg in ws:

                            try:
                                e = models.Event.load(msg)
                            except (json.decoder.JSONDecodeError, KeyError):
                                logging.exception("Unable to load/queue: %s", msg)
                            else:
                                for q in qs.copy():
                                    q.put(e)

                except Exception:
                    logging.exception(
                        "Was unable to establish a connection with %s", uri
                    )

        # This is a MUST, we don't want this event loop are attached
        # to PID's. We do not want any interference from
        loop = asyncio.new_event_loop()
        loop.run_until_complete(__listener(uri, qs))
