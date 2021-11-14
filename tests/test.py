import asyncio
import datetime
import time

import core


@core.cache("ws://localhost:4000", predicate=bool)
def slow():
    time.sleep(1)
    return datetime.datetime.now()


async def main():
    for _ in range(1_000_000):
        print(slow())
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
