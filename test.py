import asyncio
import datetime
import time

import client


@client.cache("ws://localhost:4000", predicate=bool)
def slow():
    time.sleep(1)
    return datetime.datetime.now()


async def main():
    for _ in range(1_000_000):
        print(slow())
        await asyncio.sleep(0.1)
    # c = client.WSListener("ws://localhost:4000")
    # await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
