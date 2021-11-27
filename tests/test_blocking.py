import asyncio
import datetime
import time


from core import (
    blocking,
)


@blocking.cache("ws://localhost:4000", predicate=bool)
def slow():
    time.sleep(1)
    return datetime.datetime.now()


def main():
    for _ in range(1_000_000):
        print(slow())
        time.sleep(0.25)


if __name__ == "__main__":
    asyncio.run(main())
