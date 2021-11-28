import datetime
import time


from core import (
    cache,
    strategies,
)


@cache(
    strategies.Threaded(
        "ws://localhost:4000", predicate=lambda e: e.operation == "insert"
    )
)
def slow():
    return datetime.datetime.now()


def main():
    while True:
        print(slow())
        time.sleep(0.1)


if __name__ == "__main__":
    main()
