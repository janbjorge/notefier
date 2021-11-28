from concurrent.futures import (
    ProcessPoolExecutor as Executor,
    as_completed,
)
import datetime
import time

from core import (
    cache,
    strategies,
)


def work(until: datetime.timedelta = datetime.timedelta(seconds=10)):

    strategy = strategies.Threaded(
        uri="ws://localhost:4000",
        predicate=lambda e: e.operation == "insert",
    )

    @cache(strategy=strategy)
    def slow():
        return datetime.datetime.now()

    while datetime.datetime.now() - slow() < until:
        time.sleep(0.01)


def test():

    N = 2

    with Executor(max_workers=N) as pool:
        jobs = {pool.submit(work) for _ in range(N)}
        for job in as_completed(jobs):
            job.result()


if __name__ == "__main__":
    test()
