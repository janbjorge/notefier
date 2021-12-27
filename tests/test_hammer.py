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


def work(until: datetime.timedelta = datetime.timedelta(seconds=5)):
    @cache(
        strategy=strategies.Predicate(
            uri="ws://0.0.0.0:4000",
            fn=lambda e: e.operation == "insert",
        )
    )
    def slow1():
        return datetime.datetime.now()

    @cache(
        strategy=strategies.Predicate(
            uri="ws://0.0.0.0:4000",
            fn=lambda e: e.operation == "insert",
        )
    )
    def slow2():
        return datetime.datetime.now()

    while (
        datetime.datetime.now() - slow1() < until and
        datetime.datetime.now() - slow2() < until):
        time.sleep(0.001)


def test():

    N = 2

    with Executor(max_workers=N) as pool:
        jobs = {pool.submit(work) for _ in range(N)}
        for job in as_completed(jobs):
            job.result()


if __name__ == "__main__":
    test()
