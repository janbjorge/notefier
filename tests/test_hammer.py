from concurrent.futures import (
    ProcessPoolExecutor as Executor,
    as_completed,
)
import datetime

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
    def slow():
        return datetime.datetime.now()

    while datetime.datetime.now() - slow() < until:
        pass


def test():

    N = 2

    with Executor(max_workers=N) as pool:
        jobs = {pool.submit(work) for _ in range(N)}
        for job in as_completed(jobs):
            job.result()


if __name__ == "__main__":
    test()
