import functools


def make_key(args, kwds, typed: bool = False):
    return functools._make_key(args, kwds, typed)
