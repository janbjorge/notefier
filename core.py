import asyncpg


async def asyncpg_connect() -> asyncpg.Connection:
    return await asyncpg.connect()
