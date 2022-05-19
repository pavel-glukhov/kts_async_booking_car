from asyncio import Lock, gather, create_task, wait_for

import pytest

from samples.lock_sample import Resource, worker

pytestmark = pytest.mark.asyncio


async def test():
    """
    Тест проверяет использование asyncio.Lock, все воркеры должны успеть отработать меньше, чем за 0.5 с.
    """

    res = Resource()
    lock = Lock()
    coros = [
        worker(res, lock),
        worker(res, lock),
        worker(res, lock),
        worker(res, lock),
    ]

    await wait_for(
        gather(*[create_task(c) for c in coros]),
        timeout=0.5,
    )
