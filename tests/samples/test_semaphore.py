import asyncio
from asyncio import Semaphore

import pytest

from samples.semaphore_sample import Resource, do_request

pytestmark = pytest.mark.asyncio


async def test():
    res = Resource()
    sem = Semaphore(5)

    await asyncio.wait_for(
        asyncio.gather(*[do_request(res, sem) for _ in range(10)]),
        timeout=0.3,
    )
