import asyncio
from asyncio import Semaphore


class Resource:
    def __init__(self):
        self._users_count = 0

    async def use(self):
        self._users_count += 1

        if self._users_count > 5:
            await asyncio.sleep(5)
        else:
            await asyncio.sleep(0.1)

        self._users_count -= 1


async def do_request(res: Resource, sem: Semaphore):
    """Используя Semaphore, ограничить доступ к Resource и избежать длительного сна"""
    async with sem:
        await res.use()

