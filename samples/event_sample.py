import asyncio
import typing
from asyncio import Event


class Resource:
    def __init__(self):
        self.val = 0


async def set_event(event: Event):
    await asyncio.sleep(1)
    event.set()

    while True:
        await asyncio.sleep(10)


async def worker(r: Resource):
    await asyncio.sleep(0.5)
    r.val += 1

    while True:
        await asyncio.sleep(10)


async def do_until_event(coros: list[typing.Coroutine], event: asyncio.Event):
    for c in coros:
        asyncio.create_task(c)
    """Функция должна обрабатывать Task|Coroutine|Future объекты до тех пор, пока не будет вызван метод Event.set() """
    await event.wait()
