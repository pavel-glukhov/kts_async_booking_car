import asyncio
from asyncio import Event

import pytest

from samples.event_sample import Resource, worker, set_event, do_until_event

pytestmark = pytest.mark.asyncio


async def test():
    stop_event = Event()
    res = Resource()

    coros = [worker(res) for i in range(10)]
    coros.append(set_event(stop_event))

    # Проверяем, что короутина вернет управление раньше, чем 1.5 c.
    await asyncio.wait_for(
        do_until_event(coros, event=stop_event),
        timeout=1.5,
    )

    # Проверяем, что все worker-ы успели совершить работу
    assert res.val == 10

    # Проверяем, что все Event действительно проставлен
    assert stop_event.is_set() is True
