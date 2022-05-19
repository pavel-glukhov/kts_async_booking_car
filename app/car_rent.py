import asyncio
from asyncio import Queue, Event, Semaphore
from collections import defaultdict
from typing import Optional, Any, Dict, Set

from app.const import MAX_PARALLEL_AGG_REQUESTS_COUNT, WORKERS_COUNT


class PipelineContext:
    def __init__(self, user_id: int, data: Optional[Any] = None):
        self._user_id = user_id
        self.data = data

    @property
    def user_id(self):
        return self._user_id


CURRENT_AGG_REQUESTS_COUNT = 0
BOOKED_CARS: Dict[int, Set[str]] = defaultdict(set)


def clear_booked_cars():
    BOOKED_CARS.clear()


async def get_offers(source: str) -> list[dict]:
    await asyncio.sleep(1)

    return [
        {"url": f"http://{source}/car?id=1", "price": 1_000, "brand": "LADA"},
        {"url": f"http://{source}/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
        {"url": f"http://{source}/car?id=3", "price": 3_000, "brand": "KIA"},
        {"url": f"http://{source}/car?id=4", "price": 2_000, "brand": "DAEWOO"},
        {"url": f"http://{source}/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]


async def get_offers_from_sourses(sources: list[str]) -> list[dict]:
    global CURRENT_AGG_REQUESTS_COUNT

    if CURRENT_AGG_REQUESTS_COUNT >= MAX_PARALLEL_AGG_REQUESTS_COUNT:
        await asyncio.sleep(10.0)

    CURRENT_AGG_REQUESTS_COUNT += 1
    responses = await asyncio.gather(*[get_offers(source) for source in sources])
    CURRENT_AGG_REQUESTS_COUNT -= 1

    out = list()
    for r in responses:
        out.extend(r)
    return out


async def worker_combine_service_offers(
        inbound: Queue, outbound: Queue, sem: Semaphore
):
    chink: list
    while True:
        ctx: PipelineContext = await inbound.get()

        await sem.acquire()
        ctx.data = await get_offers_from_sourses(ctx.data)
        sem.release()

        await outbound.put(ctx)


async def chain_combine_service_offers(
        inbound: Queue, outbound: Queue, **kw
):
    sem = Semaphore(MAX_PARALLEL_AGG_REQUESTS_COUNT)
    await asyncio.gather(
        asyncio.create_task(
            worker_combine_service_offers(inbound, outbound, sem))
        for _ in range(WORKERS_COUNT)
    )


async def chain_filter_offers(
        inbound: Queue,
        outbound: Queue,
        brand: Optional[str] = None,
        price: Optional[int] = None,
        **kw,
):
    while True:
        ctx = await inbound.get()
        ctx.data = [
            o
            for o in ctx.data
            if (brand is None or o.get("brand") == brand)
               and (price is None or o.get("price") <= price)

        ]

        await outbound.put(ctx)


async def cancel_book_request(user_id: int, offer: dict):
    await asyncio.sleep(1)
    BOOKED_CARS[user_id].remove(offer.get("url"))


async def book_request(user_id: int, offer: dict, event: Event) -> dict:
    try:
        BOOKED_CARS[user_id].add(offer.get("url"))
        await asyncio.sleep(1)
        if event.is_set():
            event.clear()
        else:
            await event.wait()

        return offer
    except:
        await cancel_book_request(user_id, offer)


async def worker_book_car(inbound: Queue[PipelineContext],
                          outbound: Queue[PipelineContext]):
    while True:
        ctx = await inbound.get()
        event = Event()
        event.set()
        done, pending = await asyncio.wait(
            [book_request(ctx.user_id, offer, event)
             for offer in ctx.data],
            return_when=asyncio.FIRST_COMPLETED
        )

        for d in done:
            ctx.data = d.result()

        pending: list[asyncio.Task]
        for p in pending:
            p: asyncio.Task
            p.cancel()

        await asyncio.gather(*pending)

        await outbound.put(ctx)


async def chain_book_car(inbound: Queue[PipelineContext],
                         outbound: Queue[PipelineContext], **kw
                         ):
    await asyncio.gather(
        asyncio.create_task(
            worker_book_car(inbound, outbound))
        for _ in range(WORKERS_COUNT)
    )


def run_pipeline(inbound: Queue[PipelineContext]) -> Queue[PipelineContext]:
    queue_combine_service = Queue()
    queue_filter_offers = Queue()
    queue_book_car = Queue()

    asyncio.create_task(chain_combine_service_offers(inbound, queue_combine_service))
    asyncio.create_task(chain_filter_offers(queue_combine_service, queue_filter_offers))
    asyncio.create_task(chain_book_car(queue_filter_offers, queue_book_car))

    return queue_book_car
