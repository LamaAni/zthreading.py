import asyncio
from datetime import datetime
from asgiref.sync import async_to_sync
from zthreading.tasks import Task
import itertools
import unsync


def gen_1():
    yield "lama"


g1 = gen_1()
g1.__next__()
g1.__next__()

raise "Done"
loop = asyncio.get_event_loop()
lock = asyncio.Lock()
event = asyncio.Event()

started = datetime.now()


class YieldGen:
    def __init__(self) -> None:
        self.is_waiting = True
        self.pending = []
        pass

    def __next__(self):
        pass


async def release():
    await asyncio.sleep(2)
    lock.release()


async def wait_for_lock():
    # await lock.acquire()
    await asyncio.sleep(2)
    lock_acquire = lock.acquire()
    waiter = asyncio.wait_for(lock_acquire, timeout=1)
    await waiter
    print(f"TS: {(datetime.now() - started).total_seconds()}")
    lock.release()


async def main():
    loop.create_task(release())
    await wait_for_lock()


async def wrap_iter(it):
    async for v in it:
        yield v


async def call_main_in_loop():
    def call_me():
        awaitable = main()
        task = loop.create_task(main())
        yield from event

    list(call_me())


loop.run_until_complete(call_main_in_loop())