import pytest
import asyncio
from time import sleep
from zthreading.tasks import Task
from experimental.asyncio.threading_queue import ThreadingWaiter


def test_threaded_waiter():
    waiter = ThreadingWaiter(is_asyncio=False)

    def release():
        sleep(3)
        waiter.release()

    rtask = Task(release).start()

    waiter.wait(1)


async def test_asyncio_waiter():
    waiter = ThreadingWaiter(is_asyncio=True)

    async def release():
        await asyncio.sleep(4)
        waiter.release()

    asyncio.create_task(release())

    waiter.wait(2)


if __name__ == "__main__":
    # test_threaded_waiter()
    asyncio.run(test_asyncio_waiter())
    # Task(test_asyncio_waiter).start().join()
