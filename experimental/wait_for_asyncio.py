import asyncio
from zthreading.tasks import wait_for_future


async def ok_future():
    return "all ok"


async def errored_future():
    raise Exception("lama")


print(wait_for_future(ok_future()))
wait_for_future(errored_future())