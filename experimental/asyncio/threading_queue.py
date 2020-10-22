import threading
import asyncio

from datetime import timedelta, datetime
from typing import Union, Callable, Set
from weakref import WeakSet

from zthreading.exceptions import TheradingWaiterException, ThreadingTimeoutError
from zthreading.tasks import Task


def run_or_wait(future: asyncio.Future, loop: asyncio.AbstractEventLoop):
    if not loop.is_running():
        loop.run_until_complete(future)
    else:
        list(future.__await__())


class ThreadingWaiter:
    def __init__(self, predict: Callable = None, is_asyncio=None) -> None:
        assert predict is None or isinstance(predict, Callable) and not isinstance(predict, asyncio.Future), ValueError(
            "predict cannot be a coroutine or a future and must be a callable method or None"
        )

        if is_asyncio is None:
            try:
                asyncio.get_running_loop()
                is_asyncio = True
            except RuntimeError:
                is_asyncio = False

        # Create the lock just in the case where its needed.
        self._lock: threading.Lock = None
        self._is_asyncio = is_asyncio is True
        self._is_released = False
        self.predict = predict

        self._initialize()

    def _initialize(self):
        if self._is_asyncio:
            self._lock = asyncio.Lock()
        else:
            self._lock = threading.Lock()

    @property
    def is_released(self) -> bool:
        return self._is_released

    @property
    def is_pending(self) -> bool:
        return not self._is_released

    def reset(self):
        self._is_released = False

    def wait(self, timeout: Union[timedelta, float] = None):
        if self._is_released:
            # Nothing to do.
            return

        timeout = timeout if not isinstance(timeout, timedelta) else timeout.total_seconds()
        if self._is_asyncio:
            self._wait_asyncio(timeout=timeout)
        else:
            self._wait_threaded(timeout=timeout)

    def _wait_asyncio(self, timeout: float = None):
        # Since we are within a corutine
        loop = asyncio.get_event_loop()

        async def do_wait():
            # crate a lock for the wait event.
            await self._lock.acquire()

            try:
                # will wait until can acquire again.
                await asyncio.wait_for(self._lock.acquire(), timeout=timeout, loop=loop)
            finally:
                # release the (new) lock.
                self._lock.release()

        run_or_wait(do_wait(), loop=loop)

    def _wait_threaded(self, timeout: float = None):
        # crate a lock for the wait event.
        self._lock.acquire(blocking=False)

        try:
            self._lock.acquire(blocking=True, timeout=timeout)
        finally:
            self._lock.release()

    def release(self, throw_error_if_predict_fails=False):
        if self._is_released:
            return

        if self.predict is not None and not self.predict():
            if throw_error_if_predict_fails:
                raise TheradingWaiterException("Release predict method failed. Could not release threading lock")
            return
        self._is_released = True
        self._lock.release()


class ThreadingQueue(object):
    def __init__(self) -> None:
        # a collection of data values.
        self._queue = []
        self._pending_waits: Set[ThreadingWaiter] = WeakSet()

    # Lock operations
    def _check_locks(self):
        for info in self._pending_waits:
            if info.predict():
                info

    # Queue operations
    def enqueue(self, val):
        self._queue.append(val)

    def dequeue(self, val, timeout: Union[timedelta, float] = None):
        pass

    def insert(self, index: int, val):
        pass

    def clear(self):
        pass

    def wait_for(self, predict: Callable = lambda queue: True, timeout: Union[timedelta, float] = None):
        wait_info = ThreadingWaiter(lambda: predict(self))
        self._pending_waits.add(wait_info)

        timeout = timeout if not isinstance(timeout, timedelta) else timeout.total_seconds()

        loop = asyncio.get_event_loop()
        if loop is not None and loop.is_running():
            # Case needs asyncio timeout.
            pass
        else:
            # case needs threading timeout.
            wait_info.wait_condition.wait(timeout=timeout)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)
    waiter = ThreadingWaiter()

    async def main_loop():
        logging.info("sleeping")
        await asyncio.sleep(2)
        logging.info("release")
        waiter.release()

    waiter.wait(4)