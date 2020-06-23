import pytest
import time
import asyncio
from zthreading import tasks
from zthreading.tasks import Task
from concurrent.futures import TimeoutError

tasks.Task.THROW_INTERNAL_EXCEPTIONS_DEFAULT = False


class DummyTestError(Exception):
    pass


def test_corutine_wait():
    async def coro():
        return "test"

    assert tasks.wait_for_future(coro()) == "test"


def test_corutine_wait_with_error():
    async def coro():
        raise DummyTestError("test")

    with pytest.raises(DummyTestError):
        assert tasks.wait_for_future(coro()) == "test"


def test_corutine_wait_with_timeout():
    async def coro():
        await asyncio.sleep(1000)

    with pytest.raises(TimeoutError):
        tasks.wait_for_future(coro(), timeout=0.01)


def test_already_running():
    async def coro():
        await asyncio.sleep(0.1)
        return "test"

    task1 = Task(coro, use_async_loop=True)
    task2 = Task(coro, use_async_loop=True)

    def start_task(task):
        task.start()

    main_task_1 = Task(start_task, use_async_loop=False).start(task1)
    main_task_2 = Task(start_task, use_async_loop=False).start(task2)
    time.sleep(0.01)
    task1.join()
    task2.join()
    main_task_1.join()
    main_task_2.join()


def test_task_use_async_loop():
    data = {
        "started": False,
        "complete": False,
    }

    async def run_async():
        data["started"] = True
        await asyncio.sleep(0.1)
        data["complete"] = True

    task = tasks.Task(run_async, use_async_loop=True).start()
    assert data["complete"] is not True, "invalid asyncio wait for completion"
    task.join()
    assert data["complete"] is True, "Task did not properly join"


def test_task_use_thread():
    data = {
        "started": False,
        "complete": False,
    }

    async def run_async():
        data["started"] = True
        time.sleep(0.1)
        data["complete"] = True

    task = tasks.Task(run_async, False).start()
    assert data["complete"] is not True, "invalid thread wait for completion"
    task.join()
    assert data["complete"] is True, "Task did not properly join"


def test_return_value_using_thread():
    def return_something():
        return "test"

    assert Task(return_something).start().join() == "test"


def test_return_value_using_asyncio():
    async def return_something():
        return "test"

    assert Task(return_something, use_async_loop=True).start().join() == "test"


def raise_task_exception():
    raise DummyTestError("exception from " + tasks.Task.get_thread_description())


def test_common_threaded_task_with_join_exception():
    """Test for thread exception with join.
    """
    with pytest.raises(DummyTestError):
        task = tasks.Task(raise_task_exception)
        task.start()
        task.join()


class dummytester:
    def __init__(self, throw_error=False):
        super().__init__()
        self.collected_count = 0
        self.throw_error = throw_error
        self.last_error = None

    @tasks.collect_consecutive_calls_async("async_error")
    def collected(self):
        time.sleep(0.01)
        if self.throw_error:
            raise Exception("test")
        self.collected_count += 1

    def async_error(self, err):
        self.last_error = err


def test_collect_consecutive_calls_async():
    tester = dummytester()
    for i in range(0, 100):
        tester.collected()
    time.sleep(0.1)
    assert tester.collected_count == 2


def test_collect_consecutive_calls_async_exception():
    tester = dummytester(True)
    tester.collected()

    time.sleep(0.1)

    assert tester.last_error is not None


if __name__ == "__main__":
    pytest.main(["-x", __file__])