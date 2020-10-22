import pytest
import time
import asyncio
from zthreading import tasks, decorators
from zthreading.tasks import Task
from asyncio import TimeoutError

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


def test_wait_for_one():
    def coro(t):
        time.sleep(t)
        return "test"

    task1 = Task(coro).start(0.1)
    task2 = Task(coro).start(0.2)

    Task.wait_for_one([task1, task2], timeout=1)
    assert not task1.is_running and task2.is_running, "Failed to wait for task"


def test_wait_for_one_with_error():
    def raise_error():
        # time.sleep(0.1)
        raise DummyTestError()

    task1 = Task(raise_error).start()
    with pytest.raises(DummyTestError):
        Task.wait_for_one([task1], timeout=1)


def test_wait_for_one_with_error_and_completed():
    def raise_error():
        raise DummyTestError()

    task1 = Task(raise_error).start()
    time.sleep(0.1)
    with pytest.raises(DummyTestError):
        Task.wait_for_one([task1], timeout=1)


def test_wait_for_some():
    def coro(t):
        time.sleep(t)
        return "test"

    task1 = Task(coro).start(0.1)
    task2 = Task(coro).start(0.2)
    task3 = Task(coro).start(0.3)

    Task.wait_for_some([task1, task2, task3], timeout=1, wait_count=2)
    assert not task1.is_running and not task2.is_running and task3.is_running, "Failed to wait for task"


def test_wait_for_all():
    def coro(t):
        time.sleep(t)
        return "test"

    task1 = Task(coro).start(0.1)
    task2 = Task(coro).start(0.2)
    task3 = Task(coro).start(0.3)

    Task.wait_for_all([task1, task2, task3])
    assert not task1.is_running and not task2.is_running and not task3.is_running, "Failed to wait for task"


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
    """Test for thread exception with join."""
    with pytest.raises(DummyTestError):
        task = tasks.Task(raise_task_exception)
        task.start()
        task.join()




if __name__ == "__main__":
    pytest.main(["-x", __file__])
