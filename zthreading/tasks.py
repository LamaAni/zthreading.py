import os
import threading
import asyncio

from datetime import datetime
from queue import SimpleQueue, Empty
from typing import List, Callable

from zthreading.events import EventHandler, get_active_loop


def abort_executing_thread(thread: threading.Thread):
    """Abort the thread execution.

    Args:
        thread (threading.Thread): The thread to abort.
    """
    # FIXME: Not recommended since _stop might be removed.
    thread._reset_internal_locks(False)
    thread._stop()


def wait_for_future(future: asyncio.Future, timeout: float = None):
    """Waits for anon asyncio future. Helper method.

    Args:
        future (asyncio.Future): The future to wait for.
        timeout (float, optional): The wait timeout. Defaults to None.

    Returns:
        any: The future result.
    """
    if asyncio.iscoroutine(future):
        loop = get_active_loop()
        future = loop.create_task(future)

    loop = future.get_loop()

    if timeout is not None:
        future = asyncio.wait_for(future, timeout=timeout, loop=loop)

    loop.run_until_complete(future)

    last_exception = future.exception()
    if last_exception is not None:
        raise last_exception

    return future.result()


class TaskOperationException(Exception):
    """A general class for task operation errors.
    """

    pass


class Task(EventHandler):
    TASKS_DEFAULT_TO_ASYNC_LOOP: bool = os.environ.get("TASKS_DEFAULT_TO_ASYNC_LOOP", "").lower().strip() == "true"
    TASKS_ERROR_EVENT_NAME: str = os.environ.get("TASKS_ERROR_EVENT_NAME", "error")

    def __init__(
        self,
        action: Callable,
        use_async_loop=None,
        use_daemon_thread=True,
        thread_name: str = None,
        event_name: str = "done",
    ):
        """Implements a multi approach action executor that allows for
        both a system thread or an asyncio thread (async/await calls).

        Args:
            action (Callable): The action to execute.
            use_async_loop (bool, optional): If true use an asyncio thread to execute the action otherwise uses 
                the a system thread. Defaults to the environment variable TASKS_DEFAULT_TO_ASYNC_LOOP, or false.
            use_daemon_thread (bool, optional): If true, and is using system threading, starts the task in daemon mode.
                Defaults to True.
            thread_name (str, optional): If using threads, the name of the executing thread.
                Defaults to a an auto generated name.
            event_name (str, optional): The name of the event to trigger when the task is done. Defaults to "done",
                if none, no event will be triggered.

        Events:
            [event_name] - when the task is done. (Args: [])
            "error" or env {TASKS_ERROR_EVENT_NAME} - when the task errors. (Args: [error:Exception])

        Note: This object can execute a coroutine in a different thread, and will do so if
            use_async_loop is False.
        """
        super().__init__()
        self.action = action
        self._use_async_loop = use_async_loop or self.TASKS_DEFAULT_TO_ASYNC_LOOP
        self._thread: threading.Thread = None
        self._async_loop_task: asyncio.Future = None
        self._use_daemon_thread: bool = use_daemon_thread
        self._completed_at = None
        self._action_result = None
        self._error = None
        self._thread_name = thread_name or self._generate_thread_name()
        self._event_name = event_name

    def _generate_thread_name(self):
        return f"{self.__class__.__name__}-{id(self)}"

    @property
    def use_async_loop(self) -> bool:
        """If true uses asyncio to execute threads.
        """
        return self._use_async_loop

    @property
    def async_task(self):
        """The asyncio current executing task or None."""
        return self._async_loop_task

    @property
    def is_running(self) -> bool:
        """If true is currently running"""
        if self.use_async_loop:
            return self._async_loop_task is not None and not self._async_loop_task.done()
        else:
            return self._thread is not None and self._thread.is_alive()

    @property
    def is_done(self) -> bool:
        """If true was executed successfully"""
        return self._completed_at is not None

    @property
    def completed_at(self) -> datetime:
        """Timestamp of the last successful execution."""
        return self._completed_at

    @property
    def result(self):
        """The result of the last execution"""
        return self._action_result

    @property
    def error(self) -> Exception:
        """The last execution error"""
        return self._error

    @property
    def event_name(self) -> str:
        """The name of the event to invoke once the thread executes successfully"""
        return self._event_name

    def is_current_thread(self):
        """Returns true (in system threading) if the current thread is the executing thread"""
        return self._thread.ident == threading.current_thread().ident

    async def _run_as_async(self, args, kwargs):
        rslt = None
        try:
            rslt = self.action(*args, **kwargs)
            if asyncio.iscoroutine(rslt):
                rslt = await rslt
            self._action_result = rslt
            self.emit(self.event_name)
        except Exception as ex:
            self._error = ex
            self.emit("error", self, ex)
        finally:
            self._completed_at = datetime.now()
            self.stop_all_streams()
        self._result = rslt
        return rslt

    def _run_as_thread(self, args, kwargs):
        rslt = None
        try:
            rslt = self.action(*args, **kwargs)
            if asyncio.iscoroutine(rslt):
                rslt = wait_for_future(rslt)
            self._action_result = rslt
            if self.event_name is not None:
                self.emit(self.event_name)
        except Exception as ex:
            self._error = ex
            self.emit("error", self, ex)
        finally:
            self._completed_at = datetime.now()
            self.stop_all_streams()
        self._result = rslt
        return rslt

    def start(self, *args, **kwargs) -> "Task":
        """Start an async/threaded action

        Returns:
            [Task]: The current object (for ease of use)

        Easy Use:

            executor = Task(lambda arg: print("ok "+arg)).start("my arg")
        """
        if self.use_async_loop:
            self._async_loop_task = asyncio.ensure_future(self._run_as_async(args, kwargs), loop=get_active_loop())
        else:
            self._thread = threading.Thread(
                target=lambda: self._run_as_thread(args, kwargs),
                name=self._thread_name,
                daemon=self._use_daemon_thread,
            )
            self._thread.start()
        return self

    def stop(self, timeout: float = None, throw_error_if_not_running: bool = False):
        """Stop the executing action

        Args:
            timeout (float, optional): If exists, the stop process will try and wait for the
                task to complete for {timeout} before forcefully stopping. Defaults to None.
        """
        if not self.is_running:
            if throw_error_if_not_running:
                raise Exception("The task is not running")
            return

        if timeout is not None:
            self.join(timeout)

        if self.use_async_loop:
            self.async_task.cancel()
        else:
            abort_executing_thread(self._thread)

    def join(
        self, timeout: float = None, raise_last_exception: bool = True, throw_error_if_not_running: bool = False,
    ):
        """Wait for the task to complete.

        Args:
            timeout (float, optional): Time to wait before timeout error
                Defaults to None.
            throw_error_if_not_running (bool, optional): Throw an error if not running. Defaults to False.
            raise_last_exception (bool, optional): If true, and the thread had an error, raise the error.

        Returns:
            [type]: [description]
        """
        if throw_error_if_not_running and not self.is_running:
            raise TaskOperationException("Cannot join a non running task (throw_error_if_not_running==True)")

        if self.is_running:
            if self.use_async_loop:
                wait_for_future(self.async_task, timeout)
            # FIXME: Maybe there is a better approach here. Should raise error?
            elif self._thread.is_alive() and threading.currentThread() != self._thread:
                self._thread.join(timeout)

        if raise_last_exception and self.error is not None:
            raise self.error

        return self._action_result

    @staticmethod
    def wait_for_all(tasks: List["Task"], raise_errors: bool = True, error_event_name: str = "error"):
        """Waits for all tasks in the array of tasks to complete.

        Arguments:
            tasks {List[Task]|Task} -- The tasks to wait for.

        Keyword Arguments:
            raise_errors {bool} -- If true, will raises an error if any
                of the tasks raises an error (default: {True})
            error_event_name {str} -- The name of the error event.
        """
        if isinstance(tasks, Task):
            tasks: List[Task] = [tasks]

        first_error = None

        def on_error_event(*args, **kwargs):
            nonlocal first_error
            found_error = False
            for err in args:
                if isinstance(err, Exception):
                    first_error = err
                    found_error = True

            if found_error is not True:
                first_error = TaskOperationException(
                    "Error message found but no error in args. Error event without info"
                )
            for task in tasks:
                if task.is_running:
                    task.stop()

        pipe_handler = EventHandler()
        pipe_handler.on(error_event_name, on_error_event)

        if raise_errors:
            for task in tasks:
                task.pipe(pipe_handler)

        for task in tasks:
            task.join(raise_last_exception=False)

        if raise_errors:
            for task in tasks:
                task.detach_pipe(pipe_handler)

        if first_error is not None:
            raise first_error

    @staticmethod
    def wait_for_events(
        predict, tasks: List["Task"], raise_errors: bool = True, wait_count: int = 1, timeout: float = None
    ) -> List["Task"]:
        """Waits for a specific event to be invoked on the task object.

        Args:
            predict (Callable or str): If a string, waits for the specific event. Otherwise expects true
                when a matching event is found. (lambda task, name: ? true)
            tasks (list) : List of tasks to execute for.
            raise_errors (bool): True if to raise task errors.
            wait_count (int): How many times should the event be triggered. Must be larger then zero.
            timeout (float): The timeout in seconds before throwing an error.

        Returns:
            The list of tasks sent to the method.
        """

        if isinstance(tasks, Task):
            tasks: List["Task"] = [tasks]

        assert wait_count > 0, "Wait count must be at least 1"

        def predict_event_by_name(task: Task, name: str):
            return name == (predict or task.event_name)

        if isinstance(predict, str):
            predict = predict_event_by_name

        assert callable(predict), "Predict must be a Callable or event name string"

        wait_queue = SimpleQueue()
        first_error = None
        matched_tasks = []

        def stop_on_error(task, error):
            nonlocal first_error
            first_error = error
            for task in tasks:
                if task.is_running:
                    task.stop()
            wait_queue.put("error")

        def on_piped_event(name, task, *args, **kwargs):
            if name == "error":
                stop_on_error(task, args[0])
                return
            if predict(task, name, *args, **kwargs):
                matched_tasks.append(task)
            if len(matched_tasks) == wait_count:
                wait_queue.put("done")

        pipe_handler = EventHandler(on_event=on_piped_event)
        for task in tasks:
            task.pipe(pipe_handler, use_weak_reference=True)

        try:
            wait_queue.get(timeout=timeout)
        except Empty:
            first_error = TimeoutError

        if first_error is not None:
            raise first_error

        return matched_tasks

    @staticmethod
    def wait_for_some(
        tasks: List["Task"], raise_errors: bool = True, wait_count: int = 1, timeout: float = None
    ) -> List["Task"]:
        """Waits for some of the tasks to complete.

        Args:
            tasks (list) : List of tasks to execute for.
            raise_errors (bool): True if to raise task errors.
            wait_count (int): How many tasks should complete.
            timeout (float): The timeout in seconds before throwing an error.

        Returns:
            The list of tasks sent to the method.
        """
        return Task.wait_for_events(None, tasks, raise_errors, wait_count=wait_count, timeout=timeout)

    @staticmethod
    def wait_for_one(tasks: List["Task"], raise_errors: bool = True, timeout: float = None) -> "Task":
        """Waits for one of the tasks to complete. Same as wait_for_some with wait_count==1

        Args:
            tasks (list) : List of tasks to execute for.
            raise_errors (bool): True if to raise task errors.
            timeout (float): The timeout in seconds before throwing an error.

        Returns:
            The list of tasks sent to the method.
        """
        return Task.wait_for_some(tasks, raise_errors, wait_count=1, timeout=timeout)[0]

    @staticmethod
    def get_thread_description(thread: threading.Thread = None):
        """Helper method. Gets the description of a thread.

        Args:
            thread (threading.Thread, optional): The thread to get the description for. Defaults to None. 
                If None use current thread.

        Returns:
            str: The description.
        """
        thread = thread or threading.current_thread()
        return f"{thread.name} ({thread.ident})"

    def __str__(self):
        if self.use_async_loop:
            return f"async_task ({self._thread_name})"
        return self.get_thread_description(self)


if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
