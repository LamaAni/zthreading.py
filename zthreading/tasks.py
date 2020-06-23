import os
import threading
import asyncio
import inspect
from weakref import WeakKeyDictionary

from datetime import datetime
from queue import SimpleQueue, Empty
from typing import List, Callable
from functools import wraps

from zthreading.events import EventHandler, get_active_loop


def abort_executing_thread(thread: threading.Thread):
    # FIXME: Not recommended since _stop might be removed.
    thread._reset_internal_locks(False)
    thread._stop()


def wait_for_future(future: asyncio.Future, timeout: float = None):
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
    pass


class Task(EventHandler):
    TASKS_DEFAULT_TO_ASYNC_LOOP: bool = os.environ.get("TASKS_DEFAULT_TO_ASYNC_LOOP", "").lower().strip() == "true"

    def __init__(
        self,
        action: Callable,
        use_async_loop=None,
        use_daemon_thread=True,
        thread_name: str = None,
        event_name: str = "done",
    ):
        """Implements a multi approach action executor that allows for
        both a system thread or an async/await mode.

        Note: This object can execute a coroutine in a different thread, and will do so if
        use_async_loop is False.

        Args:
            action (Callable): The action to execute.
            use_async_loop (bool, optional): If true use an async method to execute the action. Defaults to
                the environment variable TASKS_DEFAULT_TO_ASYNC_LOOP, or false.
            use_daemon_thread (bool, optional): If true, and is using threading, starts the task in daemon mode.
                Defaults to True.
            thread_name (str, optional): If using threads, the name of the executing thread.
                Defaults to a random string.
            event_name (str, optional): The name of the event to trigger when the task is done.

        Events:
            [event_name] - when the task is done. (Args: [])
            "error" - when the task errors. (Args: [error:Exception])
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
        return self._use_async_loop

    @property
    def async_task(self):
        return self._async_loop_task

    @property
    def is_running(self) -> bool:
        if self.use_async_loop:
            return self._async_loop_task is not None and not self._async_loop_task.done()
        else:
            return self._thread is not None and self._thread.is_alive()

    @property
    def is_done(self) -> bool:
        return self._completed_at is not None

    @property
    def completed_at(self) -> datetime:
        return self._completed_at

    @property
    def result(self):
        return self._action_result

    @property
    def error(self) -> Exception:
        return self._error

    @property
    def event_name(self) -> str:
        return self._event_name

    def is_current_thread(self):
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
        predict, tasks: List["Task"], raise_errors: bool = True, wait_count: int = 1, timeout: float = None,
    ) -> List["Task"]:
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
    def wait_for_some(tasks: List["Task"], raise_errors: bool = True, wait_count: int = 1) -> List["Task"]:
        return Task.wait_for_events(None, tasks, raise_errors, wait_count=wait_count)

    @staticmethod
    def wait_for_one(tasks: List["Task"], raise_errors: bool = True, wait_count: int = 1) -> "Task":
        return Task.wait_for_some(tasks, raise_errors, wait_count=1)[0]

    @staticmethod
    def get_thread_description(thread: threading.Thread = None):
        thread = thread or threading.current_thread()
        return f"{thread.name} ({thread.ident})"

    def __str__(self):
        if self.use_async_loop:
            return f"async_task ({self._thread_name})"
        return self.get_thread_description(self)


class ThreadSafeContextException(TaskOperationException):
    pass


def __is_class_method(fun: Callable):
    return "." in fun.__qualname__


def wrap_with_thread_safe_context(fun, create_context, execute_with_context, source_decorator: Callable = None):
    """DECORATOR

    Wraps a method to be executed with a context that will be create
    only once, event if called from multiple threads, for this method.

    Args:
        fun (callable): The function
        create_context (callable): A function that will create the context
            Signature: execute_with_context(self) # null in the case of a non object.
        execute_with_context (callable): A method to be called to execute with
        the context. Will preserve "self", and class arguments.
            Signature: execute_with_context(context, *args,**kwargs)


    Returns:
        callable: A wrapper decorator.
    """

    if __is_class_method(fun):
        contexts = WeakKeyDictionary()
        make_context_lock = threading.Lock()

        @wraps(fun)
        def warpper(self, *args, **kwargs):
            # make sure not to create two locks :)
            with make_context_lock:
                try:
                    if self not in contexts:
                        contexts[self] = create_context(self)
                except TypeError as typerr:
                    raise ThreadSafeContextException(
                        f"The decorator '@{(source_decorator or wrap_with_thread_safe_context).__qualname__}'"
                        + f" cannot be applied to method '{fun.__qualname__}' "
                        + "since its parent class is not hashable",
                        typerr,
                    )
                except Exception as ex:
                    raise ex

            return execute_with_context(contexts[self], self, *args, **kwargs)

        return warpper
    else:
        context = create_context(None)

        @wraps(fun)
        def warpper(*args, **kwargs):
            return execute_with_context(context, *args, **kwargs)

        return warpper


def thread_synchronized(fun):
    """DECORATOR

    The decorated function will be executed synchronically between threads.
    (Only one thread at a time; implemented using threading.Lock)
    """

    def exec_with_lock(lock, *args, **kwargs):
        raise_exception = None
        return_val = None
        with lock:
            try:
                return_val = fun(*args, **kwargs)
            except Exception as ex:
                raise_exception = ex
        if raise_exception is not None:
            raise raise_exception
        return return_val

    return wrap_with_thread_safe_context(
        fun, lambda self: threading.Lock(), exec_with_lock, source_decorator=thread_synchronized,
    )


class CollectExecutionCallsContext:
    def __init__(self, parent, fun, on_error):
        super().__init__()
        thread_name = f"ccc_async::{fun.__module__}::{fun.__qualname__}"
        if parent is not None:
            thread_name = f"{thread_name} (oid: {id(parent)})"
        self.task = Task(fun, thread_name=thread_name)
        self.is_waiting_on_call = False
        self.was_triggered = False
        self.parent = parent

        if isinstance(on_error, str):
            assert parent is not None, ValueError("Cannot assign on_error as string to a non class method")
            assert hasattr(parent, on_error), ValueError(
                f"Error method not found in {parent.__class__.__name__}.{on_error}"
            )
            self.invoke_error = getattr(parent, on_error)

        else:
            assert callable(on_error), ValueError("On error must be a callable or a string")
            self.invoke_error = on_error

        self.task.on("error", lambda task, ex: self.on_error(ex))
        self.task.on("done", lambda *args, **kwargs: self.on_done())

    def on_done(self):
        self.was_triggered = False
        if self.is_waiting_on_call:
            self.is_waiting_on_call = False
            self.execute_as_task()

    def on_error(self, ex):
        if self.invoke_error is not None:
            self.invoke_error(ex)
        self.on_done()

    def execute_as_task(self, *args):
        self.was_triggered = True
        if self.parent is not None:
            self.task.start(self.parent)
        else:
            self.task.start()


def collect_consecutive_calls_async(on_error=None):
    """DECORATOR

    Multiple calls to this function will be bundled together and
    will be executed a-synchronically. A method decorated with this decorator
    cannot have free arguments (except self)

    on_error can be a method or string, if string, and this is a method of a class,
    the collection will expect, (on_error='call_error')

    class o:
        def call_error(self,ex):
            pass
    """

    def apply_decorator(fun):
        assert len(inspect.signature(fun).parameters.values()) <= (1 if __is_class_method(fun) else 0), ValueError(
            "Cannot apply a collect_consecutive_calls_async decorator to a method that has arguments (Except self)"
        )

        def collect_execution_calls(context: CollectExecutionCallsContext, *args, **kwargs):
            if context.was_triggered:
                context.is_waiting_on_call = True
                return
            context.execute_as_task(*args)

        return wrap_with_thread_safe_context(
            fun,
            lambda self: CollectExecutionCallsContext(self, fun, on_error),
            collect_execution_calls,
            source_decorator=collect_consecutive_calls_async,
        )

    return apply_decorator


if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
