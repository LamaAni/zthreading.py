import inspect
import threading
from time import sleep
from datetime import datetime, timedelta
from functools import wraps
from typing import Callable, Union
from weakref import WeakKeyDictionary
from zthreading.tasks import Task, TaskOperationException


class ThreadSafeContextException(TaskOperationException):
    pass


def __is_class_method(fun: Callable):
    return "." in fun.__qualname__


def wrap_with_thread_safe_context(
    fun,
    create_context,
    execute_with_context,
    source_decorator: Callable = None,
):
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
        use_daemon_thread (bool): If true, use a daemon thread for the process. Defaults to true

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
        fun,
        lambda self: threading.Lock(),
        exec_with_lock,
        source_decorator=thread_synchronized,
    )


class CollectExecutionCallsContext:
    def __init__(
        self,
        parent,
        fun,
        on_error: str = None,
        use_daemon_thread: bool = True,
    ):
        super().__init__()
        thread_name = f"ccc_async::{fun.__module__}::{fun.__qualname__}"
        if parent is not None:
            thread_name = f"{thread_name} (oid: {id(parent)})"
        self.task = Task(fun, thread_name=thread_name, use_daemon_thread=use_daemon_thread)
        self.is_waiting_on_call = False
        self.was_triggered = False
        self.parent = parent

        if isinstance(on_error, str):
            assert parent is not None, ValueError("Cannot assign on_error as string to a non class method")
            assert hasattr(parent, on_error), ValueError(
                f"Error method not found in {parent.__class__.__name__}.{on_error}"
            )
            self.invoke_error = getattr(parent, on_error)

        elif on_error is not None:
            assert callable(on_error), ValueError("On error must be a callable or a string")
            self.invoke_error = on_error

        self.task.on(self.task.error_event_name, lambda task, ex: self.on_error(ex))
        self.task.on(self.task.event_name, lambda *args, **kwargs: self.on_done())

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


def collect_consecutive_calls_async(
    on_error=None,
    use_daemon_thread: bool = True,
):
    """DECORATOR

    Multiple calls to this function will be bundled together and
    will be executed a-synchronically. A method decorated with this decorator
    cannot have free arguments (except self)

    on_error can be a method or string, if string, and this is a method of a class,
    the collection will expect, (on_error='call_error')

    Args:
        on_error (str, optional): The method to call on errors (if a class is calling). Defaults to None.
        use_daemon_thread (bool): If true, use a daemon thread for the process. Defaults to true

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
            lambda self: CollectExecutionCallsContext(
                self,
                fun,
                on_error=on_error,
                use_daemon_thread=use_daemon_thread,
            ),
            collect_execution_calls,
            source_decorator=collect_consecutive_calls_async,
        )

    return apply_decorator


def __calculate_delayed_calls_async_sleep_interval(interval: float, elapsed: float):
    return interval - ((elapsed * 1000) % (interval * 1000)) / 1000


def collect_delayed_calls_async(
    interval: Union[timedelta, float] = 0.2,
    max_delay: Union[timedelta, float] = 1,
    on_error: str = None,
    use_daemon_thread: bool = True,
):
    """METHOD DECORATOR

    Multiple calls to this function will be bundled together and
    will be executed a-synchronically. This decorator will add a delay time between the current call
    and the next call allowing the caller to collect multiple calls together.

    on_error can be a method or string, if string, and this is a method of a class,
    the collection will expect, (on_error='call_error')

    class o:
        def call_error(self,ex):
            pass

    Args:
        interval (Union[float, timedelta], optional): The time before invoking the call (will loop in this interval).
            Defaults to 0.2.
        max_delay (Union[float, timedelta], optional): The maximal time override for successive calls. Defaults to 1.
        on_error (str, optional): The method to call on errors (if a class is calling). Defaults to None.
        use_daemon_thread (bool): If true, use a daemon thread for the process. Defaults to true
    """
    interval: timedelta = interval if isinstance(interval, timedelta) else timedelta(seconds=interval)
    max_delay: timedelta = max_delay if isinstance(max_delay, timedelta) else timedelta(seconds=max_delay)
    last_checked: datetime = None
    last_total_checked: datetime = None

    def get_elapsed():
        nonlocal last_checked
        last_checked = last_checked or datetime.now()
        return datetime.now() - last_checked

    def get_total_elapsed():
        nonlocal last_total_checked
        last_total_checked = last_total_checked or datetime.now()
        return datetime.now() - last_total_checked

    def mark_checked():
        nonlocal last_checked
        last_checked = datetime.now()
        last_max_checked = last_checked

    def reset():
        nonlocal last_checked
        last_checked = None

    def reset_all():
        nonlocal last_total_checked
        reset()
        last_total_checked = None

    def apply_decorator(fun):
        @wraps(fun)
        def call_method(*args, **kwargs):
            reset()
            while True:
                elapsed = get_elapsed()
                total_elapsed = get_total_elapsed()
                mark_checked()
                if elapsed >= interval or total_elapsed >= max_delay:
                    break
                sleep(
                    __calculate_delayed_calls_async_sleep_interval(
                        interval=interval.total_seconds(),
                        elapsed=total_elapsed.total_seconds(),
                    )
                )
            reset_all()
            return fun(*args, **kwargs)

        return collect_consecutive_calls_async(
            on_error=on_error,
            use_daemon_thread=use_daemon_thread,
        )(call_method)

    return apply_decorator


if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
