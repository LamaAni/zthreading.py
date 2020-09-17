import inspect
import threading
from functools import wraps
from typing import Callable
from weakref import WeakKeyDictionary
from zthreading.tasks import Task, TaskOperationException


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
