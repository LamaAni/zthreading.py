class ZThreadingException(Exception):
    pass


class TheradingWaiterException(ZThreadingException):
    pass


class ThreadingTimeoutError(ZThreadingException, TimeoutError):
    pass


class TaskException(ZThreadingException):
    pass


class TaskOperationException(TaskException):
    """A general class for task operation errors."""

    pass