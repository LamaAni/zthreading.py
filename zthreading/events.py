import weakref
import asyncio

from enum import Enum
from random import randint
from queue import SimpleQueue, Empty
from typing import Dict, Callable, List, Generator, AsyncGenerator


class Event:
    def __init__(self, name: str, args: list, kwargs: dict):
        super().__init__()
        self.name = name
        self.args = args
        self.kwargs = kwargs


def get_active_loop() -> asyncio.AbstractEventLoop:
    """returns the current active asyncio loop or creates 
    a new one.

    Returns:
        asyncio.AbstractEventLoop
    """
    loop = asyncio.events._get_running_loop()
    loop = asyncio.new_event_loop() if loop is None else loop
    return loop


def filter_events_predict(name: str, args: list, kwargs: dict) -> bool:
    """Allows or denies events from being emitted.

    Args:
        name (str): The name of the event
        args (list): The argument list
        kwargs (dict): The named argument list.

    Returns:
        bool: True if the event is allowed.
    """
    pass


class EventHandler:
    _event_actions: Dict[str, Dict[int, Callable]] = None
    _pipeto: [] = None
    _action_last_idx = 0
    stop_all_streams_event_name = None

    def __init__(self, on_event: Callable = None):
        """An event handler. Allows,
        1. Registering for event callbacks.
        2. Emitting (broadcasting) events.
        3. Piping events to other event handlers.

        Usage:
            hdnl = EventHandler()
            hndl.on("my event", lambda  msg: print("ok: "+msg))
            hndl.emit("my event", "my message)

        Args:
            on_event (optional): Called on any event. Defaults to None.
        """
        super().__init__()
        self._pipeto = []
        self._event_actions = dict()
        self.on_event = on_event
        self._action_last_idx = 0
        self._event_actions_search_by_name: dict = None
        self._events_filter: Callable = None
        self.stop_all_streams_event_name = self._create_object_instance_unique_event_name("stop_streams")
        self.catch_all_event_name = self._create_object_instance_unique_event_name("catch_all")

    @staticmethod
    def _get_value_from_reference(val):
        """Returns the value of a reference or the value itself. 
        Allows for easy access to weakref objects.
        """
        if isinstance(val, weakref.ReferenceType):
            return val()
        return val

    def _create_object_instance_unique_event_name(self, base_name: str):
        """Creates a unique name for internal event handling.
        """
        return f"{self.__class__.__name__}.{base_name} (oid: {id(self)}, rid:{randint(0,10000)})"

    def on_any_event(self, action: Callable) -> int:
        """Adds a new "catchall" event action. This action will be called
        on ANY event.

        Arguments:
            action {Callable} -- The method/other to
                be called when the event is triggered.

        Returns:
            int -- The event index, within the specific name
                dictionary. name+index are unique.
        """
        return self.on(self.catch_all_event_name, action)

    def on(self, name: str, action: Callable) -> int:
        """Add a new event handler.

        Arguments:
            name {str|List[str]} -- The event name
            action {Callable} -- The method/other to
                be called when the event is triggered.

        Returns:
            int|List[int] -- The event index, within the specific name
                dictionary. name+index are unique.
        """
        if isinstance(name, list):
            return [self.on(n, action) for n in name]

        if isinstance(name, Enum):
            name = str(name)

        assert isinstance(name, str), ValueError("name must be a string")
        assert callable(action), ValueError("action must be a callable.")

        if not self.hasEvent(name):
            self._event_actions[name] = dict()
        idx = self._action_last_idx
        self._action_last_idx += 1

        # adjusting dictionaries.
        self._event_actions_search_by_name = None
        self._event_actions[name][idx] = action

        return idx

    def hasEvent(self, name: str, index: int = None):
        """Returns true if a event action is present

        Arguments:
            name {str} -- The name of the event

        Keyword Arguments:
            index {int} -- If not None, will search for a specific
            call of this event, according to the event index returned
            from the on method. (default: {None})

        Returns:
            bool -- True if event exists.
        """
        if index is not None:
            return name in self._event_actions and index in self._event_actions[name]
        return name in self._event_actions and len(self._event_actions[name]) > 0

    def clear(self, name: str, idx: int = None):
        """Clears event action(s).

        Arguments:
            name {str|List[str]} -- The name of the event to clear.

        Keyword Arguments:
            idx {int|List[int]} -- If not None, will clear the specific
                call to the event identified by the event index
                returned from the on method. Otherwise will clear
                ALL events of name (default: {None})
        Returns:
            bool - If all clear events succeded.
        """
        if isinstance(name, list):
            return all([self.clear(n, idx) for n in name])

        assert isinstance(name, str), ValueError("name must be a string")

        if not self.hasEvent(name):
            return False

        if idx is not None:
            if isinstance(idx, list):
                return all([self.clear(name, i) for i in idx])
            del self._event_actions[name][idx]
        else:
            del self._event_actions[name]
        return True

    def _get_event_actions_by_name(self, name: str) -> List[Callable]:
        """Internal. Retruns all the events actions by name.
        """
        if self._event_actions_search_by_name is None or name not in self._event_actions_search_by_name:
            self._event_actions_search_by_name = self._event_actions_search_by_name or dict()

            if not self.hasEvent(name):
                self._event_actions_search_by_name[name] = []
            else:
                self._event_actions_search_by_name[name] = list(self._event_actions[name].values())

        return self._event_actions_search_by_name[name]

    def _get_catch_all_event_actions(self, original_event_name: str):
        """Internal gets all catch all events actions by name,
        """
        if original_event_name != self.catch_all_event_name and self.hasEvent(self.catch_all_event_name):
            return list(self._event_actions[self.catch_all_event_name].values())
        return []

    def _get_pipe_handlers(self) -> List["EventHandler"]:
        """Get all pipe handlers to propagate events.
        """
        handlers = []
        needs_pipe_cleaning = False
        for hndl in self._pipeto:
            # in case of weak references.
            h: EventHandler = self._get_value_from_reference(hndl)
            if h is not None:
                handlers.append(h)
            else:
                needs_pipe_cleaning = True

        if needs_pipe_cleaning:
            self._pipeto = [hndl for hndl in self._pipeto if self._get_value_from_reference(hndl) is not None]

        return handlers

    @classmethod
    def _process_in_thread_event_action_result(cls, action_result, debug: bool = None):
        """Call to await (if possible) aysncio results.
        """
        if asyncio.iscoroutine(action_result):
            loop = get_active_loop()
            if loop.is_running():
                loop.create_task(action_result)
            else:
                loop.run_until_complete(action_result)
        else:
            action_result

    def emit(self, name: str, *args, **kwargs):
        """Emits an event. Any arguments sent after name, will
        be passed to the event action.

        Arguments:
            name {str} -- The name of the event to emit.
        """
        if isinstance(name, Enum):
            name = str(name)
        assert isinstance(name, str), ValueError("name must be a string")

        if self._events_filter is not None:
            if self._events_filter(name, args, kwargs) is not True:
                return

        if self.on_event is not None:
            self._process_in_thread_event_action_result(self.on_event(self, name, *args, **kwargs))

        for action in self._get_event_actions_by_name(name):
            # actions are not automatically removed.
            action = self._get_value_from_reference(action)
            self._process_in_thread_event_action_result(action(*args, **kwargs))

        for action in self._get_catch_all_event_actions(name):
            # actions are not automatically removed.
            action = self._get_value_from_reference(action)
            self._process_in_thread_event_action_result(action(name, *args, **kwargs))

        for handler in self._get_pipe_handlers():
            self._process_in_thread_event_action_result(handler.emit(name, *args, **kwargs))

    def pipe(self, other: "EventHandler", use_weak_reference: bool = False):
        """Pipe all events emitted from this handler to another handler.

        Arguments:
            other {EventHandler} -- The event handler to pipe event to.

        Keyword Arguments:
            use_weak_reference {bool} -- If true, uses a weak reference, that will not
            keep the other handler in memory. Allows for weakly bound events. (default: {False})
        """
        assert EventHandler is not None and isinstance(
            other, EventHandler
        ), "other must be an instance of EventHandler."

        if use_weak_reference:
            self._pipeto.append(weakref.ref(other))
        else:
            self._pipeto.append(other)

    def detach_pipe(self, other: "EventHandler"):
        """Stops piping the events to the other handler.
        No error is throw if no pipe exists.

        Arguments:
            other {EventHandler} -- The event handler currently piping to.
        """
        assert EventHandler is not None and isinstance(
            other, EventHandler
        ), "other must be an instance of EventHandler."

        new_pipe_list = []
        for hndl in self._pipeto:
            h = self._get_value_from_reference(hndl)
            if h is None or h is other:
                continue
            new_pipe_list.append(hndl)
        self._pipeto = new_pipe_list

    def is_piping(self, other: "EventHandler"):
        """Returns true if piping to this handler.

        Arguments:
            other {EventHandler} -- The other event handler.
        """
        for hndl in self._pipeto:
            if self._get_value_from_reference(hndl) is other:
                return True
        return False

    def filter_events(self, predict: filter_events_predict):
        self._events_filter = predict

    @classmethod
    def create_events_filter_pipe(cls, predict: filter_events_predict) -> "EventHandler":
        """Faster event handler creation. The filter pipe allows for passing only specific events
        to between events handlers.

        Args:
            predict - a callable to predict if this event should be propagated.

        Returns:
            A new pipe event handler.
        """
        hndl = EventHandler()
        hndl.filter_events(predict)
        return hndl

    @classmethod
    def create_event_name_filter_pipe(cls, events: List[str]) -> "EventHandler":
        """Faster event handler creation. The filter pipe allows for passing only specific events
        to between events handlers.

        Args:
            events - a list of events to propagate.

        Returns:
            A new pipe event handler.
        """
        assert all(isinstance(ev, (str, Enum)) for ev in events)

        events = [str(ev) for ev in events]
        events = set(events)
        hndl = EventHandler()

        def filter(name, args, kwargs):
            return name in events

        hndl.filter_events(filter)
        return hndl

    def stop_all_streams(self):
        """Invokes an interanl events that stops any event streams that were opened.
        When a steam is stopped, its yield command becomes unblocking. 
        
        For the case of a loop, it would exit, eg,
        
        Example:

            for a in my_stream:
                ....
            
            hndl.stop_all_steams -> will exit loop.
        """
        self.emit(self.stop_all_streams_event_name)

    def _prepare_stream_queue(self, event_name: str = None) -> (SimpleQueue, "EventHandler"):
        """Internal, prepare a stream internal queue to manage events.
        """
        pipe_handler = EventHandler()
        queue = SimpleQueue()

        def append_to_queue(name, *args: list, **kwargs):
            if event_name is not None and name not in [
                event_name,
                self.stop_all_streams_event_name,
            ]:
                return
            queue.put(Event(name, args, kwargs))

        pipe_handler.on_any_event(append_to_queue)
        self.pipe(pipe_handler, use_weak_reference=True)

        return queue, pipe_handler

    def _get_queue_event(self, queue: SimpleQueue, timeout: float) -> Event:
        """Internal. Get next queued event, with timeout.
        """
        try:
            event = queue.get(block=True, timeout=timeout)
        except Empty:
            raise Empty("Timed out while waiting for stream")

        if event.name == "error":
            raise event.args[0]

        return event

    def _create_stream(
        self,
        queue: SimpleQueue,
        pipe_handler: "EventHander",  # noqa: F821
        timeout: float,
        process_event_data: Callable = None,
    ) -> Generator[Event, None, None]:
        """Internal. Creates a new stream.

        Args:
            queue (SimpleQueue): The associated queue.
            pipe_handler (EventHander): The event piping handler.
            process_event_data (Callable, optional): A method to pre-process event data before
                the event is sent to the stream. Defaults to None.

        Yields:
            Generator of Event.
        """
        while True:
            ev: Event = self._get_queue_event(queue, timeout)
            if ev.name == self.stop_all_streams_event_name:
                break
            if process_event_data is not None:
                ev = process_event_data(ev)
            yield ev

    async def _create_stream_async(
        self,
        queue: SimpleQueue,
        pipe_handler: "EventHander",  # noqa: F821
        timeout: float,
        process_event_data: Callable = None,
    ) -> AsyncGenerator[Event, None]:
        """Internal. Creates a new async stream.

        Args:
            queue (SimpleQueue): The associated queue.
            pipe_handler (EventHander): The event piping handler.
            process_event_data (Callable, optional): A method to pre-process event data before
                the event is sent to the stream. Defaults to None.

        Yields:
            AsyncGenerator of Event.
        """
        while True:
            ev: Event = self._get_queue_event(queue, timeout)
            if ev.name == self.stop_all_streams_event_name:
                break
            if process_event_data is not None:
                ev = process_event_data(ev)
            yield ev

    def stream(
        self,
        event_name: str = None,
        timeout: float = None,
        use_async_loop: bool = False,
        process_event_data: Callable = None,
    ) -> Generator[Event, None, None]:
        """Creates an event stream that will collect any event from this handler.

        Args:
            event_name (str, optional): The event name to stream. Defaults to None. If none, streams
                all events
            timeout (float, optional): Timeout between events. Defaults to None.
            use_async_loop (bool, optional): Use an asyncio loop to perform the stream. This would result
                in an asyncio compatible stream. Defaults to False.
            process_event_data (Callable, optional): A method to be called on any event value. The
                result of this method is passed to the stream. Defaults to None.

        Yields:
            Generator|AsyncGenerator of Event | the result of process_event_data
        """
        if isinstance(event_name, Enum):
            event_name = str(event_name)

        assert event_name is None or isinstance(event_name, str), ValueError(
            "event_name must be either a string or None"
        )

        queue, pipe_handler = self._prepare_stream_queue(event_name)
        if use_async_loop is True:
            return self._create_stream_async(queue, pipe_handler, timeout, process_event_data)
        return self._create_stream(queue, pipe_handler, timeout, process_event_data)

    def wait_for(
        self, predict, raise_errors: bool = True, wait_count: int = 1, timeout: float = None,
    ):
        """Waits for a specific event to be invoked.

        Args:
            predict (Callable or str): If a string, waits for the specific event. Otherwise expects true
                when a matching event is found. (lambda task, name: ? true)
            raise_errors (bool): True if to raise handler errors.
            wait_count (int): How many times should the event be triggered. Must be larger then zero.
            timeout (float): The timeout in seconds before throwing an error.

        Returns:
            The list of tasks sent to the method.
        """
        self.wait_for_events(predict, [self], raise_errors=raise_errors, wait_count=wait_count, timeout=timeout)

    @classmethod
    def wait_for_events(
        cls,
        predict,
        handlers: List["EventHandler"],
        raise_errors: bool = True,
        wait_count: int = 1,
        timeout: float = None,
    ) -> List["EventHandler"]:
        """Waits for a specific event to be invoked on the handlers.

        Args:
            predict (Callable or str): If a string, waits for the specific event. Otherwise expects true
                when a matching event is found. (lambda task, name: ? true)
            handlers (list) : List of handlers to execute for.
            raise_errors (bool): True if to raise handler errors.
            wait_count (int): How many times should the event be triggered. Must be larger then zero.
            timeout (float): The timeout in seconds before throwing an error.

        Returns:
            The list of tasks sent to the method.
        """

        if isinstance(handlers, EventHandler):
            handlers: List["Task"] = [handlers]

        assert wait_count > 0, "Wait count must be at least 1"

        if predict is not None and not isinstance(predict, Callable):
            predict_equals = predict

            def predict_event_by_name(sender: EventHandler, name: str, *args, **kwargs):
                return name == predict_equals

            predict = predict_event_by_name

        assert predict is None or callable(predict), "Predict must be a Callable or event name string"

        wait_queue = SimpleQueue()
        first_error = None
        matched_handlers = []

        def stop_on_error(hndl: EventHandler, error):
            nonlocal first_error
            first_error = error
            wait_queue.put("error")

        def on_piped_event(handler, name, *args, **kwargs):
            if name == "error":
                stop_on_error(handler, args[1])
                return
            if predict is None or predict(handler, name, *args, **kwargs):
                matched_handlers.append(handler)
            if len(matched_handlers) == wait_count:
                wait_queue.put("done")

        pipes = []

        def bind(handler: EventHandler):
            pipe_handler = EventHandler(
                on_event=lambda pipe, name, *args, **kwargs: on_piped_event(handler, name, *args, **kwargs)
            )
            pipes.append(pipe_handler)
            handler.pipe(
                pipe_handler, use_weak_reference=True,
            )

        for handler in handlers:
            bind(handler)

        try:
            wait_queue.get(timeout=timeout)
        except Empty:
            first_error = TimeoutError(f"Timeout while waiting for event: {predict}")

        if first_error is not None:
            raise first_error

        return matched_handlers


class AsyncEventHandler(EventHandler):
    def __init__(self, on_event: Callable = None):
        """An event handler compatible with asyncio. Allows,
        1. Registering for event callbacks.
        2. Emitting (broadcasting) events.
        3. Piping events to other event handlers.

        Usage:
            hdnl = EventHandler()
            hndl.on("my event", lambda  msg: print("ok: "+msg))
            hndl.emit("my event", "my message)

        Args:
            on_event (optional): Called on any event. Defaults to None.
        """
        super().__init__(on_event=on_event)

    @classmethod
    async def _process_async_action_result(cls, action_result):
        """Call to await (if needed) aysncio results.
        """
        if asyncio.iscoroutine(action_result):
            return await action_result
        else:
            return action_result

    async def emit(self, name: str, *args, **kwargs):
        """A-Synchronically emits an event. Any arguments sent after name, will
        be passed to the event action.

        Arguments:
            name {str} -- The name of the event to emit.
        """
        if isinstance(name, Enum):
            name = str(name)
        assert isinstance(name, str), ValueError("name must be a string")

        if self.on_event is not None:
            await self._process_async_action_result(self.on_event(name, *args, **kwargs))

        for action in self._get_event_actions_by_name(name):
            # actions are not automatically removed.
            action = self._get_value_from_reference(action)
            await self._process_async_action_result(action(*args, **kwargs))

        for action in self._get_catch_all_event_actions(name):
            # actions are not automatically removed.
            action = self._get_value_from_reference(action)
            await self._process_async_action_result(action(name, *args, **kwargs))

        for handler in self._get_pipe_handlers():
            await self._process_async_action_result(handler.emit(name, *args, **kwargs))

    def emit_sync(self, name: str, *args, **kwargs):
        """Synchronically emits an event. Any arguments sent after name, will
        be passed to the event action.

        Arguments:
            name {str} -- The name of the event to emit.
        """
        self._process_in_thread_event_action_result(self.emit(name, *args, **kwargs))


if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
