import pytest
import asyncio
import threading
import time
from zthreading import events
from zthreading.tasks import Task


class DummyExcpetion(Exception):
    pass


def test_event_handler_event():
    hndl = events.EventHandler()
    hndl.on("test_event", lambda: print("ok"))
    hndl.emit("test_event")


def test_event_handler_pipe():
    parent = events.EventHandler()

    completed = False

    def check_event_origin(event: events.Event):
        nonlocal completed
        completed = True
        assert event.sender is parent, "Invalid sender for event object"

    child = events.EventHandler(on_event=check_event_origin)
    parent.pipe(child)
    parent.emit("check")

    assert completed, "Event did not propagate"


def test_event_handler_args():
    hndl = events.EventHandler()

    rslt = {
        "arg": None,
        "named_arg": None,
    }

    def handler_method(arg, named_arg=None):
        rslt["arg"] = arg
        rslt["named_arg"] = named_arg

    hndl.on("test_event", handler_method)
    hndl.emit("test_event", True, named_arg=True)

    assert rslt["arg"] is True, "Arg list not passed to event"
    assert rslt["named_arg"] is True, "Named arg list not passed to event"


def test_event_handler_event_invalid_arg_list():
    hndl = events.EventHandler()
    hndl.on("test_event", lambda: print("ok"))
    with pytest.raises(Exception):
        hndl.emit("test_event", True)
    with pytest.raises(Exception):
        hndl.emit("test_event", named_arg=True)


def test_events_stream_preload():
    hndl = events.EventHandler()
    strm = hndl.stream(timeout=0.1)
    hndl.emit("test_event")
    assert strm.__next__() is not None


def test_events_with_multiple_allowed_stream_preload():
    hndl = events.EventHandler()
    strm = hndl.stream(["a", "b"], timeout=0.1)
    hndl.emit("a")
    hndl.emit("b")
    assert strm.__next__().name == "a"
    assert strm.__next__().name == "b"


def test_events_stream_in_thread():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.01)
        hndl.emit("test_event")

    threading.Thread(target=send_event).start()
    strm = hndl.stream(timeout=0.1)

    assert strm.__next__() is not None


def test_events_stream_in_thread_error():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.01)
        hndl.emit_error(DummyExcpetion("Test stream event errors"))

    threading.Thread(target=send_event).start()
    strm = hndl.stream()
    with pytest.raises(Exception):
        strm.__next__()


def test_events_stream_no_throw_errors():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.01)
        hndl.emit_error(DummyExcpetion("Test stream event errors"))
        hndl.emit("test_event")

    threading.Thread(target=send_event).start()
    strm = hndl.stream(throw_errors=False)
    assert strm.__next__() is not None


def test_events_stream_in_thread_child_error():
    parent = events.EventHandler()
    child = events.EventHandler()
    child.pipe(parent)

    def send_event():
        child.emit_error(DummyExcpetion("Test stream event errors"))

    strm = parent.stream()
    threading.Thread(target=send_event).start()
    with pytest.raises(Exception):
        strm.__next__()


def test_events_streams_using_threads():
    hndl = events.EventHandler()

    def do_emit():
        for i in range(0, 4):
            hndl.emit("test")
            time.sleep(0.001)
        hndl.stop_all_streams()

    event_stream = hndl.stream("test")
    Task(do_emit).start()
    col = []
    for ev in event_stream:
        col.append(ev)
    assert len(col) == 4


def test_wait_for_self():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.1)
        hndl.emit("test_event")

    # asyncio will not work here :)
    Task(send_event).start()
    hndl.wait_for("test_event", timeout=1)


def test_wait_for_self_with_predict_error():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.1)
        hndl.emit("test_event")

    def predict(sender: events.EventHandler, event: events.Event):
        raise DummyExcpetion("error")

    # asyncio will not work here :)
    Task(send_event).start()
    with pytest.raises(DummyExcpetion):
        hndl.wait_for(predict, timeout=1)


def test_wait_for_self_predict():
    parent = events.EventHandler()
    child = events.EventHandler()

    parent.pipe(child)

    def send_event():
        time.sleep(0.1)
        parent.emit("test_event", 22)

    def predict(sender: events.EventHandler, event: events.Event):
        assert sender != event.sender
        assert event.name == "test_event"
        return True

    # asyncio will not work here :)
    Task(send_event).start()
    child.wait_for(predict=predict, timeout=1)


def test_wait_for_self_emit_error():
    parent = events.EventHandler()
    child = events.EventHandler()

    parent.pipe(child)

    def send_event():
        time.sleep(0.1)
        parent.emit_error(DummyExcpetion("from parent"))

    Task(send_event).start()
    with pytest.raises(DummyExcpetion):
        child.wait_for("test_event", timeout=1)


def test_wait_for_self_emit_error_no_raise():
    parent = events.EventHandler()
    child = events.EventHandler()

    parent.pipe(child)

    def send_event():
        time.sleep(0.1)
        parent.emit_error(DummyExcpetion("from parent"))

    Task(send_event).start()
    rsp = child.wait_for("test_event", timeout=1, raise_errors=False)
    assert isinstance(rsp, DummyExcpetion)


def test_wait_for_events():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.1)
        hndl.emit("test_event")

    # asyncio will not work here :)
    Task(send_event).start()
    rslt = hndl.wait_for_events("test_event", [hndl], timeout=1)
    assert rslt[0] is hndl, "Did not return correct handler"


def test_wait_for_events_none():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.1)
        hndl.emit("test_event")

    Task(send_event).start()
    rslt = hndl.wait_for_events(None, [hndl], timeout=1)
    assert rslt[0] is hndl, "Did not return correct handler"


def test_wait_for_events_predict():
    hndl = events.EventHandler()

    def send_event():
        time.sleep(0.1)
        hndl.emit("test_event")

    Task(send_event).start()
    rslt = hndl.wait_for_events(lambda sender, event: event.name == "test_event", [hndl], timeout=1)
    assert rslt[0] is hndl, "Did not return correct handler"


def test_wait_for_events_with_error():
    def send_event():
        time.sleep(0.1)
        raise DummyExcpetion()

    task = Task(send_event).start()

    with pytest.raises(DummyExcpetion):
        task.wait_for_events(lambda sender, name, *args: name == "test_event", [task], timeout=1)


# -----------------------------------------
# AsyncIO tests


@pytest.mark.asyncio
async def test_event_handler_event_asyncio():
    hndl = events.AsyncEventHandler()
    hndl.on("test_event", lambda: print("ok"))
    await hndl.emit("test_event")


@pytest.mark.asyncio
async def test_event_handler_args_async_asyncio():
    hndl = events.AsyncEventHandler()

    rslt = {
        "arg": None,
        "named_arg": None,
    }

    def handler_method(arg, named_arg=None):
        rslt["arg"] = arg
        rslt["named_arg"] = named_arg

    hndl.on("test_event", handler_method)
    await hndl.emit("test_event", True, named_arg=True)

    assert rslt["arg"] is True, "Arg list not passed to event"
    assert rslt["named_arg"] is True, "Named arg list not passed to event"


@pytest.mark.asyncio
async def test_event_handler_event_invalid_arg_list_async():
    hndl = events.AsyncEventHandler()
    hndl.on("test_event", lambda: print("ok"))
    with pytest.raises(Exception):
        await hndl.emit("test_event", True)
    with pytest.raises(Exception):
        await hndl.emit("test_event", named_arg=True)


@pytest.mark.asyncio
async def test_events_stream_stop_asyncio():
    hndl = events.EventHandler()

    async def send_event():
        await asyncio.sleep(0.01)
        hndl.stop_all_streams()

    # asyncio will not work here :)
    Task(send_event).start()
    strm = hndl.stream(timeout=1)

    for v in strm:
        pass


@pytest.mark.asyncio
async def test_events_stream_preload_asyncio():
    hndl = events.EventHandler()
    strm = hndl.stream(timeout=0.1, use_async_loop=True)
    hndl.emit("test_event")
    assert await strm.__anext__() is not None


@pytest.mark.asyncio
async def test_events_stream_in_corutine_asyncio():
    hndl = events.EventHandler()
    strm = hndl.stream(timeout=0.1, use_async_loop=True)

    async def send_event():
        hndl.emit("test_event")
        hndl.stop_all_streams()

    Task(send_event).start()
    await asyncio.sleep(0.01)  # allow the other task to execute.
    assert (await strm.__anext__()).name == "test_event"


if __name__ == "__main__":
    pytest.main(["-x", __file__])
