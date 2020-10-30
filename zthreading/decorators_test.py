import pytest
import time
from zthreading import decorators
from zthreading.tasks import Task
from zthreading.signals import Signals
from signal import raise_signal

DUMMY_TESTER_DELAY_INTERVAL = 0.01
DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN = DUMMY_TESTER_DELAY_INTERVAL * 10


class dummytester:
    def __init__(self, throw_error=False):
        super().__init__()
        self.collected_count = 0
        self.throw_error = throw_error
        self.last_error = None

    def _collect(self):
        time.sleep(DUMMY_TESTER_DELAY_INTERVAL / 10)
        if self.throw_error:
            raise Exception("test")
        self.collected_count += 1

    @decorators.collect_consecutive_calls_async(on_error="async_error")
    def collect(self):
        self._collect()

    @decorators.collect_consecutive_calls_async(
        on_error="async_error",
        ignore_waiting_calls_timeout=DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN,
    )
    def collect_once(self):
        self._collect()

    @decorators.collect_delayed_calls_async(
        on_error="async_error",
        interval=DUMMY_TESTER_DELAY_INTERVAL,
        max_delay=DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN / 2,
    )
    def delayed_collect(self):
        self._collect()

    def async_error(self, err):
        self.last_error = err


def test_as_task():
    was_set = False

    @decorators.as_task()
    def as_task():
        nonlocal was_set
        time.sleep(0.01)
        assert was_set, "Task called before the value"

    t: Task = as_task()
    was_set = True
    t.join()


def test_collect_consecutive_calls_async():
    tester = dummytester()
    for i in range(0, 100):
        tester.collect()
    time.sleep(0.1)
    assert tester.collected_count == 2, f"Collected count was {tester.collected_count}"


def test_collect_consecutive_calls_async_exception():
    tester = dummytester(True)
    tester.collect()

    time.sleep(0.1)

    assert tester.last_error is not None


def test_collect_consecutive_calls_ones_async():
    tester = dummytester()
    for i in range(0, 100):
        tester.collect_once()
    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)
    assert tester.collected_count == 1, f"Collected count was {tester.collected_count}"


def test_collect_consecutive_calls_once_async_exception():
    tester = dummytester(True)
    tester.collect_once()

    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)

    assert tester.last_error is not None


def test_collect_delayed_calls_async():
    tester = dummytester()
    for i in range(0, 10):
        tester.delayed_collect()

    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)

    assert tester.collected_count == 1, f"Collected count was {tester.collected_count}"


def test_collect_delayed_calls_async_exception():
    tester = dummytester(True)
    tester.delayed_collect()

    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)

    assert tester.last_error is not None


def test_catch_signal():
    send_signal = Signals.SIGTERM
    executed = False

    @decorators.catch_signal(send_signal)
    def do_action():
        raise_signal(send_signal)
        nonlocal executed
        executed = True

    do_action()
    assert executed


def test_catch_signal_with_do():
    send_signal = Signals.SIGTERM
    caught = False
    executed = False

    def do_on_signal(signal: Signals, frame):
        assert signal == send_signal, "Invalid signal caught"
        nonlocal caught
        caught = True

    @decorators.catch_signal(send_signal, do_on_signal=do_on_signal)
    def do_action():
        raise_signal(send_signal)
        nonlocal executed
        executed = True
        return 1

    assert do_action()==1
    assert caught and executed


if __name__ == "__main__":
    pytest.main(["-x", __file__])
