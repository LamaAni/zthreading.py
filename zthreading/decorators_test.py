import pytest
import time
from zthreading import decorators

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


if __name__ == "__main__":
    pytest.main(["-x", __file__])
