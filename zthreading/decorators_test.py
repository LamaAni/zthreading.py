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

    @decorators.collect_consecutive_calls_async(on_error="async_error")
    def collected(self):
        time.sleep(0.01)
        if self.throw_error:
            raise Exception("test")
        self.collected_count += 1

    @decorators.collect_delayed_calls_async(
        on_error="async_error",
        interval=DUMMY_TESTER_DELAY_INTERVAL,
        max_delay=DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN / 2,
    )
    def delayed_collected(self):
        time.sleep(0.01)
        if self.throw_error:
            raise Exception("test")
        self.collected_count += 1

    def async_error(self, err):
        self.last_error = err


def test_collect_consecutive_calls_async():
    tester = dummytester()
    for i in range(0, 100):
        tester.collected()
    time.sleep(0.1)
    assert tester.collected_count == 2


def test_collect_consecutive_calls_async_exception():
    tester = dummytester(True)
    tester.collected()

    time.sleep(0.1)

    assert tester.last_error is not None


def test_collect_delayed_calls_async():
    tester = dummytester()
    for i in range(0, 100):
        tester.delayed_collected()

    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)

    assert tester.collected_count == 2, f"Collected count was {tester.collected_count}"


def test_collect_delayed_calls_async_exception():
    tester = dummytester(True)
    tester.delayed_collected()

    time.sleep(DUMMY_TESTER_DELAY_SHOULD_COMPLETE_WITHIN)

    assert tester.last_error is not None


if __name__ == "__main__":
    # test_collect_delayed_calls_async_exception()
    pytest.main(["-x", __file__])
