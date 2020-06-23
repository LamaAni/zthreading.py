import pytest
from time import sleep
from src.operations.timer import tic, toc


def test_global_timer():
    tic()
    sleep(0.01)
    assert toc() is not None and toc().total_seconds() > 0, "Invalid global timer"


def test_global_timer_with_name():
    tic("test_global_timer_with_name")
    sleep(0.01)
    assert (
        toc("test_global_timer_with_name") is not None and toc("test_global_timer_with_name").total_seconds() > 0
    ), "Invalid global timer"


def test_local_timer_name_error():
    tic()
    assert (
        toc("test_local_timer_name_error") is None
    ), "Received toc although tick not called for name test_local_timer_name_error"


if __name__ == "__main__":
    pytest.main(["-x", __file__])
