import pytest
from zthreading.signals import SignalsEventHandler, FrameType, Signals
from signal import raise_signal
from time import sleep


def test_signal_handler():
    handler = SignalsEventHandler()
    is_caught = False  # caught

    def do_on_signal(signal: Signals, frame: FrameType):
        nonlocal is_caught
        is_caught = True

    handler.on(Signals.SIGTERM, do_on_signal)
    raise_signal(Signals.SIGTERM.value)
    handler.clear(Signals.SIGTERM)

    assert is_caught, "Signal not caught"


if __name__ == "__main__":
    test_signal_handler()
    raise_signal(Signals.SIGTERM)
    print("ALLOK")
