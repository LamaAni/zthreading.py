import pytest
from zthreading.signals import SignalsEventHandler, FrameType, Signals
from signal import raise_signal
from time import sleep


def test_register_and_destroy():
    handler = SignalsEventHandler()
    is_caught = False  # caught

    def do_on_signal(signal: Signals, frame: FrameType):
        nonlocal is_caught
        is_caught = True

    handler.on(Signals.SIGTERM, do_on_signal)
    raise_signal(Signals.SIGTERM.value)

    assert is_caught, "Signal not caught"
    handler.clear(Signals.SIGTERM)


if __name__ == "__main__":
    test_register_and_destroy()
    raise_signal(Signals.SIGTERM.value)
    while True:
        print(1)
        sleep(1)
