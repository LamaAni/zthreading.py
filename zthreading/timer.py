from datetime import datetime, timedelta


class TicTocTimer(object):
    _timestamp_map: dict = None

    def __init__(self):
        super().__init__()
        self._timestamp_map = dict()

    def tic(self, name: str = "global") -> datetime:
        ts = datetime.now()
        self._timestamp_map[name] = ts
        return ts

    def toc(self, name: str = "global") -> timedelta:
        if name not in self._timestamp_map:
            return None
        return datetime.now() - self._timestamp_map[name]


GLOBAL_TIMER = TicTocTimer()


def tic(name: str = "global") -> datetime:
    return GLOBAL_TIMER.tic(name)


def toc(name: str = "global") -> timedelta:
    return GLOBAL_TIMER.toc(name)


if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
