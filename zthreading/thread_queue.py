import sys

# assert sys.version_info.major > 2, "zthreading.py dose not support python versions less than 3"
if sys.version_info.major > 3 or sys.version_info.minor > 6:
    from queue import SimpleQueue as Queue
else:
    from queue import Queue as Queue

if __name__ == "__main__":
    import pytest

    pytest.main(["-x", __file__[:-3] + "_test.py"])
