import pytest
from time import sleep
from threading import Thread
from queue import Queue
from zthreading.thread_queue import Queue


def test_simple_queue():
    queue = Queue()

    def do_enqueue():
        sleep(0.1)
        queue.put("valid")
        pass

    Thread(target=do_enqueue).start()
    assert queue.get(timeout=0.2) == "valid"


if __name__ == "__main__":
    # test_simple_queue()
    pytest.main(["-x", __file__])
