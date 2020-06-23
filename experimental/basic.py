from zthreading.events import EventHandler

handler = EventHandler()


def handle_test_event(msg: str):
    print("The event messge: " + msg)


handler.on("test", handle_test_event)
handler.emit("test", "the message")


from zthreading.events import EventHandler
from zthreading.tasks import Task

handler = EventHandler()


def handle_test_event(msg: str):
    print("The event messge: " + msg)


def run_in_a_different_thread(msg):
    handler.emit("test", msg)


handler.on("test", handle_test_event)
Task(run_in_a_different_thread).start("A message from a thread").join()

from zthreading.decorators import collect_consecutive_calls_async


@collect_consecutive_calls_async()
def consecutive_calls_action():  # Like save this to file.. for example.
    # should be printed twice, once for the first call, and another for the last call.
    print("consecutive called action")


for i in range(1, 20):
    consecutive_calls_action()
