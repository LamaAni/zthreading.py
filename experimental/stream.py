from random import random
from time import sleep
from zthreading.tasks import Task

# A task is an EventHandler, and has the
# on method as well.
task: Task = None


def invoke_timed_events():
    sleep(1)
    for i in range(1, 10):
        sleep(random() / 10)
        task.emit("test", f"loop index {i}")
    task.stop_all_streams()


task = Task(invoke_timed_events).start()

for ev in task.stream("test"):
    print(f"{ev.name}, {ev.args[0]}")
