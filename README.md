# zthreading.py

A collection of wrapper classes for event broadcast and task management for python (Python Threads or Asyncio)

# TL;DR

## Events

```python
from zthreading.events import EventHandler

handler = EventHandler()


def handle_test_event(msg: str):
    print("The event messge: " + msg)


handler.on("test", handle_test_event)
handler.emit("test", "the message")
```

## Tasks

```python
from zthreading.events import EventHandler
from zthreading.tasks import Task

handler = EventHandler()


def handle_test_event(msg: str):
    print("The event messge: " + msg)


def run_in_a_different_thread(msg):
    handler.emit("test", msg)


handler.on("test", handle_test_event)
Task(run_in_a_different_thread).start("A message from a thread").join()
```

## Decorators

```python
from zthreading.decorators import collect_consecutive_calls_async


@collect_consecutive_calls_async()
def consecutive_calls_action():  # Like save this to file.. for example.
    # should be printed twice, once for the first call, and another for the last call.
    print("consecutive called action")


for i in range(1, 20):
    consecutive_calls_action()
```

# Environment variables

1. TASKS_DEFAULT_TO_ASYNC_LOOP - If set to "true", will default all tasks to use asyncio.
1. TASKS_ERROR_EVENT_NAME - The name of the error event, emitted on any task errors. Defaults to "error".

# Advanced Methods and capabilities

Note: The task object is an EventHandler and has all the capabilities of one.

## Task wait functions (staticmethod)

(Examples in code)

1. Task.wait_for_all (tasks.... )
1. Task.wait_for_some (tasks... )
1. Task.wait_for_one (tasks...)
1. Task.wait_for_events(tasks, event names....)

## Piping events

Transferring events from one handler to another. If a weak reference is used
then then the second handler can be deleted by garbage collection.

```python
from zthreading.events import EventHandler

handler_a = EventHandler()
handler_b = EventHandler()

# Transfer all events to handler b, as
# long as handler b is object is in memory. Will
# not keep handler_b in memory.
handler_a.pipe(handler_b, use_weak_reference=True)


def handle_test_event(msg: str):
    print("The event messge: " + msg)


handler_b.on("test", handle_test_event)
handler_a.emit("test", "The piped message")
```

## Streaming events and using tasks to do it.

Events can be streamed (yield generator),

```python
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
```

# Install

```shell
pip install zthreading
```

## From the git repo directly

To install from master branch,

```shell
pip install git+https://github.com/LamaAni/zthreading.py.git@master
```

To install from a release (tag)

```shell
pip install git+https://github.com/LamaAni/zthreading.py.git@[tag]
```

# Contribution

Feel free to ping me in issues or directly on LinkedIn to contribute.

# Licence

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/postgres-xl-helm/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in `LICENSE`.
