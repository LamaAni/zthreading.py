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
