from signal import Signals
from types import FrameType
from signal import Handlers, default_int_handler
from signal import signal as set_signal_handler
from signal import getsignal as get_signal_handler
from typing import Union, Set, Callable, List, Dict
from zthreading.events import EventHandler
from weakref import WeakKeyDictionary
from enum import Enum


class SignalsEventHandlerException(Exception):
    pass


VALUE_ERROR_MESSAGE = "signal must be of type signals.Signals, a non empty string, int (or a list of any of these)"


def signal_action(signal: Signals, frame: FrameType):
    pass


def to_signal(s):
    assert s is not None, ValueError(VALUE_ERROR_MESSAGE)
    if isinstance(s, int):
        s = Signals(s)
    if isinstance(s, str):
        assert s, ValueError(VALUE_ERROR_MESSAGE)
        return getattr(Signals, s)
    return s


class SignalsRegisterAction:
    def __init__(self, signal: Signals, action: Callable, override_default: bool = True) -> None:
        self.signal = signal
        self.action = action
        self.override_default = override_default


class SignalsRegister:
    def __init__(self) -> None:
        self._actions: Dict[Signals, Dict[object, SignalsRegisterAction]] = dict()
        self._original_signals: Dict[Signals, Callable] = dict()

    def _add_object_action(self, signal: Signals, object, action: SignalsRegisterAction):
        if signal not in self._actions:
            self._actions[signal] = WeakKeyDictionary()
        self._actions[signal][object] = action

    def _get_actions(self, signal: Signals):
        if signal not in self._actions:
            return []
        return self._actions[signal].values()

    def handle_signal_action(self, signal: int, frame: FrameType):
        signal = to_signal(signal)
        needs_original_action: bool = True
        for action in self._get_actions(signal):
            if action.signal != signal:
                continue
            if action.override_default:
                needs_original_action = False
            action.action(signal, frame)

        if needs_original_action:
            original = self._original_signals[signal]
            if isinstance(original, int):
                original = Handlers(original)
            if isinstance(original, Handlers):
                if original == Handlers.SIG_IGN:
                    original = None
                else:
                    original = default_int_handler

            if original is not None:
                original(signal.value, frame)

    def register_signal_action(
        self,
        caller: object,
        signal: Union[int, str, Signals],
        action: Callable,
        override_default: bool = True,
    ):
        signal = to_signal(signal)
        if signal not in self._original_signals:
            self._original_signals[signal] = get_signal_handler(signal.value)
            set_signal_handler(signal, self.handle_signal_action)

        self._add_object_action(
            signal,
            caller,
            SignalsRegisterAction(
                signal=signal,
                action=action,
                override_default=override_default,
            ),
        )

    def clear_signal_action(self, caller, signal: Signals):
        if signal not in self._actions:
            return False
        if caller not in self._actions[signal]:
            return False
        del self._actions[signal][caller]


global SIGNALS_REGISTER
try:
    SIGNALS_REGISTER
except NameError:
    # Not defined.
    SIGNALS_REGISTER = SignalsRegister()


class SignalsEventHandler(EventHandler):
    def __init__(self, on_event=None) -> None:
        super().__init__(on_event=on_event)
        self._active_signals: Set[Signals] = set()

    @property
    def active_signals(self) -> Set[Signals]:
        return set(self._active_signals)

    def _process_signal_response(self, signal: Signals, frame: FrameType):
        self.emit(signal.name, signal, frame)

    def on(
        self,
        signal: Union[Signals, str, int, List[str], List[int], List[Signals]],
        action: Callable,
        override_default_handler: bool = True,
    ) -> int:
        """Add a new signal event handler.

        Arguments:
            signal {str|Signals|List[str]|List[Signals]} -- The event name
            action {Callable[signal: Signals, frame: FrameType]} --
                The method/other to be called when the event is triggered.

        Returns:
            int|List[int] -- The event index, within the specific name
                dictionary. name+index are unique.
        """

        if not isinstance(signal, list):
            signal = [to_signal(signal)]
        else:
            signal = [to_signal(s) for s in signal]

        assert all([isinstance(s, Signals) for s in signal]), ValueError(
            "signal must be of type signals.Signals or a non empty string (of a list of them)"
        )

        for s in set(signal):
            SIGNALS_REGISTER.register_signal_action(
                caller=self,
                signal=s,
                action=self._process_signal_response,
            )

        super().on(
            name=[s.name for s in signal],
            action=action,
        )

    def clear(self, signal: Union[Signals, str, int], idx: int = None):
        """Clears event action(s).

        Arguments:
            name {Union[Signals, str, int]} -- The signal to clear.

        Keyword Arguments:
            idx {int|List[int]} -- If not None, will clear the specific
                call to the event identified by the event index
                returned from the on method. Otherwise will clear
                ALL events of name (default: {None})
        Returns:
            bool - True, ff all clear events succeeded.
        """
        if isinstance(signal, list):
            return all([self.clear(s) for s in signal])

        signal = to_signal(signal)
        rt = super().clear(signal.name, idx)
        if not self.has_event(signal.name):
            SIGNALS_REGISTER.clear_signal_action(caller=self, signal=signal)
        return rt
