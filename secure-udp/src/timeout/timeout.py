from threading import Timer
from collections.abc import Callable
from dataclasses import dataclass

@dataclass
class MessageTimeout:
    interval: float
    addr: tuple[str, int]
    timer: Timer | None = None

    def reset_timer(self, action: Callable, sequence: int):
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(
            interval=self.interval,
            function=action,
            args=[self.addr, sequence]
        )

        self.timer.start()

    def cancel(self):
        if self.timer:
            self.timer.cancel()

    def is_alive(self):
        return self.timer.is_alive() if self.timer is not None else False

        