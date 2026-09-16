"""Per-symbol sequence validation and event-time reordering.

The venue sequence number on each tick is validated against a per-symbol
high-water mark:

* a *gap* is a jump of more than one in the sequence (``seq > high_water + 1``);
* an *out-of-order* message arrives with ``seq < high_water``;
* a message repeating ``high_water`` exactly is a duplicate and produces no
  event (it still flows downstream so consumers can deduplicate);

Messages with ``sequence == 0`` carry no venue sequence (e.g. Coinbase
level-2 updates) and bypass sequence tracking entirely.

Every message also passes through a small reorder buffer keyed by
``exchange_ts_ns`` (event time). Messages are held for a configurable window
(default 50 ms of event time); once event time has advanced beyond the window,
all expired messages are released in exchange-timestamp order.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from tickpipe.core.types import Tick

DEFAULT_REORDER_WINDOW_NS: Final[int] = 50_000_000

NO_VENUE_SEQUENCE: Final[int] = 0


class GapEvent(BaseModel):
    """A detected jump of more than one in the venue sequence."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    expected_sequence: int
    received_sequence: int
    gap_size: int
    exchange_ts_ns: int


class OutOfOrderEvent(BaseModel):
    """A message with a sequence number below the high-water mark."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    high_water_sequence: int
    received_sequence: int
    exchange_ts_ns: int


SequencingEvent = GapEvent | OutOfOrderEvent


@dataclass(frozen=True)
class SequenceResult:
    """Outcome of processing one tick: messages ready for downstream plus events."""

    ready: list[Tick]
    events: list[SequencingEvent]
    reordered: bool = False


class SequenceTracker:
    """Tracks sequences for a single symbol and reorders by event time."""

    def __init__(self, symbol: str, reorder_window_ns: int = DEFAULT_REORDER_WINDOW_NS) -> None:
        if reorder_window_ns < 0:
            raise ValueError("reorder_window_ns must be >= 0")
        self.symbol = symbol
        self.reorder_window_ns = reorder_window_ns
        self.high_water_sequence: int | None = None
        self._max_exchange_ts_ns: int | None = None
        self._buffer: list[Tick] = []
        self.reordered_count = 0

    def process(self, msg: Tick) -> SequenceResult:
        events: list[SequencingEvent] = []
        seq = msg.sequence
        if seq != NO_VENUE_SEQUENCE:
            high_water = self.high_water_sequence
            if high_water is None:
                self.high_water_sequence = seq
            elif seq > high_water + 1:
                events.append(
                    GapEvent(
                        symbol=msg.symbol,
                        expected_sequence=high_water + 1,
                        received_sequence=seq,
                        gap_size=seq - high_water - 1,
                        exchange_ts_ns=msg.exchange_ts_ns,
                    )
                )
                self.high_water_sequence = seq
            elif seq < high_water:
                events.append(
                    OutOfOrderEvent(
                        symbol=msg.symbol,
                        high_water_sequence=high_water,
                        received_sequence=seq,
                        exchange_ts_ns=msg.exchange_ts_ns,
                    )
                )
            elif seq == high_water:
                pass
            else:
                self.high_water_sequence = seq

        insert_at = bisect.bisect_right(
            self._buffer, msg.exchange_ts_ns, key=lambda m: m.exchange_ts_ns
        )
        self._buffer.insert(insert_at, msg)

        reordered = (
            self._max_exchange_ts_ns is not None
            and msg.exchange_ts_ns < self._max_exchange_ts_ns
        )
        if reordered:
            self.reordered_count += 1
        if self._max_exchange_ts_ns is None or msg.exchange_ts_ns > self._max_exchange_ts_ns:
            self._max_exchange_ts_ns = msg.exchange_ts_ns

        ready = self._expire(msg.exchange_ts_ns)
        return SequenceResult(ready=ready, events=events, reordered=reordered)

    def flush(self) -> list[Tick]:
        """Release everything still buffered, in exchange-timestamp order."""
        ready = self._buffer
        self._buffer = []
        return ready

    def _expire(self, current_ts_ns: int) -> list[Tick]:
        threshold = current_ts_ns - self.reorder_window_ns
        split_at = bisect.bisect_right(self._buffer, threshold, key=lambda m: m.exchange_ts_ns)
        if split_at == 0:
            return []
        ready = self._buffer[:split_at]
        del self._buffer[:split_at]
        return ready


class Sequencer:
    """One :class:`SequenceTracker` per symbol."""

    def __init__(self, reorder_window_ns: int = DEFAULT_REORDER_WINDOW_NS) -> None:
        self.reorder_window_ns = reorder_window_ns
        self._trackers: dict[str, SequenceTracker] = {}

    def process(self, msg: Tick) -> SequenceResult:
        tracker = self._trackers.setdefault(
            msg.symbol, SequenceTracker(msg.symbol, self.reorder_window_ns)
        )
        return tracker.process(msg)

    def flush(self) -> list[Tick]:
        """Release remaining buffered messages from all symbols, sorted by event time."""
        ready: list[Tick] = []
        for tracker in self._trackers.values():
            ready.extend(tracker.flush())
        ready.sort(key=lambda m: m.exchange_ts_ns)
        return ready

    @property
    def reordered_count(self) -> int:
        return sum(t.reordered_count for t in self._trackers.values())


__all__ = [
    "DEFAULT_REORDER_WINDOW_NS",
    "GapEvent",
    "NO_VENUE_SEQUENCE",
    "OutOfOrderEvent",
    "SequenceResult",
    "SequenceTracker",
    "Sequencer",
    "SequencingEvent",
]
