"""Point-in-time views over the tick store: lookahead-bias protection.

A :class:`PointInTimeView` wraps a :class:`~tickpipe.store.reader.TickStore`
and freezes the latest exchange timestamp at which data is visible. Every
query through the view is unconditionally clamped to
``exchange_ts_ns <= as_of_ns``; the clamp is applied by the view itself and
cannot be widened after construction — a caller may only narrow it further by
passing an earlier ``as_of_ns``, and attempting to pass a later one raises
:class:`LookaheadError`.

Feature and backtest code must accept a ``PointInTimeView`` and never a raw
``TickStore``. All time filters use ``exchange_ts_ns`` (venue time) only;
``ingest_ts_ns`` must never be used for row selection.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from tickpipe.store.reader import TickStore
from tickpipe.store.writer import MAX_INT64

__all__ = ["LookaheadError", "PointInTimeView"]


class LookaheadError(Exception):
    """Raised when a caller asks for data later than a view's ``as_of_ns``."""


class PointInTimeView:
    """A read-only, lookahead-safe window over a :class:`TickStore`."""

    def __init__(self, store: TickStore, as_of_ns: int) -> None:
        if as_of_ns < 0:
            raise ValueError("as_of_ns must be non-negative")
        self._store = store
        self._as_of_ns = as_of_ns

    @property
    def as_of_ns(self) -> int:
        return self._as_of_ns

    def scan(
        self,
        symbols: Sequence[str] | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        *,
        as_of_ns: int | None = None,
    ) -> pa.Table:
        """Scan ticks, unconditionally clamped to ``exchange_ts_ns <= as_of``.

        A caller may pass ``as_of_ns`` only to narrow the view further; a
        value later than the view's own ``as_of_ns`` raises
        :class:`LookaheadError`. An ``end_ns`` beyond the as-of is silently
        clamped, never widened.
        """
        if as_of_ns is not None and as_of_ns > self._as_of_ns:
            raise LookaheadError(
                f"as_of_ns {as_of_ns} exceeds this view's as_of_ns {self._as_of_ns}"
            )
        effective_as_of = self._as_of_ns if as_of_ns is None else min(self._as_of_ns, as_of_ns)
        effective_end = self._clamped_end_ns(end_ns, effective_as_of)
        return self._store.scan(
            symbols=symbols,
            start_ns=start_ns if start_ns is not None else 0,
            end_ns=effective_end,
        )

    @staticmethod
    def _clamped_end_ns(end_ns: int | None, as_of_ns: int) -> int | None:
        if end_ns is None or end_ns > as_of_ns:
            return None if as_of_ns >= MAX_INT64 else as_of_ns + 1
        return end_ns
