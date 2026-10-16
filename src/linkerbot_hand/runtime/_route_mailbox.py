"""Direct actor access to one bounded transport receive route."""

from __future__ import annotations

import threading
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Protocol, cast

from linkerbot_transport.errors import ClosedError as TransportClosedError

from ..api.errors import ProtocolError
from ._transport_binding import TransportBinding


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """Message and monotonic receive time read from a transport route."""

    message: object
    received_at_s: float
    received_order: int | None = None


class _RoutedMessage(Protocol):
    message: object
    received_at_s: float


class _RouteSubscription(Protocol):
    def try_receive(self) -> _RoutedMessage | None: ...

    def get_health(self) -> _RouteHealth: ...

    def close(self) -> None: ...


class _RouteHealth(Protocol):
    active: bool
    queue_depth: int


class RouteMailbox:
    """Expose a transport route directly to its single owning actor."""

    def __init__(
        self,
        *,
        binding: TransportBinding,
        route_keys: tuple[Hashable, ...],
        route_capacity: int,
        mailbox_capacity: int,
        wake: threading.Event,
    ) -> None:
        self._binding = binding
        self._route_keys = route_keys
        self._route_capacity = min(route_capacity, mailbox_capacity)
        self._wake = wake
        self._control_lock = threading.Lock()
        self._data_lock = threading.Lock()
        self._route: _RouteSubscription | None = None
        self._closing = False
        self._failure: BaseException | None = None

    def reset(self) -> None:
        """Prepare an empty route after the prior subscription has stopped."""

        with self._control_lock:
            if self._route is not None:
                raise ProtocolError("cannot reset an active receive route")
            self._closing = False
        with self._data_lock:
            self._failure = None

    def start(self) -> None:
        """Subscribe once and wake the actor when the route becomes available."""

        route = cast(
            _RouteSubscription,
            self._binding.subscribe_routes(
                self._route_keys,
                queue_capacity=self._route_capacity,
                on_available=self._wake.set,
            ),
        )
        with self._control_lock:
            if self._route is not None:
                route.close()
                raise ProtocolError("receive route is already active")
            self._closing = False
            self._route = route

    def close(self) -> None:
        """Stop route admission and wake the actor without waiting."""

        with self._control_lock:
            self._closing = True
            route = self._route
            self._route = None
        if route is not None:
            route.close()
        self._wake.set()

    def join(self, *, deadline_s: float) -> None:
        """Retain the former bounded-join surface; no copy thread remains."""

    def stop(self, *, deadline_s: float) -> None:
        """Close route admission; there is no receiver thread to join."""

        self.close()

    def pop(self) -> InboundMessage | None:
        """Return the next routed message without blocking."""

        with self._control_lock:
            route = self._route
        if route is None:
            return None
        try:
            routed = route.try_receive()
        except TransportClosedError as error:
            self._set_error_if_unexpected(route, error)
            return None
        except Exception as error:  # noqa: BLE001 - actor observes route failure
            self._set_error_if_unexpected(route, error)
            return None
        if routed is None:
            return None
        return InboundMessage(
            routed.message,
            routed.received_at_s,
            getattr(routed, "received_order", None),
        )

    def error(self) -> BaseException | None:
        """Return the receiver's terminal error, if one occurred."""

        with self._data_lock:
            return self._failure

    def has_pending(self) -> bool:
        """Return whether a routed message is waiting without removing it."""

        with self._control_lock:
            route = self._route
        return route is not None and route.get_health().queue_depth > 0

    def _set_error_if_unexpected(
        self,
        route: _RouteSubscription,
        error: BaseException,
    ) -> None:
        with self._control_lock:
            expected = self._closing or self._route is not route
        if not expected:
            self._set_error(error)

    def _set_error(self, error: BaseException) -> None:
        with self._data_lock:
            self._failure = error
        self._wake.set()


__all__ = ["InboundMessage", "RouteMailbox"]
