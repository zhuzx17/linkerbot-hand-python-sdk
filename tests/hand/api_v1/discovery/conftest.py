"""Keep synchronous discovery semantics independent of runner scheduling."""

import pytest

from linkerbot_hand.discovery import _budget, _engine
from linkerbot_transport import RouteSubscription
from linkerbot_transport.errors import TimeoutError as RxTimeout
from linkerbot_transport.routing import _validate_timeout

from ._clock import DiscoveryClock


@pytest.fixture(autouse=True)
def discovery_clock(request, monkeypatch):
    """Advance scan time only for explicit delays or an empty receive inbox.

    Discovery peers in this directory reply synchronously inside send(). Keep
    the real router, receipt timestamps and event ordering; only scan waiting
    is simulated. Runtime calls after discovery and other threads retain their
    normal blocking receives. Indirect ``real`` parametrization preserves the
    separate wall-clock budget check.
    """

    if getattr(request, "param", "logical") == "real":
        yield None
        return

    clock = DiscoveryClock()
    monkeypatch.setattr(_engine, "time", clock)
    monkeypatch.setattr(_budget, "time", clock)
    receive = RouteSubscription.receive

    def receive_during_discovery(subscription, *, timeout_s=None):
        if _budget.deadline.get() is None:
            return receive(subscription, timeout_s=timeout_s)
        _validate_timeout(timeout_s)
        assert timeout_s is not None, "discovery receives must be bounded"
        try:
            return receive(subscription, timeout_s=0.0)
        except RxTimeout:
            clock.sleep(timeout_s)
            raise

    monkeypatch.setattr(RouteSubscription, "receive", receive_during_discovery)
    yield clock
