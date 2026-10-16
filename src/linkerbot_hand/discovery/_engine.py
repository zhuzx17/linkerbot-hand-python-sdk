"""Identity-only discovery orchestration across classic CAN and CAN FD."""

from __future__ import annotations

import errno
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from typing import cast

import can

from linkerbot_transport import (
    CANRouteKey,
    RouteSubscription,
    SendOutcome,
    SendResult,
    TransportState,
)
from linkerbot_transport import ClosedError as TransportClosedError
from linkerbot_transport import DeviceNotReadyError as TransportNotReadyError
from linkerbot_transport import TimeoutError as RxTimeout
from linkerbot_transport import UnsupportedOperationError as TransportUnsupportedError
from linkerbot_transport import ValidationError as TransportValidationError
from linkerbot_transport.canfd import CANFDMessage

from ..api.enums import HandSide, TransportKind
from ..api.errors import (
    HandError,
    IdentityMismatchError,
    ProtocolError,
    ValidationError,
)
from ..capabilities.registry import get_declared_capabilities
from ..protocols.base import IdentityResult, ProtocolTransaction
from ..protocols.common.classic_can import (
    ClassicIdentityCollector,
    ClassicIdentityConflictError,
    build_classic_message,
    classic_route,
    matching_data,
    parse_factory_identity,
)
from ..protocols.l30.codec import PARENT_CONFIG, SUB_DEVICE_INFO, SUB_PRODUCT_CODE
from ..protocols.l30.framing import build_request as build_l30_request
from ..protocols.l30.framing import parse_can_id as parse_l30_can_id
from ..protocols.l30.framing import response_route as l30_response_route
from ..protocols.l30.identity import l30_identity_transaction
from ..protocols.o20.codec import (
    REG_DEVICE_INFO,
    device_id_for_side,
)
from ..protocols.o20.codec import (
    build_read_message as build_o20_read,
)
from ..protocols.o20.codec import (
    read_route as o20_read_route,
)
from ..protocols.o20.identity import o20_identity_transaction
from ..protocols.o30.codec import MI_PRODUCT_INFO
from ..protocols.o30.framing import (
    HOPAddressing,
    matching_response_frame,
)
from ..protocols.o30.framing import (
    build_read_message as build_o30_read,
)
from ..protocols.o30.framing import (
    response_route as o30_response_route,
)
from ..protocols.o30.identity import o30_identity_transaction
from ..protocols.registry import create_protocol_backend
from . import _budget
from ._classic_routes import (
    CLASSIC_BROADCAST_ROUTE,
    CLASSIC_DEFAULT_ROUTES,
    CLASSIC_DISCOVERY_ROUTES,
)
from .contracts import (
    DiscoveryAddress,
    DiscoveryCandidate,
    DiscoveryChannel,
    DiscoveryConfig,
    DiscoveryFailure,
    DiscoveryIdentity,
    DiscoveryProblem,
    DiscoveryReport,
    DiscoverySource,
    DiscoveryStatus,
    TransportSpec,
)
from .errors import DiscoveryError, NoHandFoundError
from .sources import PlatformDiscoverySource, _minicanfd_fallback, enumerate_source

_CLASSIC_RESPONSE_GUARD_S = 0.02

_CLASSIC_MODELS = {
    "LHL6": "L6",
    "LHO6": "O6",
    "LHT10": "L20Lite",
    # LHT20 is the current factory prefix. L25/LHL25 and LHG20 occur on
    # deployed devices; every wire identifier maps to the public L20 profile.
    "L20": "L20",
    "LHT20": "L20",
    "L25": "L20",
    "LHL25": "L20",
    "LHG20": "L20",
}


class _NoResponse(Exception):
    def __init__(self, *, matched: bool) -> None:
        super().__init__("identity response timed out")
        self.matched = matched


class _ResponseError(Exception):
    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


class _TransportProbeError(Exception):
    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


def discover_hands(
    config: DiscoveryConfig | None = None,
    *,
    source: DiscoverySource | None = None,
) -> DiscoveryReport:
    """Actively read identity from all selected channels and then close them."""

    resolved_config = DiscoveryConfig() if config is None else config
    if not isinstance(resolved_config, DiscoveryConfig):
        raise DiscoveryError(
            "config must be DiscoveryConfig", operation="discover_hands"
        )
    resolved_source = PlatformDiscoverySource() if source is None else source
    if not isinstance(resolved_source, DiscoverySource):
        raise DiscoveryError(
            "source must implement DiscoverySource",
            operation="discover_hands",
        )
    started = time.monotonic()
    token = _budget.deadline.set(started + resolved_config.total_timeout_s)
    try:
        return _discover(resolved_config, resolved_source, started)
    finally:
        _budget.deadline.reset(token)


def _discover(
    resolved_config: DiscoveryConfig, resolved_source: DiscoverySource, started: float
) -> DiscoveryReport:
    try:
        enumeration = enumerate_source(resolved_source, resolved_config)
        channels = enumeration.channels
    except (DiscoveryError, ValidationError):
        raise
    except Exception as error:
        raise DiscoveryError(
            "discovery source could not list channels",
            operation="discover_hands",
            cause=error,
        ) from error
    if not isinstance(channels, tuple) or any(
        not isinstance(channel, DiscoveryChannel) for channel in channels
    ):
        raise DiscoveryError(
            "discovery source returned invalid channels",
            operation="discover_hands",
        )
    candidates: list[DiscoveryCandidate] = []
    failures = list(enumeration.failures)
    report_channels = list(channels)
    for channel in channels:
        if _budget.expired():
            break
        first_failure = len(failures)
        _probe_channel(resolved_source, channel, resolved_config, candidates, failures)
        if (
            _budget.expired()
            or any(candidate.channel == channel for candidate in candidates)
            or any(f.cleanup is not None for f in failures[first_failure:])
        ):
            continue
        try:
            fallback = _minicanfd_fallback(channel, resolved_config)
        except Exception as error:  # noqa: BLE001 - independent adapter boundary
            _record_failure(
                failures,
                operation="enumerate_minicanfd_fallback",
                error=error,
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
            )
            continue
        if fallback is None:
            continue
        failures.extend(fallback.failures)
        if any(
            failure.code in {"library", "busy", "lifecycle", "scan", "cleanup"}
            for failure in fallback.failures
        ):
            # The fallback could not run; retain its driver/lifecycle diagnosis
            # instead of claiming a completed search found no hand.
            continue
        report_channels.extend(fallback.channels)
        for next_channel in fallback.channels:
            if _budget.expired():
                break
            _probe_channel(
                resolved_source, next_channel, resolved_config, candidates, failures
            )
        if not _budget.expired() and not any(
            candidate.channel in (channel, *fallback.channels)
            for candidate in candidates
        ):
            adapter = channel.name.rsplit(":", 1)[0]
            failures.append(
                DiscoveryFailure(
                    operation="probe_minicanfd_channels",
                    code=NoHandFoundError.code.value,
                    message=f"no hand found on {adapter} channels 0 or 1",
                    error_type=NoHandFoundError.__name__,
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    backend="minicanfd",
                )
            )
    if _budget.expired() and not any(f.code == "scan_timeout" for f in failures):
        failures.append(
            DiscoveryFailure(
                operation="discover_hands",
                code="scan_timeout",
                message="discovery total time budget exhausted; remaining probes were skipped",
                error_type="TimeoutError",
            )
        )
    return DiscoveryReport(
        channels=tuple(report_channels),
        candidates=tuple(candidates),
        failures=tuple(failures),
        elapsed_s=max(0.0, time.monotonic() - started),
    )


def _probe_channel(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> None:
    for wire in channel.probe_protocols or ():
        if _budget.expired():
            break
        failed = False
        if wire is TransportKind.CAN and _channel_matches_classic(channel):
            failed = _probe_classic(source, channel, config, candidates, failures)
        elif wire is TransportKind.CAN_FD:
            failed = _probe_can_fd(source, channel, config, candidates, failures)
        if failed or any(
            c.channel == channel and c.status is DiscoveryStatus.VERIFIED
            for c in candidates
        ):
            break


def _probe_classic(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> bool:
    transport: object | None = None
    failed = False
    mode_initialization = True
    products: dict[CANRouteKey, DiscoveryCandidate] = {}
    try:
        _budget.remaining(config.total_timeout_s)
        transport = source.create_transport(channel, TransportKind.CAN)
        _open_transport(transport)
        mode_initialization = False
        for attempt, request_route in enumerate(
            (CLASSIC_BROADCAST_ROUTE, *CLASSIC_DEFAULT_ROUTES)
        ):
            routes = CLASSIC_DISCOVERY_ROUTES if attempt == 0 else (request_route,)
            messages = _collect(
                transport,
                routes,
                (build_classic_message(request_route, b"\xc0"),),
                config,
                discard_s=_CLASSIC_RESPONSE_GUARD_S,
            )
            found: list[DiscoveryCandidate] = []
            for route, replies in _group_by_route(messages).items():
                responses = tuple(
                    message
                    for message in replies
                    if (data := matching_data(message, route)) is not None
                    and data.startswith(b"\xc0")
                    and len(data) > 1
                )
                if not responses:
                    continue
                product = _classic_product_candidate(source, channel, route, responses)
                # Replace a previous incomplete attempt; never join its fragments.
                products[route] = product
                found.append(product)
            if any(
                product.problems[0].code != "product_code_incomplete"
                for product in found
            ):
                break
        for product in products.values():
            candidates.append(
                _verify_classic_candidate(transport, product, config, failures)
            )
    except Exception as error:  # noqa: BLE001 - discovery source boundary
        # A mode-specific construction/open failure is isolated from the next
        # explicitly requested wire mode. Once the mode is open, a dead link
        # still stops the channel and prevents sends on a failed generation.
        failed = _fatal_link_error(error, transport, initializing=mode_initialization)
        _record_failure(
            failures,
            operation="probe_classic_identity",
            error=error,
            channel=channel,
            wire_protocol=TransportKind.CAN,
        )
        # A failed fallback must not erase evidence from the broadcast attempt.
        for product in products.values():
            if not any(
                candidate.channel == channel and candidate.address == product.address
                for candidate in candidates
            ):
                candidates.append(
                    replace(
                        product,
                        problems=(
                            DiscoveryProblem(
                                "transport_error",
                                "transport failed during classic discovery",
                            ),
                        ),
                    )
                )
    finally:
        if transport is not None:
            try:
                _close_transport(transport)
            except Exception as error:  # noqa: BLE001 - transport boundary
                failed = True
                _record_failure(
                    failures,
                    operation="close_classic_probe_transport",
                    error=error,
                    channel=channel,
                    wire_protocol=TransportKind.CAN,
                    cleanup=partial(_close_transport, transport),
                )
    return failed


def _classic_product_candidate(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    route: CANRouteKey,
    responses: tuple[object, ...],
) -> DiscoveryCandidate:
    address = DiscoveryAddress(
        scheme="classic_can_id",
        request_id=route.arbitration_id,
        response_id=route.arbitration_id,
        is_extended_id=False,
    )
    prefix = (
        next(
            (
                data[2:]
                for message in responses
                if (data := matching_data(message, route)) is not None
                and len(data) >= 3
                and data[1] == 0
            ),
            b"",
        )
        .split(b"-", 1)[0]
        .strip()
        .upper()
    )
    collector = ClassicIdentityCollector(
        route,
        l6_pcb_format=prefix == b"LHL6",
        allow_short_product_code=prefix == b"LHT10",
    )
    product_code: str | None = None
    model: str | None = None
    side: HandSide | None = None
    raw = _classic_response_bytes(responses, route)
    try:
        for message in responses:
            collector.offer(message)
        if not collector.product_code_complete:
            problem = DiscoveryProblem(
                "product_code_incomplete", "classic product code has missing fragments"
            )
        else:
            raw = collector.product_code_bytes
            product_code = collector.product_code()
            model = _classic_model(product_code)
            fields = product_code.split("-")
            if len(fields) > 3:
                side = {"L": HandSide.LEFT, "R": HandSide.RIGHT}.get(
                    fields[3].strip().upper()
                )
            if model is None:
                problem = DiscoveryProblem(
                    "unknown_model",
                    f"unrecognized classic product code {product_code!r}",
                )
            elif side is None:
                problem = DiscoveryProblem(
                    "malformed_identity", "classic product code has no valid hand side"
                )
            else:
                product_code = parse_factory_identity(
                    product_code,
                    expected_model=tuple(
                        prefix
                        for prefix, public in _CLASSIC_MODELS.items()
                        if public == model
                    ),
                    configured_side=side,
                ).product_code
                if route not in CLASSIC_DEFAULT_ROUTES:
                    problem = DiscoveryProblem(
                        "unsupported_address",
                        "classic control supports only the default 0x27/0x28 addresses",
                    )
                elif route != classic_route(side):
                    problem = DiscoveryProblem(
                        "identity_mismatch",
                        "product code side disagrees with the responding CAN address",
                    )
                else:
                    problem = DiscoveryProblem(
                        "identity_pending",
                        "complete classic identity needs verification",
                    )
    except ClassicIdentityConflictError as error:
        problem = DiscoveryProblem("address_collision", str(error))
    except HandError as error:
        problem = _problem_from_error(error)
    return _partial_candidate(
        channel=channel,
        wire_protocol=TransportKind.CAN,
        address=address,
        transport=_transport_spec(source, channel, TransportKind.CAN),
        model=model,
        side=side,
        product_code=product_code,
        raw_identity=raw,
        problem=problem,
    )


def _verify_classic_candidate(
    transport: object,
    candidate: DiscoveryCandidate,
    config: DiscoveryConfig,
    failures: list[DiscoveryFailure],
) -> DiscoveryCandidate:
    if candidate.problems[0].code != "identity_pending":
        return candidate
    assert candidate.model is not None and candidate.side is not None
    capabilities = get_declared_capabilities(candidate.model, side=candidate.side)
    backend = create_protocol_backend(capabilities)
    try:
        transaction = backend.identity_transaction(capabilities)
        result = _execute_identity(
            transport,
            cast(tuple[CANRouteKey, ...], backend.route_keys),
            transaction,
            config,
            discard_s=max(_CLASSIC_RESPONSE_GUARD_S, transaction.late_response_guard_s),
        )
        if result.device_info.product_code != candidate.identity.product_code:
            raise _ResponseError(
                IdentityMismatchError(
                    "product code changed during identity verification"
                )
            )
    except _NoResponse:
        problem = DiscoveryProblem(
            "identity_timeout",
            "classic product code responded but complete identity timed out",
        )
    except _ResponseError as wrapped:
        problem = _problem_from_error(wrapped.error)
    except _TransportProbeError as wrapped:
        if _fatal_link_error(wrapped, transport):
            raise
        _record_failure(
            failures,
            operation="verify_classic_identity",
            error=wrapped.error,
            channel=candidate.channel,
            wire_protocol=TransportKind.CAN,
        )
        problem = DiscoveryProblem(
            "transport_error", "transport failed during complete identity verification"
        )
    else:
        return _verified_candidate(
            candidate.channel,
            TransportKind.CAN,
            candidate.address,
            candidate.transport,
            result,
        )
    return replace(candidate, problems=(problem,))


def _probe_can_fd(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> bool:
    compatible_o20 = _channel_matches_model(channel, "O20", HandSide.LEFT)
    compatible_l30 = _channel_matches_model(channel, "L30", None)
    compatible_o30 = _channel_matches_model(channel, "O30", HandSide.LEFT)
    if not any((compatible_o20, compatible_l30, compatible_o30)):
        return False
    transport: object | None = None
    failed = False
    mode_initialization = True
    try:
        _budget.remaining(config.total_timeout_s)
        transport = source.create_transport(channel, TransportKind.CAN_FD)
        _open_transport(transport)
        mode_initialization = False
        probes = (
            (
                "o20",
                compatible_o20,
                lambda: _probe_o20(source, channel, transport, config, candidates),
            ),
            (
                "o30",
                compatible_o30,
                lambda: _probe_o30(
                    source, channel, transport, config, candidates, failures
                ),
            ),
            (
                "l30",
                compatible_l30,
                lambda: _probe_l30(
                    source, channel, transport, config, candidates, failures
                ),
            ),
        )
        for model, compatible, probe in probes:
            _budget.remaining(config.total_timeout_s)
            if not compatible:
                continue
            if not _probe_transport_open(transport):
                failed = True
                break
            try:
                probe()
            except Exception as error:  # noqa: BLE001 - per-model probe boundary
                _record_failure(
                    failures,
                    operation=f"probe_{model}_identity",
                    error=error,
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                )
                if _fatal_link_error(error, transport):
                    failed = True
                    break
            if any(
                c.channel == channel and c.status is DiscoveryStatus.VERIFIED
                for c in candidates
            ):
                break
    except Exception as error:  # noqa: BLE001 - discovery source boundary
        failed = _fatal_link_error(error, transport, initializing=mode_initialization)
        _record_failure(
            failures,
            operation="probe_can_fd_identity",
            error=error,
            channel=channel,
            wire_protocol=TransportKind.CAN_FD,
        )
    finally:
        if transport is not None:
            try:
                _close_transport(transport)
            except Exception as error:  # noqa: BLE001 - transport boundary
                failed = True
                _record_failure(
                    failures,
                    operation="close_can_fd_probe_transport",
                    error=error,
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    cleanup=partial(_close_transport, transport),
                )
    return failed


def _fatal_link_error(
    error: Exception, transport: object | None, *, initializing: bool = False
) -> bool:
    if not initializing and (transport is None or not _probe_transport_open(transport)):
        return True
    cause: BaseException | None = error
    seen: set[int] = set()
    mode_error = False
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, (TransportClosedError, TransportNotReadyError)):
            return True
        if isinstance(cause, OSError) and cause.errno in {
            errno.ENETDOWN,
            errno.ENODEV,
            errno.EBADF,
            errno.ENXIO,
            errno.EBUSY,
            errno.EACCES,
            errno.EPERM,
        }:
            return True
        if isinstance(
            cause,
            (
                can.CanInitializationError,
                can.CanInterfaceNotImplementedError,
                TransportUnsupportedError,
                TransportValidationError,
                ValidationError,
                NotImplementedError,
            ),
        ):
            mode_error = True
        cause = (
            getattr(cause, "error", None)
            or getattr(cause, "cause", None)
            or cause.__cause__
        )
    # Before open succeeds, CLOSED/FAULTED alone says nothing about the next
    # wire mode. Allow diagnosed initialization/configuration failures only;
    # unknown I/O failures remain terminal. The caller still requires cleanup.
    return initializing and not mode_error


def _probe_transport_open(transport: object) -> bool:
    """Stop a dead managed link; custom sources may omit health inspection."""

    get_health = getattr(transport, "get_health", None)
    if not callable(get_health):
        return True
    return get_health().state is TransportState.OPEN


def _probe_o20(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    transport: object,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
) -> None:
    endpoints = tuple(
        (
            side,
            device_id_for_side(side),
            o20_read_route(device_id_for_side(side), REG_DEVICE_INFO),
        )
        for side in (HandSide.LEFT, HandSide.RIGHT)
    )
    messages = _collect(
        transport,
        tuple(route for _, _, route in endpoints),
        tuple(
            build_o20_read(device_id, REG_DEVICE_INFO) for _, device_id, _ in endpoints
        ),
        config,
    )
    grouped = _group_by_route(messages)
    spec = _transport_spec(source, channel, TransportKind.CAN_FD)
    for side, device_id, route in endpoints:
        responses = grouped.get(route, ())
        if not responses:
            continue
        address = DiscoveryAddress(
            scheme="o20_device_id",
            request_id=route.arbitration_id,
            response_id=route.arbitration_id,
            is_extended_id=True,
            node_id=device_id,
        )
        if len(responses) != 1:
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="O20",
                    side=side,
                    problem=DiscoveryProblem(
                        "address_collision",
                        "multiple O20 DeviceInfo responses used the same device ID",
                    ),
                )
            )
            continue
        capabilities = get_declared_capabilities("O20", side=side)
        transaction = o20_identity_transaction(capabilities)
        try:
            progress = transaction.consume(responses[0])
            result = _require_identity_progress(progress, transaction)
        except HandError as error:
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="O20",
                    side=side,
                    problem=_problem_from_error(error),
                )
            )
        else:
            candidates.append(
                _verified_candidate(
                    channel,
                    TransportKind.CAN_FD,
                    address,
                    spec,
                    result,
                )
            )


def _probe_l30(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    transport: object,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> None:
    endpoints = tuple(
        (
            node_id,
            l30_response_route(
                parent=PARENT_CONFIG,
                subcommand=SUB_DEVICE_INFO,
                write=False,
                node_id=node_id,
            ),
        )
        for node_id in config.l30_node_ids
    )
    requests = tuple(
        build_l30_request(
            parent=PARENT_CONFIG,
            subcommand=SUB_DEVICE_INFO,
            write=False,
            node_id=node_id,
        )
        for node_id, _ in endpoints
    )
    messages = _collect(
        transport,
        tuple(route for _, route in endpoints),
        requests,
        config,
    )
    grouped = _group_by_route(messages)
    spec = _transport_spec(source, channel, TransportKind.CAN_FD)
    for node_id, route in endpoints:
        responses = grouped.get(route, ())
        if not responses:
            continue
        request = build_l30_request(
            parent=PARENT_CONFIG,
            subcommand=SUB_DEVICE_INFO,
            write=False,
            node_id=node_id,
        )
        address = DiscoveryAddress(
            scheme="l30_node_id",
            request_id=request.arbitration_id,
            response_id=route.arbitration_id,
            is_extended_id=True,
            node_id=node_id,
        )
        if len(responses) != 1:
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="L30",
                    side=HandSide.NOT_APPLICABLE,
                    problem=DiscoveryProblem(
                        "address_collision",
                        "multiple L30 DeviceInfo responses used the same node ID",
                    ),
                )
            )
            continue
        capabilities = get_declared_capabilities("L30")
        transaction = l30_identity_transaction(
            capabilities,
            node_id=node_id,
            host_id=0,
        )
        try:
            progress = transaction.consume(responses[0])
            if not progress.matched or not progress.complete:
                raise ProtocolError("L30 DeviceInfo probe response was incomplete")
            next_transaction = transaction.build_next_transaction()
            if next_transaction is None:
                raise ProtocolError("L30 identity omitted its product-code stage")
            routes = (
                route,
                l30_response_route(
                    parent=PARENT_CONFIG,
                    subcommand=SUB_PRODUCT_CODE,
                    write=False,
                    node_id=node_id,
                ),
            )
            result = _execute_identity(
                transport,
                routes,
                next_transaction,
                config,
            )
        except _NoResponse:
            problem = DiscoveryProblem(
                "identity_timeout",
                "L30 DeviceInfo responded but product code timed out",
            )
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="L30",
                    side=HandSide.NOT_APPLICABLE,
                    problem=problem,
                )
            )
        except _ResponseError as wrapped:
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="L30",
                    side=HandSide.NOT_APPLICABLE,
                    problem=_problem_from_error(wrapped.error),
                )
            )
        except _TransportProbeError as wrapped:
            if _fatal_link_error(wrapped, transport):
                raise
            _record_failure(
                failures,
                operation="verify_l30_identity",
                error=wrapped.error,
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
            )
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="L30",
                    side=HandSide.NOT_APPLICABLE,
                    problem=DiscoveryProblem(
                        "transport_error",
                        "transport failed during complete L30 identity verification",
                    ),
                )
            )
        except HandError as error:
            candidates.append(
                _partial_candidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    transport=spec,
                    model="L30",
                    side=HandSide.NOT_APPLICABLE,
                    problem=_problem_from_error(error),
                )
            )
        else:
            candidates.append(
                _verified_candidate(
                    channel,
                    TransportKind.CAN_FD,
                    address,
                    spec,
                    result,
                    reported_side=_l30_reported_side(result),
                )
            )


def _probe_o30(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    transport: object,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> None:
    # Defaults cover both sides; custom configured IDs do not encode a side.
    for request_id in config.o30_request_ids:
        _probe_o30_endpoint(
            HOPAddressing(request_id, request_id | 0x400),
            source,
            channel,
            transport,
            config,
            candidates,
            failures,
        )


def _probe_o30_endpoint(
    addressing: HOPAddressing,
    source: DiscoverySource,
    channel: DiscoveryChannel,
    transport: object,
    config: DiscoveryConfig,
    candidates: list[DiscoveryCandidate],
    failures: list[DiscoveryFailure],
) -> None:
    route = o30_response_route(addressing.response_id)
    messages = _collect(
        transport,
        (route,),
        (
            build_o30_read(
                MI_PRODUCT_INFO,
                sub_index=0,
                length=0x10,
                request_id=addressing.request_id,
            ),
        ),
        config,
    )
    matching: list[object] = []
    malformed: Exception | None = None
    for message in messages:
        try:
            frame = matching_response_frame(message, addressing.response_id)
        except HandError as error:
            malformed = error
            continue
        if (
            frame is not None
            and frame.main_index == MI_PRODUCT_INFO
            and frame.sub_index == 0
            and not frame.rts
        ):
            matching.append(message)
    if not matching and malformed is None:
        return
    spec = _transport_spec(source, channel, TransportKind.CAN_FD)
    address = DiscoveryAddress(
        scheme="o30_hop_endpoint",
        request_id=addressing.request_id,
        response_id=route.arbitration_id,
        is_extended_id=False,
    )
    if malformed is not None and not matching:
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=_problem_from_error(malformed),
            )
        )
        return
    if len(matching) != 1:
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=DiscoveryProblem(
                    "address_collision",
                    "multiple O30 product responses used the same HOP endpoint",
                ),
            )
        )
        return
    provisional = get_declared_capabilities("O30", side=HandSide.LEFT)
    transaction = o30_identity_transaction(provisional, addressing)
    try:
        progress = transaction.consume(matching[0])
        if not progress.matched or not progress.complete:
            raise ProtocolError("O30 product-model probe response was incomplete")
        next_transaction = transaction.build_next_transaction()
        if next_transaction is None:
            raise ProtocolError("O30 identity omitted required fields")
        result = _execute_identity(
            transport,
            (route,),
            next_transaction,
            config,
        )
    except _NoResponse:
        problem = DiscoveryProblem(
            "identity_timeout",
            "O30 product model responded but complete identity timed out",
        )
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=problem,
            )
        )
    except _ResponseError as wrapped:
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=_problem_from_error(wrapped.error),
            )
        )
    except _TransportProbeError as wrapped:
        if _fatal_link_error(wrapped, transport):
            raise
        _record_failure(
            failures,
            operation="verify_o30_identity",
            error=wrapped.error,
            channel=channel,
            wire_protocol=TransportKind.CAN_FD,
        )
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=DiscoveryProblem(
                    "transport_error",
                    "transport failed during complete O30 identity verification",
                ),
            )
        )
    except HandError as error:
        candidates.append(
            _partial_candidate(
                channel=channel,
                wire_protocol=TransportKind.CAN_FD,
                address=address,
                transport=spec,
                model="O30",
                side=None,
                problem=_problem_from_error(error),
            )
        )
    else:
        reported_side = result.device_info.reported_side
        if reported_side is not None:
            candidates.append(
                _verified_candidate(
                    channel,
                    TransportKind.CAN_FD,
                    address,
                    spec,
                    result,
                    reported_side=reported_side,
                )
            )
        else:
            candidates.append(
                DiscoveryCandidate(
                    channel=channel,
                    wire_protocol=TransportKind.CAN_FD,
                    address=address,
                    status=DiscoveryStatus.PARTIAL,
                    identity=_identity_from_result(result, side=None),
                    transport=spec,
                    problems=(
                        DiscoveryProblem(
                            "missing_side",
                            "O30 HOP identity does not provide a verified hand side",
                        ),
                    ),
                )
            )


def _collect(
    transport: object,
    route_keys: tuple[CANRouteKey, ...],
    requests: tuple[object, ...],
    config: DiscoveryConfig,
    *,
    discard_s: float = _CLASSIC_RESPONSE_GUARD_S,
) -> tuple[object, ...]:
    subscribe = getattr(transport, "subscribe_routes", None)
    if not callable(subscribe):
        raise DiscoveryError("probe transport lacks subscribe_routes()")
    subscription = subscribe(
        route_keys,
        queue_capacity=config.route_queue_capacity,
    )
    messages: list[object] = []
    try:
        _discard_responses(subscription, discard_s)
        overflows = subscription.get_health().overflows
        results = _send_requests(transport, requests, config)
        claims = _claim_results(route_keys, requests, results)
        deadline = time.monotonic() + _budget.remaining(config.response_timeout_s)
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining == 0:
                break
            try:
                routed = subscription.receive(timeout_s=remaining)
            except RxTimeout:
                break
            if _response_after_claim(
                routed.route_key,
                routed.message,
                routed.received_at_s,
                claims,
                received_order=getattr(routed, "received_order", None),
            ):
                messages.append(routed.message)
        if subscription.get_health().overflows != overflows:
            raise DiscoveryError("discovery receive queue overflowed")
    finally:
        subscription.close()
    return tuple(messages)


def _execute_identity(
    transport: object,
    route_keys: tuple[CANRouteKey, ...],
    transaction: ProtocolTransaction,
    config: DiscoveryConfig,
    *,
    discard_s: float = _CLASSIC_RESPONSE_GUARD_S,
) -> IdentityResult:
    subscribe = getattr(transport, "subscribe_routes", None)
    if not callable(subscribe):
        raise _TransportProbeError(
            DiscoveryError("probe transport lacks subscribe_routes()")
        )
    try:
        subscription = subscribe(
            route_keys,
            queue_capacity=config.route_queue_capacity,
        )
    except Exception as error:
        raise _TransportProbeError(error) from error
    matched_any = False
    current: ProtocolTransaction | None = transaction
    result: IdentityResult | None = None
    try:
        try:
            _discard_responses(subscription, discard_s)
        except Exception as error:
            raise _TransportProbeError(error) from error
        while current is not None:
            overflows = subscription.get_health().overflows
            try:
                results = _send_requests(
                    transport,
                    current.request_messages,
                    config,
                    minimum_interval_s=current.request_interval_s,
                )
                claims = _claim_results(route_keys, current.request_messages, results)
            except Exception as error:
                raise _TransportProbeError(error) from error
            deadline = time.monotonic() + _budget.remaining(config.response_timeout_s)
            complete = False
            # Classic identities have no transaction token. Check the whole
            # response window, including contradictory trailing duplicates.
            while not complete or discard_s:
                if subscription.get_health().overflows != overflows:
                    raise _TransportProbeError(
                        DiscoveryError("identity receive queue overflowed")
                    )
                remaining = max(0.0, deadline - time.monotonic())
                if remaining == 0:
                    if complete:
                        break
                    raise _NoResponse(matched=matched_any)
                try:
                    routed = subscription.receive(timeout_s=remaining)
                except RxTimeout as error:
                    if complete:
                        break
                    raise _NoResponse(matched=matched_any) from error
                if not _response_after_claim(
                    routed.route_key,
                    routed.message,
                    routed.received_at_s,
                    claims,
                    received_order=getattr(routed, "received_order", None),
                ):
                    continue
                try:
                    progress = current.consume(routed.message)
                except Exception as error:
                    raise _ResponseError(error) from error
                if not progress.matched:
                    continue
                matched_any = True
                if complete and not progress.complete:
                    raise _ResponseError(
                        ProtocolError("trailing response invalidated complete identity")
                    )
                complete = progress.complete
                if isinstance(progress.result, IdentityResult):
                    result = progress.result
            try:
                current = current.build_next_transaction()
            except Exception as error:
                raise _ResponseError(error) from error
    finally:
        subscription.close()
    if result is None:
        raise _ResponseError(ProtocolError("identity transaction returned no result"))
    return result


def _discard_responses(
    subscription: RouteSubscription[object, CANRouteKey], duration_s: float
) -> None:
    """Drain one bounded late-response window before starting a fresh attempt."""

    deadline = time.monotonic() + _budget.remaining(duration_s)
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            subscription.receive(timeout_s=remaining)
        except RxTimeout:
            break


def _send_requests(
    transport: object,
    requests: tuple[object, ...],
    config: DiscoveryConfig,
    *,
    minimum_interval_s: float = 0.0,
) -> tuple[SendResult, ...]:
    send = getattr(transport, "send", None)
    if not callable(send):
        raise DiscoveryError("probe transport lacks send()")
    results: list[SendResult] = []
    for index, request in enumerate(requests):
        _budget.remaining(config.send_timeout_s)
        receipt = send(request)
        result = receipt.result(timeout=_budget.remaining(config.send_timeout_s))
        if result.outcome is not SendOutcome.DISPATCHED:
            raise DiscoveryError(
                f"probe send completed as {result.outcome.value}",
                operation="send_identity_probe",
            )
        results.append(result)
        interval = max(config.inter_request_delay_s, minimum_interval_s)
        if index + 1 < len(requests) and interval:
            time.sleep(_budget.remaining(interval))
    return tuple(results)


def _claim_results(
    route_keys: tuple[CANRouteKey, ...],
    requests: tuple[object, ...],
    results: tuple[SendResult, ...],
) -> dict[tuple[CANRouteKey, int | None], SendResult]:
    """Associate each sent request with its exact response route.

    A transaction may subscribe to more routes than its current stage uses
    (for example L30 identity keeps both DeviceInfo and product-code routes).
    Derive the stage route from the request and retain the individual claim
    boundary so a later request cannot authorize an earlier response.
    """

    if len(requests) != len(results):
        raise DiscoveryError("identity request and receipt counts differ")
    claims: dict[tuple[CANRouteKey, int | None], SendResult] = {}
    for request, result in zip(requests, results, strict=True):
        if result.claimed_at_s is None:
            raise DiscoveryError("dispatched identity probe has no claim timestamp")
        route = _response_route_for_request(request, route_keys)
        discriminator = (
            request.data[0]
            if isinstance(request, can.Message) and request.data
            else None
        )
        claims.setdefault((route, discriminator), result)
    return claims


def _response_route_for_request(
    request: object, route_keys: tuple[CANRouteKey, ...]
) -> CANRouteKey:
    """Find the subscribed route corresponding to one protocol request."""

    candidates: list[CANRouteKey] = []
    if isinstance(request, can.Message):
        candidates.append(CANRouteKey(request.arbitration_id, request.is_extended_id))
    elif isinstance(request, CANFDMessage):
        if request.is_extended_id:
            # O20 responses use the same extended ID. L30 responses swap the
            # source/destination fields, so add that route as a second form.
            candidates.append(CANRouteKey(request.arbitration_id, True))
            try:
                frame = parse_l30_can_id(request.arbitration_id)
            except (HandError, ValueError):
                pass
            else:
                candidates.append(
                    l30_response_route(
                        parent=frame.parent,
                        subcommand=frame.subcommand,
                        write=frame.access == 1,
                        node_id=frame.destination_id,
                        host_id=frame.source_id,
                    )
                )
        else:
            # O30 uses standard request IDs and response_id=request_id|0x400.
            candidates.append(CANRouteKey(request.arbitration_id | 0x400, False))
    for candidate in candidates:
        if candidate in route_keys:
            return candidate
    raise DiscoveryError("identity request has no subscribed response route")


def _response_after_claim(
    route: CANRouteKey,
    message: object,
    received_at_s: float,
    claims: dict[tuple[CANRouteKey, int | None], SendResult],
    *,
    received_order: int | None = None,
) -> bool:
    discriminator = (
        message.data[0] if isinstance(message, can.Message) and message.data else None
    )
    claim = claims.get((route, discriminator))
    # The broadcast C0 request is sent to 0xFF while a transport routes a
    # device's reply on its real 0x27/0x28 address. Permit that explicit
    # discovery claim for either default reply route only.
    if claim is None and route in CLASSIC_DEFAULT_ROUTES:
        claim = claims.get((CLASSIC_BROADCAST_ROUTE, discriminator))
    if claim is None:
        return False
    claimed_order = getattr(claim, "claimed_order", None)
    if received_order is not None and claimed_order is not None:
        return received_order > claimed_order
    return claim.claimed_at_s is not None and received_at_s >= claim.claimed_at_s


def _open_transport(transport: object) -> None:
    open_transport = getattr(transport, "open", None)
    if not callable(open_transport):
        raise DiscoveryError("probe transport lacks open()")
    open_transport()


def _close_transport(transport: object) -> None:
    close_transport = getattr(transport, "close", None)
    if not callable(close_transport):
        raise DiscoveryError("probe transport lacks close()")
    close_transport()


def _group_by_route(
    messages: tuple[object, ...],
) -> dict[CANRouteKey, tuple[object, ...]]:
    grouped: defaultdict[CANRouteKey, list[object]] = defaultdict(list)
    for message in messages:
        if not isinstance(message, (can.Message, CANFDMessage)):
            continue
        grouped[CANRouteKey(message.arbitration_id, message.is_extended_id)].append(
            message
        )
    return {key: tuple(value) for key, value in grouped.items()}


def _classic_response_bytes(
    responses: tuple[object, ...],
    route: CANRouteKey,
) -> bytes | None:
    fragments: list[bytes] = []
    for response in responses:
        data = matching_data(response, route)
        if data is not None and len(data) >= 3:
            fragments.append(data[2:])
    raw = b"".join(fragments).rstrip(b"\x00")
    return raw or None


def _classic_model(product_code: str) -> str | None:
    prefix = product_code.split("-", 1)[0].strip().upper()
    return _CLASSIC_MODELS.get(prefix)


def _channel_matches_model(
    channel: DiscoveryChannel,
    model: str,
    side: HandSide | None,
) -> bool:
    capabilities = get_declared_capabilities(model, side=side)
    required = capabilities.transport
    return (
        channel.nominal_bitrate is None
        or channel.nominal_bitrate == required.nominal_bitrate
    ) and (
        # A profile with no declared data bitrate (e.g. O30, CAN FD with BRS
        # disabled) accepts any interface data bitrate.
        required.data_bitrate is None
        or channel.data_bitrate is None
        or channel.data_bitrate == required.data_bitrate
    )


def _channel_matches_classic(channel: DiscoveryChannel) -> bool:
    required = get_declared_capabilities("L6", side=HandSide.LEFT).transport
    return (
        channel.nominal_bitrate is None
        or channel.nominal_bitrate == required.nominal_bitrate
    )


def _transport_spec(
    source: DiscoverySource,
    channel: DiscoveryChannel,
    wire_protocol: TransportKind,
) -> TransportSpec:
    if channel.transport_specs:
        for spec in channel.transport_specs:
            if spec.kind is wire_protocol:
                return spec
        raise DiscoveryError("channel has no matching transport recipe")
    return TransportSpec(
        provider=channel.provider,
        channel=channel.name,
        kind=wire_protocol,
        nominal_bitrate=channel.nominal_bitrate,
        data_bitrate=(
            channel.data_bitrate if wire_protocol is TransportKind.CAN_FD else None
        ),
        factory=lambda: source.create_transport(channel, wire_protocol),
    )


def _verified_candidate(
    channel: DiscoveryChannel,
    wire_protocol: TransportKind,
    address: DiscoveryAddress,
    transport: TransportSpec,
    result: IdentityResult,
    *,
    reported_side: HandSide | None = None,
) -> DiscoveryCandidate:
    return DiscoveryCandidate(
        channel=channel,
        wire_protocol=wire_protocol,
        address=address,
        status=DiscoveryStatus.VERIFIED,
        identity=_identity_from_result(result, side=result.device_info.side),
        transport=transport,
        reported_side=reported_side,
        capabilities=result.capabilities,
        device_info=result.device_info,
    )


def _identity_from_result(
    result: IdentityResult,
    *,
    side: HandSide | None,
) -> DiscoveryIdentity:
    info = result.device_info
    return DiscoveryIdentity(
        model=info.model,
        side=side,
        serial_number=info.serial_number,
        pcb_version=info.pcb_version,
        firmware_version=info.firmware_version,
        mechanical_version=info.mechanical_version,
        product_code=info.product_code,
        sensor_variant=info.sensor_variant,
        raw_identity=info.raw_identity,
        fingerprint=result.fingerprint,
    )


def _partial_candidate(
    *,
    channel: DiscoveryChannel,
    wire_protocol: TransportKind,
    address: DiscoveryAddress,
    transport: TransportSpec,
    model: str | None,
    side: HandSide | None,
    problem: DiscoveryProblem,
    product_code: str | None = None,
    raw_identity: bytes | tuple[int, ...] | None = None,
) -> DiscoveryCandidate:
    return DiscoveryCandidate(
        channel=channel,
        wire_protocol=wire_protocol,
        address=address,
        status=DiscoveryStatus.PARTIAL,
        identity=DiscoveryIdentity(
            model=model,
            side=side,
            product_code=product_code,
            raw_identity=raw_identity,
        ),
        transport=transport,
        problems=(problem,),
    )


def _problem_from_error(error: Exception) -> DiscoveryProblem:
    if isinstance(error, IdentityMismatchError):
        code = "identity_mismatch"
    elif isinstance(error, ProtocolError):
        code = "malformed_identity"
    elif isinstance(error, HandError):
        code = error.code.value
    else:
        code = "malformed_identity"
    message = str(error) or type(error).__name__
    return DiscoveryProblem(code, message)


def _record_failure(
    failures: list[DiscoveryFailure],
    *,
    operation: str,
    error: Exception,
    channel: DiscoveryChannel,
    wire_protocol: TransportKind,
    cleanup: Callable[[], None] | None = None,
) -> None:
    cause = error.error if isinstance(error, _TransportProbeError) else error
    code_value = getattr(getattr(cause, "code", None), "value", None)
    failures.append(
        DiscoveryFailure(
            operation=operation,
            code=code_value
            or (
                "scan_timeout"
                if isinstance(cause, TimeoutError) and _budget.expired()
                else "discovery_failure"
            ),
            message=str(cause) or type(cause).__name__,
            error_type=type(cause).__name__,
            channel=channel,
            wire_protocol=wire_protocol,
            cleanup=cleanup,
        )
    )


def _require_identity_progress(
    progress: object,
    transaction: ProtocolTransaction,
) -> IdentityResult:
    if not getattr(progress, "matched", False) or not getattr(
        progress, "complete", False
    ):
        raise ProtocolError("identity response was incomplete")
    result = getattr(progress, "result", None)
    if (
        transaction.next_transaction is not None
        or transaction.next_transaction_factory is not None
    ):
        raise ProtocolError("identity response unexpectedly requires another stage")
    if not isinstance(result, IdentityResult):
        raise ProtocolError("identity response returned no verified result")
    return result


def _l30_reported_side(result: IdentityResult) -> HandSide:
    product_code = result.device_info.product_code
    if product_code is None:
        raise ProtocolError("L30 verified identity has no product code")
    parts = product_code.split("-")
    if len(parts) < 4:
        raise ProtocolError("L30 product code does not contain a side")
    if parts[3] == "L":
        return HandSide.LEFT
    if parts[3] == "R":
        return HandSide.RIGHT
    raise ProtocolError("L30 product code contains an unknown side")


__all__ = ["discover_hands"]
