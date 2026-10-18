"""Pure configuration and preflight for explicitly selected local profiles."""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from medre.config.identifiers import adapter_id_problem
from medre.config.paths import MedrePaths


def chat_id(value: str | None) -> int:
    """Require canonical decimal chat IDs; special chats are never routable."""
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value):
        raise ValueError("Chatmail channels must be decimal chat IDs")
    number = int(value)
    if not 10 <= number <= 4_294_967_295:
        raise ValueError("Chatmail channels must identify ordinary chats")
    return number


def plain_text(value: object) -> bool:
    """Reject malformed Unicode and control bytes without rewriting content."""
    if not isinstance(value, str) or any(
        ord(char) < 32 and char not in "\n\r\t" for char in value
    ):
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


@dataclass(frozen=True)
class ChatmailConfig:
    adapter_id: str
    accounts_dir: str = ""
    account_id: int = 1
    chats: tuple[int, ...] = ()
    state_dir: str = ""
    rpc_server_path: str = ""
    rpc_timeout_seconds: float = 30.0
    poll_interval_seconds: float = 5.0
    reconnect_delay_seconds: float = 5.0
    max_text_bytes: int = 16384
    max_pending: int = 100
    origin_label: str = ""
    relay_prefix: str = "[{origin_label}] {sender}: "

    def validate(self) -> ChatmailConfig:
        problem = adapter_id_problem(self.adapter_id)
        if problem:
            raise ValueError(problem)
        if (
            type(self.account_id) is not int
            or not 1 <= self.account_id <= 4_294_967_295
        ):
            raise ValueError("account_id must be a positive account ID")
        if not isinstance(self.chats, tuple) or any(
            type(i) is not int for i in self.chats
        ):
            raise ValueError("chats must be a tuple of integer chat IDs")
        for item in self.chats:
            chat_id(str(item))
        if len(set(self.chats)) != len(self.chats):
            raise ValueError("chats must not contain repeated IDs")
        for name in (
            "accounts_dir",
            "state_dir",
            "rpc_server_path",
            "origin_label",
            "relay_prefix",
        ):
            if not plain_text(getattr(self, name)):
                raise ValueError(f"{name} must be valid text")
        for name in (
            "rpc_timeout_seconds",
            "poll_interval_seconds",
            "reconnect_delay_seconds",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive finite number")
        for name in ("max_text_bytes", "max_pending"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        return self

    def destination(self, channel: str | None) -> int:
        number = chat_id(channel)
        if number not in self.chats:
            raise ValueError("Destination is not in the configured Chatmail chats")
        return number


def prepare_config(
    config: ChatmailConfig, *, adapter_id: str, paths: MedrePaths, routes: Iterable[Any]
) -> ChatmailConfig:
    """Resolve private paths and validate routes without opening a profile."""
    root = paths.adapter_transport_state_dir(adapter_id, "chatmail")
    config = replace(
        config,
        accounts_dir=(
            str(paths.expand_placeholder(config.accounts_dir))
            if config.accounts_dir
            else str(root / "accounts")
        ),
        state_dir=(
            str(paths.expand_placeholder(config.state_dir))
            if config.state_dir
            else str(root)
        ),
    ).validate()
    for route in routes:
        if not route.enabled:
            continue
        if route.source.adapter == adapter_id and route.source.channel is not None:
            config.destination(route.source.channel)
        for target in route.targets:
            if target.adapter == adapter_id:
                if (
                    target.destination is not None
                    and target.destination.kind != "channel"
                ):
                    raise ValueError(
                        "Chatmail destinations use configured chat-ID channels"
                    )
                config.destination(target.channel)
    return config
