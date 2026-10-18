"""Provider-owned native snapshots and deterministic runtime boundaries."""

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.core.contracts.adapter import AdapterContext
from medre.core.events.delivery import DeliveryAttemptProvenance
from medre.core.rendering.renderer import RenderingResult
from medre.adapters.chatmail.config import ChatmailConfig

NOW = datetime(2026, 10, 18, tzinfo=timezone.utc)


@pytest.fixture
def config(tmp_path):
    root = tmp_path / "accounts"
    root.mkdir()
    return ChatmailConfig(
        "mail",
        accounts_dir=str(root),
        state_dir=str(tmp_path / "state"),
        chats=(10,),
        relay_prefix="",
    )


def snapshot(message_id=20, **updates):
    return {
        "id": message_id,
        "chatId": 10,
        "fromId": 12,
        "state": 10,
        "downloadState": "Done",
        "viewType": "Text",
        "isInfo": False,
        "isEdited": False,
        "text": "Hello <Chatmail> & 世界",
        "showPadlock": True,
        "sender": {"isBlocked": False, "displayName": "Alice"},
        **updates,
    }


def attempt(number=1):
    return DeliveryAttemptProvenance(
        event_id="event",
        delivery_plan_id="plan",
        target_adapter="mail",
        target_channel="10",
        outbox_id="outbox",
        attempt_number=number,
        source="live",
    )


def rendered(provenance=None, **updates):
    return RenderingResult(
        **{
            "event_id": "event",
            "target_adapter": "mail",
            "target_channel": "10",
            "payload": {"text": "Rendered message", "chat_id": 10},
            "attempt_provenance": provenance,
            **updates,
        }
    )


@pytest.fixture
def context():
    checkpoints = {}

    async def commit(stream, cursor, metadata):
        checkpoints[stream] = SimpleNamespace(cursor=cursor, metadata_json=metadata)

    ctx = AdapterContext(
        "mail",
        AsyncMock(),
        logging.getLogger("chatmail-test"),
        lambda: NOW,
        asyncio.Event(),
        admit_inbound=AsyncMock(),
        load_checkpoint=AsyncMock(side_effect=lambda key: checkpoints.get(key)),
        commit_checkpoint=AsyncMock(side_effect=commit),
        report_delivery_feedback=AsyncMock(),
    )
    return ctx, checkpoints


async def wait_until(predicate):
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    handle = None

    def check():
        nonlocal handle
        if ready.done():
            return
        if predicate():
            ready.set_result(None)
        else:
            handle = loop.call_later(0.001, check)

    check()
    try:
        await asyncio.wait_for(ready, 3)
    finally:
        if handle is not None:
            handle.cancel()


class NativeSession:
    """Native database survives session replacement; no SDK or network involved."""

    def __init__(self, config, messages=None):
        self.config = config
        self.messages = messages if messages is not None else {}
        self.wake = asyncio.Event()
        self.started = False
        self.sent = []
        self.calls = []
        self.connectivity = 4000

    async def start(self):
        self.started = True

    async def stop(self, timeout=5):
        self.started = False

    def alive(self):
        return self.started

    async def call(self, name, *args):
        self.calls.append((name, args))
        if name in {"start_io", "stop_io_for_all_accounts"}:
            return None
        if name == "get_all_account_ids":
            return [1]
        if name == "is_configured":
            return True
        if name == "get_blob_dir":
            return "/native/profile-uuid/dc.db-blobs"
        if name == "get_basic_chat_info":
            return {
                "isContactRequest": False,
                "isDeviceChat": False,
                "isSelfTalk": False,
                "chatType": "Group",
            }
        if name == "get_chat_contacts":
            return [1, 12, 13]
        if name == "get_message_ids":
            return [
                key
                for key, value in self.messages.items()
                if value["chatId"] == args[1]
            ]
        if name == "get_message":
            return self.messages[args[1]]
        if name == "get_connectivity":
            return self.connectivity
        if name == "send_msg":
            self.sent.append(args)
            key = max([9, *self.messages]) + 1
            self.messages[key] = snapshot(key, fromId=1, state=20, text=args[2]["text"])
            return key
        raise AssertionError(name)
