import json
import subprocess
import sys
from dataclasses import fields, replace
from pathlib import Path

import pytest

from medre.config.loader import parse_runtime_config
from medre.config.paths import resolve
from medre.core.rendering.renderer import RenderingContext
from medre.adapters.chatmail.codec import ChatmailCodec
from medre.adapters.chatmail.config import ChatmailConfig, chat_id, prepare_config
from medre.adapters.chatmail.fake import FakeChatmailAdapter
from medre.adapters.chatmail.metadata import native_namespace, versioned
from medre.adapters.chatmail.renderer import ChatmailRenderer

from conftest import NOW, snapshot, rendered


@pytest.mark.parametrize(
    "channel", [None, "", "01", "9", "10.0", " 10", "+10", "1" * 100, 10, True]
)
def test_ambiguous_channels_are_rejected(channel):
    with pytest.raises(ValueError):
        chat_id(channel)


@pytest.mark.parametrize(
    "updates",
    [
        {"account_id": True},
        {"account_id": 0},
        {"chats": [10]},
        {"chats": (True,)},
        {"chats": (10, 10)},
        {"max_pending": 0},
        {"max_text_bytes": True},
        {"rpc_timeout_seconds": float("nan")},
        {"poll_interval_seconds": 0},
        {"reconnect_delay_seconds": float("inf")},
        {"relay_prefix": "\x00"},
        {"accounts_dir": None},
        {"origin_label": "\ud800"},
        {"adapter_id": "../mail"},
    ],
)
def test_configuration_refuses_invalid_values(config, updates):
    with pytest.raises(ValueError):
        replace(config, **updates).validate()


def test_generic_loader_and_preflight_are_pure(tmp_path, monkeypatch):
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "home"))
    loaded = parse_runtime_config(
        {
            "adapters": {"chatmail": {"mail": {"chats": [10], "adapter_kind": "fake"}}},
            "routes": {},
        },
        resolve(),
    )
    config = loaded.adapters.for_transport("chatmail")["mail"].config
    assert isinstance(config, ChatmailConfig)
    assert config.chats == (10,)
    prepared = prepare_config(config, adapter_id="mail", paths=resolve(), routes=[])
    assert prepared.accounts_dir.endswith("/adapters/mail/chatmail/accounts")
    assert not (tmp_path / "home").exists()


def test_registration_and_fake_do_not_import_sdk():
    script = """
import importlib.abc, sys
class RefuseSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('deltachat_rpc_client', 'deltachat_rpc_server')):
            raise AssertionError('SDK import: ' + fullname)
sys.meta_path.insert(0, RefuseSDK())
from medre.adapters.chatmail.plugin import adapter_spec
spec=adapter_spec()
spec.config.load()
spec.fake_adapter.load()
spec.renderer_factory.load()
spec.sample_config.load()()
print('sdk-free')
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "sdk-free"


@pytest.mark.parametrize(
    "updates",
    [
        {"state": 20},
        {"fromId": 1},
        {"fromId": 2},
        {"isInfo": True},
        {"isEdited": True},
        {"chatId": 11},
        {"viewType": "File"},
        {"downloadState": "Available"},
        {"text": "\x00"},
        {"text": " "},
        {"sender": {"isBlocked": True}},
        {"id": True},
    ],
)
def test_controls_echoes_media_and_unconfigured_chats_are_excluded(config, updates):
    assert (
        ChatmailCodec(config, "profile").decode(snapshot(**updates), timestamp=NOW)
        is None
    )


def test_identity_scope_quotes_and_encryption_evidence(config):
    codec = ChatmailCodec(config, "profile")
    event = codec.decode(
        snapshot(quote={"kind": "WithMessage", "messageId": 19, "chatId": 10}),
        timestamp=NOW,
    )
    assert event.source_transport_id == "profile:contact:12"
    assert event.source_native_ref.native_message_id == "profile:20"
    assert event.relations[0].target_native_ref.native_message_id == "profile:19"
    assert native_namespace(event.metadata.native.data)["encrypted_and_signed"] is True
    assert codec.decode(snapshot(), timestamp=NOW).event_id == event.event_id
    assert (
        ChatmailCodec(config, "other").decode(snapshot(), timestamp=NOW).event_id
        != event.event_id
    )
    assert not codec.decode(
        snapshot(quote={"kind": "WithMessage", "messageId": 19, "chatId": 11}),
        timestamp=NOW,
    ).relations
    assert not native_namespace({"chatmail": {"schema_version": 2}})
    assert versioned({"chatmail": {"schema_version": 2}})


def test_native_timestamp_is_preserved(config):
    event = ChatmailCodec(config, "profile").decode(
        snapshot(timestamp=1000), timestamp=NOW
    )
    assert event.timestamp.timestamp() == 1000


async def test_renderer_preserves_text_and_refuses_unknown_kinds_and_budgets(config):
    event = ChatmailCodec(config, "profile").decode(snapshot(), timestamp=NOW)
    renderer = ChatmailRenderer({"mail": config})
    ctx = RenderingContext(
        target_adapter="mail",
        target_channel="10",
        target_platform="chatmail",
        delivery_strategy="direct",
    )
    result = await renderer.render(event, ctx)
    assert result.payload == {"text": event.payload["body"], "chat_id": 10}
    with pytest.raises(ValueError, match="budget"):
        await renderer.render(event, replace(ctx, max_text_bytes=1))
    with pytest.raises(ValueError, match="configured"):
        await renderer.render(event, replace(ctx, target_channel="11"))
    import msgspec

    assert not renderer.can_render(
        msgspec.structs.replace(event, event_kind="future.notification"), ctx
    )


async def test_fake_uses_rendered_validation_and_bounded_history(config, context):
    adapter = FakeChatmailAdapter("mail", config)
    from medre.core.contracts.adapter import AdapterSendError, AdapterPermanentError

    with pytest.raises(AdapterSendError):
        await adapter.deliver(rendered())
    await adapter.start(context[0])
    for _ in range(1002):
        await adapter.deliver(rendered())
    assert len(adapter.delivered_payloads) == 1000
    assert adapter.make_event(text="hello").payload["body"] == "hello"
    with pytest.raises(AdapterPermanentError):
        await adapter.deliver(rendered(payload={"text": "bad", "chat_id": 11}))
    assert (
        await adapter.health_check()
    ).capabilities.max_text_bytes == config.max_text_bytes
    await adapter.stop()
    assert (await adapter.health_check()).health == "unknown"


def test_schemas_follow_configuration_and_native_shapes(config):
    from jsonschema import Draft202012Validator
    import msgspec

    root = Path(__file__).parents[1] / "docs/schemas"
    schema = json.loads((root / "config.schema.json").read_text())
    assert set(schema["properties"]) - {"enabled", "adapter_kind"} == {
        field.name for field in fields(ChatmailConfig)
    }
    Draft202012Validator.check_schema(schema)
    native = json.loads((root / "native.schema.json").read_text())
    event = ChatmailCodec(config, "profile").decode(snapshot(), timestamp=NOW)
    Draft202012Validator(native).validate(
        msgspec.to_builtins(event.metadata.native.data["chatmail"])
    )


def test_editor_schema_accepts_actual_sample_adapter_block():
    import json
    from pathlib import Path

    from jsonschema import Draft202012Validator
    from medre.adapters.chatmail.sample import sample_config

    schema = json.loads(
        (
            Path(__file__).resolve().parents[1] / "docs/schemas/config.schema.json"
        ).read_text()
    )
    sample = sample_config()
    # Adapter instance IDs belong to the enclosing MEDRE transport map.
    Draft202012Validator(schema).validate(sample.config)
