"""SDK-free fake using the provider's codec and rendered-payload checks."""

from datetime import datetime, timezone
from uuid import uuid4

from medre.core.contracts.adapter import (
    AdapterContext,
    AdapterHandoffResult,
    AdapterInfo,
    AdapterSendError,
)
from medre.core.events.canonical import CanonicalEvent
from medre.core.rendering.renderer import RenderingResult

from .adapter import ChatmailAdapter, validate_payload
from .config import ChatmailConfig, chat_id


class FakeChatmailAdapter(ChatmailAdapter):
    def __init__(self, adapter_id: str, config: ChatmailConfig | None = None) -> None:
        super().__init__(config or ChatmailConfig(adapter_id, chats=(10,)))
        self.delivered_payloads: list[RenderingResult] = []
        self._started = False

    async def start(self, ctx: AdapterContext) -> None:
        self.ctx = ctx
        self._mark_started(ctx)
        self._started = True

    async def stop(self, timeout: float = 5.0) -> None:
        self._started = False

    async def deliver(self, result: RenderingResult) -> AdapterHandoffResult:
        from dataclasses import replace

        config = self.config
        channel = chat_id(result.target_channel)
        if channel not in config.chats:
            config = replace(config, chats=(*config.chats, channel))
        validate_payload(result, config)
        if not self._started:
            raise AdapterSendError("Chatmail fake is not started")
        self.delivered_payloads.append(result)
        del self.delivered_payloads[:-1000]
        return AdapterHandoffResult(
            native_message_id=f"fake-chatmail:{uuid4().hex}",
            native_channel_id=result.target_channel,
            note="SDK-free fake; no native delivery",
        )

    async def health_check(self) -> AdapterInfo:
        return AdapterInfo(
            self.adapter_id,
            self.platform,
            self.role,
            "0.1.0",
            self._capabilities,
            health="healthy" if self._started else "unknown",
        )

    def diagnostics(self) -> dict[str, object]:
        return {
            "fake": True,
            "started": self._started,
            "delivered": len(self.delivered_payloads),
        }

    def make_event(
        self,
        *,
        text: str,
        event_kind: str = "message.created",
        channel: str | None = None,
    ) -> CanonicalEvent:
        from dataclasses import replace
        import msgspec
        from .codec import ChatmailCodec

        if event_kind not in {"message.created", "message.text"}:
            raise ValueError("Chatmail fake source supports text messages")
        conversation = chat_id(channel or str(self.config.chats[0]))
        config = replace(
            self.config, chats=tuple(set(self.config.chats) | {conversation})
        )
        event = ChatmailCodec(config, "fake-profile").decode(
            {
                "id": 10 + uuid4().int % 4_000_000_000,
                "chatId": conversation,
                "fromId": 10,
                "text": text,
                "state": 10,
                "downloadState": "Done",
                "viewType": "Text",
                "isInfo": False,
                "isEdited": False,
                "sender": {"isBlocked": False, "displayName": "Alice"},
            },
            timestamp=self.ctx.clock() if self.ctx else datetime.now(timezone.utc),
        )
        if event is None:
            raise ValueError("Invalid fake Chatmail message")
        return msgspec.structs.replace(event, event_kind=event_kind)
