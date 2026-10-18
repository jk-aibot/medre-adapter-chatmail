"""Plain-text compatibility rendering using MEDRE attribution and fallbacks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from medre.adapters.metadata import project_source_fields
from medre.core.events.canonical import CanonicalEvent
from medre.core.rendering.attribution import (
    build_relay_attribution,
    format_relay_prefix,
)
from medre.core.rendering.relations import degrade_relations_inline
from medre.core.rendering.renderer import RenderingContext, RenderingResult

from .config import ChatmailConfig, plain_text


class ChatmailRenderer:
    name = "chatmail"

    def __init__(
        self,
        configs: Mapping[str, ChatmailConfig],
        source_attribution: Mapping[str, Any] | None = None,
    ) -> None:
        self.configs = dict(configs)
        self.source_attribution = dict(source_attribution or {})

    def can_render(self, event: CanonicalEvent, ctx: RenderingContext) -> bool:
        return (
            ctx.target_platform == "chatmail"
            and ctx.target_adapter in self.configs
            and ctx.delivery_strategy != "skip"
            and (
                event.event_kind in {"message.created", "message.text"}
                or (ctx.delivery_strategy == "fallback_text" and bool(event.relations))
            )
        )

    async def render(
        self, event: CanonicalEvent, ctx: RenderingContext
    ) -> RenderingResult:
        if not self.can_render(event, ctx):
            raise ValueError("Unsupported Chatmail rendering target or strategy")
        config = self.configs[ctx.target_adapter]
        if (
            ctx.target_destination is not None
            and ctx.target_destination.kind != "channel"
        ):
            raise ValueError("Chatmail rendering requires a configured chat-ID channel")
        destination = config.destination(ctx.target_channel)
        text = event.payload.get("body", event.payload.get("text", ""))
        if not isinstance(text, str):
            raise ValueError("Chatmail message body must be text")
        fallback = ctx.delivery_strategy == "fallback_text"
        if fallback:
            text = degrade_relations_inline(event, text)
        if not text.strip():
            raise ValueError("Chatmail message body must not be empty")
        attr_config = self.source_attribution.get(event.source_adapter)
        label = ctx.source_origin_label
        if label is None and attr_config is not None:
            label = attr_config.origin_label
        native = dict(event.metadata.native.data) if event.metadata.native else {}
        projected = project_source_fields(
            native,
            source_adapter=event.source_adapter,
            source_transport_id=event.source_transport_id,
            platform_hint=getattr(attr_config, "platform", None),
        )
        attr = build_relay_attribution(
            event, source_origin_label=label, projected_fields=projected
        )
        prefix = format_relay_prefix(config.relay_prefix, attr)
        if prefix.formatting_error:
            raise ValueError("Invalid Chatmail relay prefix template")
        text = prefix.rendered_prefix + text
        if not text.strip() or not plain_text(text):
            raise ValueError("Chatmail message requires non-empty text")
        # Fail rather than silently cut an identity prefix or message content.
        limits = [config.max_text_bytes]
        if ctx.max_text_bytes is not None:
            limits.append(ctx.max_text_bytes)
        if len(text.encode("utf-8")) > min(limits) or (
            ctx.max_text_chars is not None and len(text) > ctx.max_text_chars
        ):
            raise ValueError("Rendered Chatmail text exceeds the destination budget")
        return RenderingResult(
            event_id=event.event_id,
            target_adapter=ctx.target_adapter,
            target_channel=ctx.target_channel,
            payload={"text": text, "chat_id": destination},
            metadata={"chatmail": {"format": "plain", "attribution": "relay"}},
            fallback_applied="strategy_fallback_text" if fallback else None,
        )


def build_renderer(
    *,
    runtime_configs: Mapping[str, Any],
    all_runtime_configs: Mapping[str, Any],
    source_attribution: Mapping[str, Any],
) -> ChatmailRenderer | None:
    del all_runtime_configs
    configs = {
        rtc.adapter_id: rtc.config
        for rtc in runtime_configs.values()
        if rtc.enabled and rtc.config is not None
    }
    return ChatmailRenderer(configs, source_attribution) if configs else None
