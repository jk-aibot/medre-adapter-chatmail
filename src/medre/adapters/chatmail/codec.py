"""Convert complete native text snapshots into stable canonical evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone

from medre.core.events.canonical import CanonicalEvent, EventRelation, NativeRef
from medre.core.events.metadata import EventMetadata, NativeMetadata, TransportMetadata

from .config import ChatmailConfig, plain_text


def native_id(profile: str, message_id: int) -> str:
    """Local message IDs belong to a persistent Chatmail profile namespace."""
    return f"{profile}:{message_id}"


class ChatmailCodec:
    def __init__(self, config: ChatmailConfig, profile: str) -> None:
        self.config = config
        self.profile = profile

    def decode(
        self, snapshot: Mapping, *, timestamp: datetime
    ) -> CanonicalEvent | None:
        message_id = snapshot.get("id")
        conversation = snapshot.get("chatId")
        sender_id = snapshot.get("fromId")
        text = snapshot.get("text")
        sender = snapshot.get("sender")
        if (
            type(message_id) is not int
            or message_id <= 9
            or type(conversation) is not int
            or conversation not in self.config.chats
            or type(sender_id) is not int
            or sender_id <= 9
            or snapshot.get("state") not in {10, 13, 16}
            or snapshot.get("downloadState") != "Done"
            or snapshot.get("viewType") != "Text"
            or snapshot.get("isInfo") is not False
            or snapshot.get("isEdited") is not False
            or not isinstance(sender, Mapping)
            or sender.get("isBlocked") is not False
            or not plain_text(text)
            or not text.strip()
            or len(text.encode("utf-8")) > self.config.max_text_bytes
        ):
            return None
        channel = str(conversation)
        sent_timestamp = snapshot.get("timestamp")
        if type(sent_timestamp) is int and sent_timestamp > 0:
            try:
                timestamp = datetime.fromtimestamp(sent_timestamp, tz=timezone.utc)
            except (ValueError, OverflowError, OSError):
                return None
        reference = native_id(self.profile, message_id)
        identity = json.dumps([self.config.adapter_id, self.profile, message_id])
        relations = ()
        quote = snapshot.get("quote")
        if isinstance(quote, Mapping) and quote.get("kind") == "WithMessage":
            quoted_id, quoted_chat = quote.get("messageId"), quote.get("chatId")
            if type(quoted_id) is int and quoted_id > 9 and quoted_chat == conversation:
                relations = (
                    EventRelation(
                        relation_type="reply",
                        target_event_id=None,
                        target_native_ref=NativeRef(
                            self.config.adapter_id,
                            channel,
                            native_id(self.profile, quoted_id),
                        ),
                        key=None,
                        fallback_text=None,
                    ),
                )
        label = sender.get("displayName")
        if not plain_text(label) or not label.strip():
            label = f"contact {sender_id}"
        return CanonicalEvent(
            event_id="chatmail:" + hashlib.sha256(identity.encode()).hexdigest(),
            event_kind="message.created",
            schema_version=1,
            timestamp=timestamp,
            source_adapter=self.config.adapter_id,
            source_transport_id=f"{self.profile}:contact:{sender_id}",
            source_channel_id=channel,
            parent_event_id=None,
            lineage=(),
            relations=relations,
            payload={"body": text},
            source_native_ref=NativeRef(self.config.adapter_id, channel, reference),
            metadata=EventMetadata(
                transport=TransportMetadata(protocol="chatmail", substrate="ip"),
                native=NativeMetadata(
                    data={
                        "chatmail": {
                            "schema_version": 1,
                            "profile_id": self.profile,
                            "account_id": self.config.account_id,
                            "chat_id": conversation,
                            "message_id": message_id,
                            "contact_id": sender_id,
                            "sender_label": label,
                            "encrypted_and_signed": snapshot.get("showPadlock") is True,
                        }
                    }
                ),
            ),
        )
