"""Durable ingress and deferred SMTP handoff through a dedicated profile."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from medre.core.contracts.adapter import (
    AdapterCapabilities,
    AdapterContext,
    AdapterContract,
    AdapterHandoffResult,
    AdapterInfo,
    AdapterPermanentError,
    AdapterRole,
    AdapterSendError,
)
from medre.core.contracts.delivery import (
    DeferredHandoffCompleted,
    DeferredHandoffFailed,
)
from medre.core.rendering.renderer import RenderingResult

from .codec import ChatmailCodec, native_id
from .config import ChatmailConfig, plain_text
from .session import ChatmailSession, RpcCallError, SessionError
from .state import Journal, PendingLimitError

CAPABILITIES = AdapterCapabilities(
    text=True,
    title=False,
    replies="fallback",
    threads="fallback",
    reactions="unsupported",
    edits="unsupported",
    deletes="unsupported",
    attachments=False,
    metadata_fields=False,
    store_and_forward=True,
    direct_messages=True,
    channels=True,
    identity_encryption=False,
    presence=False,
    topic_rooms=True,
    mesh_routing=False,
    priority_delivery=False,
)


def validate_payload(
    result: RenderingResult, config: ChatmailConfig
) -> tuple[int, str]:
    payload = result.payload
    if result.target_adapter != config.adapter_id or not isinstance(payload, Mapping):
        raise AdapterPermanentError("Invalid Chatmail delivery target or payload")
    try:
        conversation = config.destination(result.target_channel)
    except ValueError:
        raise AdapterPermanentError("Unconfigured Chatmail destination") from None
    text = payload.get("text")
    if (
        type(payload.get("chat_id")) is not int
        or payload["chat_id"] != conversation
        or set(payload) != {"chat_id", "text"}
        or not plain_text(text)
        or not text.strip()
        or len(text.encode("utf-8")) > config.max_text_bytes
    ):
        raise AdapterPermanentError("Invalid rendered Chatmail addressing or text")
    return conversation, text


class ChatmailAdapter(AdapterContract):
    platform = "chatmail"
    role = AdapterRole.HYBRID

    def __init__(
        self, config: ChatmailConfig, *, session_factory: Callable = ChatmailSession
    ) -> None:
        super().__init__()
        self.config = config.validate()
        self.adapter_id = config.adapter_id
        self._capabilities = replace(CAPABILITIES, max_text_bytes=config.max_text_bytes)
        self.ctx: AdapterContext | None = None
        self.session = None
        self.journal = None
        self.codec = None
        self._session_factory = session_factory
        self._task = None
        self._state = "unknown"
        self._profile = ""
        self._cursors: dict[int, int] = {}
        self._recovery_heads: dict[int, int] = {}
        self._stopping = False
        self._failed_ingress = 0
        self._reconnects = 0

    async def start(self, ctx: AdapterContext) -> None:
        if self.session is not None:
            raise RuntimeError("Chatmail adapter is started")
        if (
            not self.config.chats
            or not self.config.accounts_dir
            or not self.config.state_dir
        ):
            raise AdapterPermanentError(
                "Chatmail requires a prepared profile and configured chat IDs"
            )
        if any(
            getattr(ctx, name) is None
            for name in (
                "admit_inbound",
                "load_checkpoint",
                "commit_checkpoint",
                "report_delivery_feedback",
            )
        ):
            raise AdapterPermanentError(
                "Chatmail requires durable admission, checkpoints and delivery feedback"
            )
        self.ctx = ctx
        self._mark_started(ctx)
        self._stopping = False
        self._state = "starting"
        try:
            await self._connect()
            self.journal = await asyncio.to_thread(
                Journal, self.config.state_dir, self._profile
            )
            self._task = asyncio.create_task(
                self._run(), name=f"chatmail:{self.adapter_id}"
            )
        except BaseException:
            await self.stop()
            self._state = "failed"
            raise

    async def _connect(self) -> None:
        self.session = self._session_factory(self.config)
        await self.session.start()
        account = self.config.account_id
        await self.session.call("stop_io_for_all_accounts")
        if account not in await self.session.call(
            "get_all_account_ids"
        ) or not await self.session.call("is_configured", account):
            raise AdapterPermanentError("The selected Chatmail account is unconfigured")
        blob_dir = await self.session.call("get_blob_dir", account)
        if not isinstance(blob_dir, str) or not blob_dir:
            raise AdapterPermanentError("Chatmail profile identity is unavailable")
        profile = Path(blob_dir).parent.name
        if self._profile and profile != self._profile:
            raise AdapterPermanentError(
                "Chatmail native profile changed during recovery"
            )
        self._profile = profile
        self.codec = ChatmailCodec(self.config, profile)
        self._cursors.clear()
        for conversation in self.config.chats:
            info = await self.session.call("get_basic_chat_info", account, conversation)
            if (
                info.get("isContactRequest") is not False
                or info.get("isDeviceChat") is not False
                or info.get("isSelfTalk") is not False
                or info.get("chatType") not in {"Group", "Single"}
            ):
                raise AdapterPermanentError(
                    "Chatmail destinations must be accepted groups or direct conversations with other contacts"
                )
            members = await self.session.call(
                "get_chat_contacts", account, conversation
            )
            if not any(type(member) is int and member > 9 for member in members):
                raise AdapterPermanentError(
                    "Chatmail conversations require at least one other contact"
                )
            stream = f"chatmail:{account}:{conversation}"
            checkpoint = await self.ctx.load_checkpoint(stream)
            ids = await self.session.call(
                "get_message_ids", account, conversation, False, False
            )
            self._recovery_heads[conversation] = max(ids, default=0)
            if checkpoint is None:
                # I/O is stopped while first-use baselines are established.
                self._cursors[conversation] = max(ids, default=0)
                await self._checkpoint(conversation, self._cursors[conversation])
            else:
                try:
                    metadata = json.loads(checkpoint.metadata_json)
                    cursor = int(checkpoint.cursor)
                except (ValueError, TypeError):
                    raise AdapterPermanentError(
                        "Invalid Chatmail ingress checkpoint"
                    ) from None
                if metadata != {"profile_id": profile} or cursor < 0:
                    raise AdapterPermanentError(
                        "Chatmail checkpoint belongs to a different native profile"
                    )
                self._cursors[conversation] = cursor
        await self.session.call("start_io", account)
        self.session.wake.set()

    async def _checkpoint(self, conversation: int, cursor: int) -> None:
        await self.ctx.commit_checkpoint(
            f"chatmail:{self.config.account_id}:{conversation}",
            str(cursor),
            json.dumps({"profile_id": self._profile}),
        )
        self._cursors[conversation] = cursor

    async def _scan(self) -> None:
        for conversation in self.config.chats:
            ids = await self.session.call(
                "get_message_ids", self.config.account_id, conversation, False, False
            )
            pending = sorted(i for i in ids if i > self._cursors[conversation])
            if len(pending) > 32:
                self.session.wake.set()
            for message_id in pending[:32]:
                snapshot = await self.session.call(
                    "get_message", self.config.account_id, message_id
                )
                if snapshot.get("chatId") != conversation:
                    raise SessionError(
                        "Chatmail message changed conversation during admission"
                    )
                # A partial text message may become complete using the same
                # native ID. Keep its cursor pending rather than lose it.
                if (
                    snapshot.get("viewType") == "Text"
                    and snapshot.get("downloadState") != "Done"
                ):
                    break
                event = self.codec.decode(snapshot, timestamp=self.ctx.clock())
                if event is not None:
                    try:
                        provenance = (
                            "recovered"
                            if message_id <= self._recovery_heads[conversation]
                            else "live"
                        )
                        await self.admit_inbound(event, provenance)
                    except Exception:
                        self._failed_ingress += 1
                        raise
                # Unsupported controls/media do not enter message history.
                # Admission and checkpoint failures leave this ID retryable.
                await self._checkpoint(conversation, message_id)
                await asyncio.sleep(0)

    async def deliver(self, result: RenderingResult) -> AdapterHandoffResult:
        conversation, text = validate_payload(result, self.config)
        provenance = result.attempt_provenance
        if (
            provenance is None
            or self.ctx is None
            or self.ctx.report_delivery_feedback is None
        ):
            raise AdapterPermanentError(
                "Deferred Chatmail delivery requires attempt provenance and feedback"
            )
        if (
            provenance.event_id,
            provenance.target_adapter,
            provenance.target_channel,
        ) != (result.event_id, result.target_adapter, result.target_channel):
            raise AdapterPermanentError(
                "Chatmail attempt provenance contradicts its delivery target"
            )
        if (
            self._stopping
            or self.journal is None
            or self._task is None
            or self._task.done()
        ):
            raise AdapterSendError("Chatmail adapter is not accepting deliveries")
        try:
            await asyncio.to_thread(
                self.journal.enqueue,
                provenance,
                conversation,
                text,
                self.config.max_pending,
            )
        except PendingLimitError:
            raise AdapterSendError(
                "Chatmail durable queue is full",
                retry_after_seconds=self.config.poll_interval_seconds,
            ) from None
        if self.session is not None:
            self.session.wake.set()
        return AdapterHandoffResult(
            disposition="deferred",
            confirmation_level="local_queue",
            native_channel_id=result.target_channel,
            note="Retained in the private Chatmail queue; SMTP handoff is pending",
        )

    async def _flush(self) -> None:
        for row in await asyncio.to_thread(self.journal.rows):
            provenance = self.journal.provenance(row)
            if row["status"] == "queued":
                await asyncio.to_thread(
                    self.journal.update, row["identity"], "submitting"
                )
                # Commit the claim before invoking the external native core.
                # A crash before correlating its reply is terminal uncertainty,
                # never permission to send the same attempt a second time.
                if row["chat"] not in self.config.chats:
                    await asyncio.to_thread(
                        self.journal.update, row["identity"], "abandoned"
                    )
                    self.session.wake.set()
                    continue
                members = await self.session.call(
                    "get_chat_contacts", self.config.account_id, row["chat"]
                )
                if not any(type(member) is int and member > 9 for member in members):
                    await asyncio.to_thread(
                        self.journal.update, row["identity"], "abandoned"
                    )
                    self.session.wake.set()
                    continue
                try:
                    message_id = await self.session.call(
                        "send_msg",
                        self.config.account_id,
                        row["chat"],
                        {"text": row["body"]},
                    )
                except RpcCallError:
                    self.session.wake.set()
                    continue
                if type(message_id) is not int or message_id <= 9:
                    raise SessionError("Chatmail returned an invalid queued message ID")
                await asyncio.to_thread(
                    self.journal.update, row["identity"], "pending", message_id
                )
                row["status"], row["native"] = "pending", message_id
            if row["status"] in {"submitting", "abandoned"}:
                feedback = DeferredHandoffFailed(
                    attempt_provenance=provenance,
                    outcome="abandoned",
                    error="Native submission outcome is uncertain; inspect Chatmail before resending",
                )
            else:
                try:
                    snapshot = await self.session.call(
                        "get_message", self.config.account_id, row["native"]
                    )
                except RpcCallError:
                    await asyncio.to_thread(
                        self.journal.update, row["identity"], "abandoned"
                    )
                    self.session.wake.set()
                    continue
                if (
                    snapshot.get("id"),
                    snapshot.get("chatId"),
                    snapshot.get("fromId"),
                ) != (row["native"], row["chat"], 1):
                    raise AdapterPermanentError(
                        "Chatmail queued message identity does not match its attempt"
                    )
                state = snapshot.get("state")
                if state in {26, 28}:
                    feedback = DeferredHandoffCompleted(
                        attempt_provenance=provenance,
                        handoff=AdapterHandoffResult(
                            disposition="transport_handoff",
                            confirmation_level="remote_service",
                            native_message_id=native_id(self._profile, row["native"]),
                            native_channel_id=str(row["chat"]),
                            note="Chatmail reports SMTP acceptance; recipient delivery and reading are unconfirmed",
                        ),
                    )
                elif state == 24:
                    feedback = DeferredHandoffFailed(
                        attempt_provenance=provenance,
                        outcome="permanent_failed",
                        error="Chatmail reports terminal native send failure",
                    )
                else:
                    continue
            await self.ctx.report_delivery_feedback(feedback)
            await asyncio.to_thread(self.journal.remove, row["identity"])

    async def _run(self) -> None:
        while not self._stopping:
            try:
                self.session.wake.clear()
                if not self.session.alive():
                    raise SessionError("Chatmail native process is unavailable")
                await self._scan()
                await self._flush()
                connectivity = await self.session.call(
                    "get_connectivity", self.config.account_id
                )
                self._state = "healthy" if connectivity >= 3000 else "degraded"
                try:
                    await asyncio.wait_for(
                        self.session.wake.wait(), self.config.poll_interval_seconds
                    )
                except TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except AdapterPermanentError:
                self._state = "failed"
                return
            except SessionError:
                self._state = "degraded"
                try:
                    await self.session.stop()
                except Exception:
                    self._state = "failed"
                    return
                await asyncio.sleep(self.config.reconnect_delay_seconds)
                try:
                    await self._connect()
                    self._reconnects += 1
                except AdapterPermanentError:
                    self._state = "failed"
                    return
                except Exception:
                    self._state = "degraded"
            except Exception:
                # Storage/admission/feedback remains owned by its durable
                # source. Retry without discarding native or queued work.
                self._state = "degraded"
                await asyncio.sleep(self.config.poll_interval_seconds)

    async def stop(self, timeout: float = 5.0) -> None:
        self._stopping = True
        self._state = "stopping"
        if self._task is not None:
            task, self._task = self._task, None
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self.session is not None:
            session, self.session = self.session, None
            await session.stop(timeout=timeout)
        if self.journal is not None:
            journal, self.journal = self.journal, None
            await asyncio.to_thread(journal.close)
        self._state = "unknown"

    def diagnostics(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "failed_ingress": self._failed_ingress,
            "reconnects": self._reconnects,
            "account_id": self.config.account_id,
            "configured_chats": len(self.config.chats),
        }

    async def health_check(self) -> AdapterInfo:
        return AdapterInfo(
            self.adapter_id,
            self.platform,
            self.role,
            "0.1.0",
            self._capabilities,
            health=self._state,
        )
