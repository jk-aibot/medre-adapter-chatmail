import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.core.contracts.adapter import AdapterPermanentError, AdapterSendError
from medre.core.contracts.delivery import (
    DeferredHandoffCompleted,
    DeferredHandoffFailed,
)
from medre.adapters.chatmail.adapter import ChatmailAdapter
from medre.adapters.chatmail.session import RpcCallError, SessionError
from medre.adapters.chatmail.state import Journal, PendingLimitError, ProfileLease

from conftest import NativeSession, attempt, rendered, snapshot, wait_until


class ManualIngressAdapter(ChatmailAdapter):
    """Drive ingress steps explicitly without a concurrent background scan."""

    async def _run(self):
        await asyncio.Event().wait()


def test_profile_lease_excludes_other_commands_and_releases_on_close(config):
    lease = ProfileLease(config.accounts_dir)
    try:
        with pytest.raises(RuntimeError, match="in use"):
            ProfileLease(config.accounts_dir)
    finally:
        lease.close()
    ProfileLease(config.accounts_dir).close()


def test_journal_bounds_exact_attempts_and_recovers_without_resubmission(config):
    journal = Journal(config.state_dir, "profile")
    journal.enqueue(attempt(), 10, "one", 1)
    journal.enqueue(attempt(), 10, "one", 1)
    with pytest.raises(PendingLimitError):
        journal.enqueue(attempt(2), 10, "two", 1)
    with pytest.raises(ValueError, match="contradicts"):
        journal.enqueue(attempt(), 10, "changed", 1)
    with pytest.raises(ValueError, match="contradicts"):
        journal.enqueue(replace(attempt(), source="retry"), 10, "one", 1)
    row = journal.rows()[0]
    assert journal.provenance(row) == attempt()
    journal.close()
    journal = Journal(config.state_dir, "profile")
    assert journal.rows()[0]["status"] == "abandoned"
    journal.close()
    with pytest.raises(ValueError, match="different"):
        Journal(config.state_dir, "other-profile")


async def test_ingress_baseline_live_admission_and_checkpoint_retry(config, context):
    ctx, checkpoints = context
    session = NativeSession(config, {20: snapshot()})
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        assert not ctx.admit_inbound.called
        assert checkpoints["chatmail:1:10"].cursor == "20"
        session.messages[21] = snapshot(21)
        ctx.commit_checkpoint.side_effect = RuntimeError("storage unavailable")
        with pytest.raises(RuntimeError):
            await adapter._scan()
        assert adapter._cursors[10] == 20

        async def commit(stream, cursor, metadata):
            checkpoints[stream] = SimpleNamespace(cursor=cursor, metadata_json=metadata)

        ctx.commit_checkpoint.side_effect = commit
        await adapter._scan()
        assert adapter._cursors[10] == 21
        calls = ctx.admit_inbound.call_args_list
        assert calls[-1].args[0].event_id == calls[-2].args[0].event_id
        assert calls[-1].args[1] == "live"
    finally:
        await adapter.stop()


async def test_restored_cursor_recovers_offline_messages_and_holds_partial_text(
    config, context
):
    ctx, checkpoints = context
    checkpoints["chatmail:1:10"] = SimpleNamespace(
        cursor="19", metadata_json=json.dumps({"profile_id": "profile-uuid"})
    )
    session = NativeSession(
        config, {20: snapshot(downloadState="Available"), 21: snapshot(21)}
    )
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await adapter._scan()
        assert not ctx.admit_inbound.called
        assert adapter._cursors[10] == 19
        session.messages[20]["downloadState"] = "Done"
        await adapter._scan()
        assert ctx.admit_inbound.await_count == 2
        assert all(c.args[1] == "recovered" for c in ctx.admit_inbound.call_args_list)
    finally:
        await adapter.stop()


async def test_admission_failure_does_not_advance_cursor(config, context):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        ctx.admit_inbound.side_effect = RuntimeError("admission rejected")
        session.messages[20] = snapshot()
        with pytest.raises(RuntimeError):
            await adapter._scan()
        assert adapter._cursors[10] == 0
        assert adapter.diagnostics()["failed_ingress"] >= 1
    finally:
        await adapter.stop()


@pytest.mark.parametrize(
    "state,expected",
    [
        (26, DeferredHandoffCompleted),
        (28, DeferredHandoffCompleted),
        (24, DeferredHandoffFailed),
    ],
)
async def test_delivery_reports_only_native_terminal_evidence(
    config, context, state, expected
):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ChatmailAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        with pytest.raises(AdapterPermanentError, match="provenance"):
            await adapter.deliver(rendered())
        handoff = await adapter.deliver(rendered(attempt()))
        assert handoff.disposition == "deferred"
        assert handoff.confirmation_level == "local_queue"
        assert handoff.native_message_id is None
        await wait_until(lambda: bool(session.sent))
        await wait_until(lambda: adapter.journal.rows()[0]["status"] == "pending")
        assert not ctx.report_delivery_feedback.called
        message = next(m for m in session.messages.values() if m["fromId"] == 1)
        message["state"] = state
        await adapter._flush()
        feedback = ctx.report_delivery_feedback.call_args.args[0]
        assert isinstance(feedback, expected)
        assert feedback.attempt_provenance == attempt()
        if state in {26, 28}:
            assert feedback.handoff.confirmation_level == "remote_service"
        assert not adapter.journal.rows()
    finally:
        await adapter.stop()


async def test_feedback_failure_retains_native_correlation_without_resending(
    config, context
):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ChatmailAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await adapter.deliver(rendered(attempt()))
        await wait_until(lambda: bool(session.sent))
        await wait_until(lambda: adapter.journal.rows()[0]["status"] == "pending")
        session.messages[10]["state"] = 26
        ctx.report_delivery_feedback.side_effect = RuntimeError("sink unavailable")
        with pytest.raises(RuntimeError):
            await adapter._flush()
        assert len(adapter.journal.rows()) == 1
        ctx.report_delivery_feedback.side_effect = None
        await adapter._flush()
        assert len(session.sent) == 1
        assert not adapter.journal.rows()
    finally:
        await adapter.stop()


async def test_uncertain_native_submission_is_abandoned_without_retry(config, context):
    ctx, _ = context
    session = NativeSession(config)
    original = session.call

    async def call(name, *args):
        if name == "send_msg":
            session.sent.append(args)
            raise RpcCallError("native rejected")
        return await original(name, *args)

    session.call = call
    adapter = ChatmailAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await adapter.deliver(rendered(attempt()))
        await wait_until(lambda: ctx.report_delivery_feedback.called)
        assert ctx.report_delivery_feedback.call_args.args[0].outcome == "abandoned"
        assert len(session.sent) == 1
    finally:
        await adapter.stop()


async def test_restart_polls_correlated_native_jobs_without_sending_again(
    config, context
):
    ctx, _ = context
    session = NativeSession(config, {20: snapshot(20, fromId=1, state=26)})
    journal = Journal(config.state_dir, "profile-uuid")
    journal.enqueue(attempt(), 10, "body", 100)
    row = journal.rows()[0]
    journal.update(row["identity"], "pending", 20)
    journal.close()
    adapter = ChatmailAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await wait_until(lambda: ctx.report_delivery_feedback.called)
        assert not session.sent
        assert isinstance(
            ctx.report_delivery_feedback.call_args.args[0], DeferredHandoffCompleted
        )
    finally:
        await adapter.stop()


async def test_start_refuses_wrong_profile_and_missing_durable_seams(config, context):
    ctx, checkpoints = context
    adapter = ChatmailAdapter(config, session_factory=NativeSession)
    ctx.admit_inbound = None
    with pytest.raises(AdapterPermanentError, match="durable"):
        await adapter.start(ctx)
    ctx.admit_inbound = AsyncMock()
    checkpoints["chatmail:1:10"] = SimpleNamespace(
        cursor="19", metadata_json='{"profile_id":"other"}'
    )
    with pytest.raises(AdapterPermanentError, match="different"):
        await adapter.start(ctx)
    assert adapter.session is None


async def test_stopped_and_full_queues_refuse_delivery(config, context):
    session = NativeSession(config)
    adapter = ChatmailAdapter(
        replace(config, max_pending=1), session_factory=lambda _: session
    )
    with pytest.raises(AdapterSendError):
        await adapter.deliver(rendered(attempt()))
    await adapter.start(context[0])
    try:
        await adapter.deliver(rendered(attempt()))
        with pytest.raises(AdapterSendError, match="full"):
            await adapter.deliver(rendered(attempt(2)))
    finally:
        await adapter.stop()
    await adapter.stop()


@pytest.mark.parametrize("kind,members", [("Group", [1, 12, 13]), ("Single", [12])])
async def test_group_and_direct_chats_share_channel_routing(
    config, context, kind, members
):
    session = NativeSession(config)
    original = session.call

    async def call(name, *args):
        if name == "get_basic_chat_info":
            return {
                "chatType": kind,
                "isContactRequest": False,
                "isDeviceChat": False,
                "isSelfTalk": False,
            }
        if name == "get_chat_contacts":
            return members
        return await original(name, *args)

    session.call = call
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(context[0])
    try:
        session.messages[20] = snapshot(20, fromId=12)
        session.messages[21] = snapshot(21, fromId=13 if kind == "Group" else 12)
        await adapter._scan()
        events = [call.args[0] for call in context[0].admit_inbound.call_args_list]
        assert {event.source_channel_id for event in events} == {"10"}
        assert len({event.event_id for event in events}) == 2
        assert len({event.source_transport_id for event in events}) == (
            2 if kind == "Group" else 1
        )
    finally:
        await adapter.stop()


@pytest.mark.parametrize(
    "updates,members",
    [
        ({"isSelfTalk": True}, [1]),
        ({"chatType": "OutBroadcast"}, [1, 12]),
        ({"isContactRequest": True}, [12]),
        ({}, [1]),
    ],
)
async def test_self_only_special_and_unaccepted_chats_refuse_startup(
    config, context, updates, members
):
    session = NativeSession(config)
    original = session.call

    async def call(name, *args):
        if name == "get_basic_chat_info":
            return {
                "chatType": "Group",
                "isContactRequest": False,
                "isDeviceChat": False,
                "isSelfTalk": False,
                **updates,
            }
        if name == "get_chat_contacts":
            return members
        return await original(name, *args)

    session.call = call
    adapter = ChatmailAdapter(config, session_factory=lambda _: session)
    with pytest.raises(AdapterPermanentError):
        await adapter.start(context[0])
    assert adapter.session is None


async def test_one_chat_admission_failure_does_not_starve_other_chats(config, context):
    ctx, _ = context
    config = replace(config, chats=(10, 11))
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        session.messages[20] = snapshot(20, chatId=10)
        session.messages[21] = snapshot(21, chatId=11)

        async def admit(event, provenance, *, attachment=None):
            if event.source_channel_id == "10":
                raise RuntimeError("chat 10 admission unavailable")

        ctx.admit_inbound.side_effect = admit
        with pytest.raises(RuntimeError, match="chat 10"):
            await adapter._scan()
        assert adapter._cursors[10] == 0
        assert adapter._cursors[11] == 21
        assert ctx.admit_inbound.await_count == 2
    finally:
        await adapter.stop()


async def test_ingress_failure_does_not_starve_outbound_feedback(config, context):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ChatmailAdapter(
        replace(config, poll_interval_seconds=0.01),
        session_factory=lambda _: session,
    )
    await adapter.start(ctx)
    try:
        ctx.admit_inbound.side_effect = RuntimeError("checkpoint admission unavailable")
        session.messages[20] = snapshot(20)
        await adapter.deliver(rendered(attempt()))
        await wait_until(lambda: bool(session.sent))
        message_id = next(i for i, m in session.messages.items() if m["fromId"] == 1)
        session.messages[message_id]["state"] = 26
        await wait_until(lambda: ctx.report_delivery_feedback.called)
        assert adapter._cursors[10] == 0
        assert isinstance(
            ctx.report_delivery_feedback.call_args.args[0], DeferredHandoffCompleted
        )
    finally:
        await adapter.stop()


async def test_reconnect_does_not_publish_partially_loaded_cursors(config, context):
    config = replace(config, chats=(10, 11))
    first = NativeSession(config)
    second = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: first)
    await adapter.start(context[0])
    original = second.call

    async def call(name, *args):
        if name == "get_basic_chat_info" and args[-1] == 11:
            raise SessionError("native info temporarily unavailable")
        return await original(name, *args)

    second.call = call
    adapter._session_factory = lambda _: second
    try:
        baseline = dict(adapter._cursors)
        with pytest.raises(SessionError, match="temporarily"):
            await adapter._connect()
        assert adapter._cursors == baseline
    finally:
        await adapter.stop()
        await first.stop()


async def test_undecipherable_text_skips_terminal_item_and_advances(config, context):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        session.messages[20] = snapshot(20, downloadState="Undecipherable")
        session.messages[21] = snapshot(21)
        await adapter._scan()
        assert ctx.admit_inbound.await_count == 1
        assert ctx.admit_inbound.call_args.args[
            0
        ].source_native_ref.native_message_id.endswith(":21")
        assert adapter._cursors[10] == 21
    finally:
        await adapter.stop()


async def test_rejected_preflight_reports_definite_failure_without_send(
    config, context
):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await adapter.deliver(rendered(attempt()))
        original = session.call

        async def call(name, *args):
            if name == "get_chat_contacts":
                return [1]
            return await original(name, *args)

        session.call = call
        await adapter._flush()
        assert not session.sent
        report = ctx.report_delivery_feedback.call_args.args[0]
        assert isinstance(report, DeferredHandoffFailed)
        assert report.outcome == "permanent_failed"
        assert not adapter.journal.rows()
    finally:
        await adapter.stop()


async def test_preflight_outage_does_not_claim_uncertain_submission(config, context):
    ctx, _ = context
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(ctx)
    try:
        await adapter.deliver(rendered(attempt()))
        original = session.call

        async def call(name, *args):
            if name == "get_chat_contacts":
                raise SessionError("native unavailable")
            return await original(name, *args)

        session.call = call
        with pytest.raises(SessionError):
            await adapter._flush()
        assert adapter.journal.rows()[0]["status"] == "queued"
    finally:
        await adapter.stop()


async def test_available_text_requests_one_native_download_until_state_changes(
    config, context
):
    session = NativeSession(config)
    adapter = ManualIngressAdapter(config, session_factory=lambda _: session)
    await adapter.start(context[0])
    try:
        session.messages[20] = snapshot(20, downloadState="Available")
        await adapter._scan()
        await adapter._scan()
        assert [
            call for call in session.calls if call[0] == "download_full_message"
        ] == [("download_full_message", (1, 20))]
        assert adapter._cursors[10] == 0
        session.messages[20]["downloadState"] = "Done"
        await adapter._scan()
        assert adapter._cursors[10] == 20
        assert context[0].admit_inbound.await_count == 1
    finally:
        await adapter.stop()


async def test_failed_partial_reconnect_closes_session_and_recovers(config, context):
    config = replace(
        config,
        chats=(10, 11),
        reconnect_delay_seconds=0.005,
        poll_interval_seconds=0.005,
    )
    sessions = [NativeSession(config) for _ in range(3)]
    original = sessions[1].call

    async def incomplete(name, *args):
        if name == "get_basic_chat_info" and args[-1] == 11:
            raise SessionError("native metadata unavailable")
        return await original(name, *args)

    sessions[1].call = incomplete
    created = iter(sessions)
    adapter = ChatmailAdapter(config, session_factory=lambda _: next(created))
    await adapter.start(context[0])
    try:
        sessions[0].started = False
        sessions[0].wake.set()
        await wait_until(lambda: adapter._reconnects > 0)
        assert not sessions[1].started
        assert sessions[2].started
        assert adapter._cursors == {10: 0, 11: 0}
    finally:
        await adapter.stop()
