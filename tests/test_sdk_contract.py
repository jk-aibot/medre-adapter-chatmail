"""Real released client/server contracts without an external mail service."""

import asyncio
from dataclasses import replace

import pytest

from medre.adapters.chatmail.session import ChatmailSession, RpcCallError, SessionError

pytestmark = pytest.mark.chatmail_sdk


async def test_native_profile_protocol_and_exclusive_lifecycle(config):
    session = ChatmailSession(config)
    await session.start()
    process = session.rpc.process
    try:
        account = await session.call("add_account")
        assert account in await session.call("get_all_account_ids")
        assert await session.call("is_configured", account) is False
        await session.call("set_config", account, "displayname", "MEDRE test")
        assert await session.call("get_config", account, "displayname") == "MEDRE test"
        group = await session.call("create_group_chat", account, "MEDRE group", False)
        info = await session.call("get_basic_chat_info", account, group)
        assert info["chatType"] == "Group"
        assert info["name"] == "MEDRE group"
        assert info["isDeviceChat"] is False
        with pytest.raises(RpcCallError):
            await session.call("medre_invalid_method")
        assert session.alive()
        other = ChatmailSession(config)
        with pytest.raises(RuntimeError, match="in use"):
            await other.start()
    finally:
        await session.stop(timeout=1)
    assert process.poll() is not None
    assert not session._active
    await session.stop()


async def test_invalid_executable_releases_profile_lease(config):
    session = ChatmailSession(
        replace(config, rpc_server_path="/medre-missing-native-server")
    )
    with pytest.raises(SessionError):
        await session.start()
    assert session.lease is None
    replacement = ChatmailSession(config)
    await replacement.start()
    await replacement.call("add_account")
    await replacement.stop(timeout=1)


async def test_hung_native_rpc_is_killed_and_drained(config):
    session = ChatmailSession(config)
    await session.start()
    process = session.rpc.process
    original = session.config
    try:
        # The long-poll blocks when there are no events; an independent
        # never-returning callable pins the wrapper's ownership behavior.
        session.config = replace(config, rpc_timeout_seconds=0.05)

        def wait_for_process():
            process.wait()

        with pytest.raises(SessionError):
            await session._run(wait_for_process)
        await asyncio.wait_for(
            asyncio.gather(*session._active, return_exceptions=True), 2
        )
        assert process.poll() is not None
    finally:
        session.config = original
        await session.stop(timeout=1)
