import pytest

from medre.adapters.chatmail.cli import execute, main, parser, read_qr, selected_config
from conftest import NativeSession


def test_help_does_not_open_profiles(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "chats" in capsys.readouterr().out


def test_default_paths_and_explicit_account_selection(monkeypatch, tmp_path):
    monkeypatch.setenv("MEDRE_HOME", str(tmp_path / "home"))
    options = parser().parse_args(["chats", "--adapter", "mail", "--account-id", "2"])
    config = selected_config(options)
    assert config.account_id == 2
    assert config.accounts_dir.endswith("/adapters/mail/chatmail/accounts")
    assert not (tmp_path / "home").exists()


async def test_accounts_and_chats_expose_identifiers_without_credentials(config):
    session = NativeSession(config)
    original = session.call

    async def call(name, *args):
        if name == "get_chatlist_entries":
            return [1, 10]
        if name == "get_basic_chat_info":
            return {
                "name": "Bridge",
                "isDeviceChat": False,
                "isContactRequest": False,
                "isEncrypted": True,
                "isSelfTalk": False,
                "chatType": "Group",
            }
        return await original(name, *args)

    session.call = call
    accounts = await execute(
        parser().parse_args(["accounts"]), config, session_factory=lambda _: session
    )
    assert accounts == [{"account_id": 1, "configured": True}]
    chats = await execute(
        parser().parse_args(["chats"]), config, session_factory=lambda _: session
    )
    assert chats == [
        {
            "chat_id": 10,
            "kind": "Group",
            "name": "Bridge",
            "accepted": True,
            "encrypted": True,
        }
    ]
    assert not session.started


async def test_init_is_idempotent_and_does_not_prompt_for_configured_accounts(
    config, monkeypatch
):
    monkeypatch.setattr(
        "medre.adapters.chatmail.cli.read_qr",
        lambda _: pytest.fail("unexpected prompt"),
    )
    session = NativeSession(config)
    result = await execute(
        parser().parse_args(["init"]), config, session_factory=lambda _: session
    )
    assert result["configured"] is True
    assert not any(name == "add_transport_from_qr" for name, _ in session.calls)


async def test_private_qr_is_sent_only_to_the_native_setup_call(config, monkeypatch):
    secret = "DCLOGIN:test-only-private-value"
    monkeypatch.setattr("medre.adapters.chatmail.cli.read_qr", lambda _: secret)
    session = NativeSession(config)
    original = session.call
    configured = False

    async def call(name, *args):
        nonlocal configured
        if name == "is_configured":
            return configured
        if name == "add_transport_from_qr":
            assert args == (1, secret)
            configured = True
            return None
        return await original(name, *args)

    session.call = call
    result = await execute(
        parser().parse_args(["init"]), config, session_factory=lambda _: session
    )
    assert secret not in str(result)


def test_noninteractive_qr_requires_explicit_stdin(monkeypatch):
    import io

    monkeypatch.setattr("sys.stdin", io.StringIO("DCACCOUNT:relay.test\n"))
    with pytest.raises(ValueError, match="qr-stdin"):
        read_qr(parser().parse_args(["init"]))
    assert (
        read_qr(parser().parse_args(["init", "--qr-stdin"])) == "DCACCOUNT:relay.test"
    )
