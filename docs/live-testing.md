# Native relay tests

Provision two dedicated Chatmail profiles with configured relay accounts and an
accepted encrypted group conversation between them. Keep their directories separate;
each native server owns its profile exclusively. The two profiles may use the
same relay. The test does not deploy or reconfigure that relay.

Write a private JSON fixture outside the checkout:

```json
{
  "bot_accounts_dir": "/private/medre-chatmail-bot",
  "bot_account_id": 1,
  "bot_chat_id": 12,
  "peer_accounts_dir": "/private/medre-chatmail-peer",
  "peer_account_id": 1,
  "peer_chat_id": 15
}
```

Use actual account and chat IDs from the provisioning commands. Restrict fixture
permissions to its owner and stop clients using these profiles before running:

```bash
MEDRE_CHATMAIL_TEST_CONFIG=/private/chatmail-live.json \
  uv run --no-sync pytest tests/test_live.py -m live -q
```

The test sends nonce text in both directions. It observes native ingress through
the adapter, deferred SMTP handoff feedback, and independent native peer receipt.
The fixture must select the same group in both profiles. Its core admission/checkpoint callbacks are test sinks; it does not claim a full
routed core-storage proof. Queue correlation uses a temporary real SQLite journal.

The [official Docker relay](https://github.com/chatmail/docker) is a deployment
option. Its systemd/cgroup requirements are separate from the adapter and are not
enabled by the tests. Do not change host settings or grant container privileges
to run this suite. Use a preconfigured relay or a dedicated disposable machine
for that deployment. The default CI tier needs neither Docker nor a relay.
