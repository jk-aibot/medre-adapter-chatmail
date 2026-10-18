# MEDRE Chatmail adapter

Bridge accepted Chatmail group conversations and direct chats through the official
Python RPC client and native Chatmail core. The provider supplies plain-text
rendering, durable ingress checkpoints, a bounded outbound queue, and an
SDK-free fake for offline development.

## Install

For a persistent bridge, keep core and adapters in one locked uv project:

```bash
uv init --bare medre-bridge
cd medre-bridge
uv add 'medre @ git+https://github.com/jeremiah-k/medre.git@main' \
  'medre-adapter-chatmail @ git+https://github.com/jeremiah-k/medre-adapter-chatmail.git@main'
```

Both RPC packages are installed together; no Rust build or separate daemon
installation is needed. Use immutable Git revisions for reproducible deployments.
`uv run --locked` preserves the project lock when starting commands.

## Set up a profile

```bash
uv run --locked medre-chatmail init --adapter mail
uv run --locked medre-chatmail accounts --adapter mail
uv run --locked medre-chatmail join --adapter mail
uv run --locked medre-chatmail chats --adapter mail --groups --json
```

`init` asks for a relay's private DCACCOUNT or DCLOGIN QR payload using hidden
terminal input. It creates a profile only on this explicit command and keeps an
existing configured account. `join` accepts a contact or group invitation.
Use `--qr-stdin` to supply a QR payload through standard input in automation;
credentials never belong in command arguments or committed files.

Profiles default to MEDRE's instance state directory, under
`adapters/mail/chatmail/accounts`. `MEDRE_HOME` and the usual XDG paths are
honored. Use the same `--adapter` and `--accounts-dir` options for setup and
runtime configuration. Once a runtime YAML exists, `--config bridge.yaml
--adapter mail` selects its profile and account directly.

The profile is dedicated to one adapter instance. Stop the bridge before running
profile commands; concurrent MEDRE commands refuse the profile lock. Do not open
the directory simultaneously in an unrelated Chatmail client. Keep the profile,
MEDRE database and pending-delivery journal together when backing up state.

Groups appear first in `chats`; `--groups` limits discovery to groups. Omit it
to include direct conversations. Use the accepted chat IDs from `chats` in the adapter configuration and routes:

```yaml
adapters:
  chatmail:
    mail:
      account_id: 1
      chats: [12] # Replace with the accepted chat IDs from this profile.
      origin_label: Chatmail
```

Add routes using decimal channel strings, such as `source_channel: "12"` or
`dest_channel: "12"`. Chat IDs are local to a profile; contact addresses are not
used to guess conversation identity. Run `medre config check` before starting
the bridge. Explicit `accounts_dir`, `state_dir` and `rpc_server_path` overrides
are available for existing profiles and native server installations.

## Delivery and rendering

- Preserve plain text and MEDRE attribution; represent replies and threads through
  core's inline fallback. Refuse text exceeding the configured UTF-8 budget.
- Baseline existing history on first use. Resume after the last durable checkpoint
  on subsequent starts, committing the cursor only after canonical admission.
- Keep partial text messages pending. Exclude self echoes, blocked contacts,
  informational controls, edits and unsupported media from fresh-message ingress.
- Return local queue admission as deferred delivery. Report SMTP acceptance only
  after the native core reports its sent state; this does not claim recipient
  delivery or reading.
- Retain exact attempt/native-ID correlation until core accepts delivery feedback.
  Recover correlated native jobs without resending. Abandon unsent or uncertain
  work recovered across a restart, requiring inspection before an explicit retry.
- Expose encryption/signature observations in native metadata. Confidentiality
  across the bridge and identity puppeting are not implied.

Attachments, native edits/deletes/reactions, forwarded read receipts, typing
notifications and puppeting are not advertised. The native core owns SMTP/IMAP,
cryptography, protocol retries and account storage. MEDRE owns routing and
lifecycle authority.

See the [provider contract](docs/spec/chatmail.md) and
[configuration schema](docs/schemas/config.schema.json).

## Develop and test

Create an isolated environment from checkouts at any paths:

```bash
uv venv
uv pip install -e /absolute/path/to/medre -e '.[test]'
uv run --no-sync pytest -q
uv run --no-sync pytest -q -m chatmail_sdk
```

The default tier excludes native SDK and live tests. The SDK tier runs the actual
pinned RPC client and server against temporary local profiles without an external
mail service. CI runs both tiers on Python 3.11–3.14 and qualifies the adapter
against core main; workflow dispatch accepts an exact candidate core revision.
The run summary records both resolved commit IDs. SDK constraints are exported
from core's lock without installing private core test helpers into this provider.

The [live test](tests/test_live.py) accepts an explicitly prepared relay/profile
fixture. See [live testing](docs/live-testing.md). All service fixtures use
ordinary container permissions; no host system configuration is required by this
provider's tests.
