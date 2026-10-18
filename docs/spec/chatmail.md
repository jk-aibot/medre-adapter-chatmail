# Chatmail provider contract

## Boundary

The `chatmail` provider is a hybrid adapter. It delegates account storage,
SMTP/IMAP, encryption and wire retries to the official RPC client and server.
Registration, configuration, metadata projection, rendering and fake adapters
MUST remain importable without the SDK.

Runtime MUST use an explicitly selected configured account and an allowlist of
accepted ordinary chats. A profile MUST have one MEDRE owner. Configuration
loading and runtime preflight MUST NOT provision accounts or create directories.
Profile provisioning is an explicit operator action. Native client and server versions
MUST agree with the native core version returned by the running server.

## Identity and ingress

Chat IDs, contact IDs and message IDs are local to a native profile. Canonical
IDs and native references MUST include the profile identity, preventing local ID
reuse from binding evidence from different profiles. Email addresses MUST NOT be
used as contact identity lookup keys.

First-use checkpoints baseline existing chat history before native I/O starts.
On restart, messages beyond the durable cursor are admitted with recovered
provenance; subsequent arrivals use live provenance. Native timestamps are
preserved when present. Complete eligible text is admitted through the core's
durable admission seam before its cursor advances. A checkpoint failure MUST
leave the message retryable with the same canonical identity. A partial text
message MUST NOT advance its cursor; other chats may continue independently.

SDK notifications are coalesced into a wakeup rather than retained in unbounded
Python account queues. Native storage remains the recovery source. A scan batch
handles at most 32 messages per chat before yielding to outbound processing.
The native API enumerates a chat's IDs without pagination; large chat histories
therefore have enumeration cost even though payload processing is bounded.

Controls, blocked contacts, self echoes, edits and media MUST NOT masquerade as
fresh text. Native reply references are restricted to the same profile and chat;
outbound replies and threads use core's plain-text fallback. Encryption/signature
metadata describes native evidence and MUST NOT imply bridge confidentiality.

## Outbound ownership

`deliver` accepts only renderer-ready payloads for an allowed chat. It MUST reject
before admission when immutable attempt provenance or the feedback sink is
absent, and MUST reject contradictory event/target provenance.

The private SQLite journal bounds queued attempts using `max_pending`. Admission
returns `deferred` with `local_queue` confirmation and no native message ID. The
worker persists a submission claim before invoking native send. A correlated
native message remains pending until the server reports state 26 or 28, which
permits a `remote_service` handoff fact for SMTP acceptance. State 24 produces
permanent failure. These facts MUST NOT imply recipient delivery or reading.

Exact provenance remains associated with the native ID until core acknowledges
feedback. Feedback failure MUST NOT resubmit the native message. Recovery polls
correlated native jobs; it MUST abandon recovered unsent work and uncertain
submissions rather than dispatch a stale attempt or risk a duplicate send.
Native message identity and chat ownership MUST match before reporting handoff.
The journal is adapter evidence, not authority over core outbox or retry state.

## Lifecycle and capabilities

Native blocking operations execute outside the event loop with bounded waits.
Timeout and cancellation terminate the owned native process and drain its client
threads. Transient session failure reconnects through a fresh profile-owning
session. Shutdown stops ingress and retains pending native correlation.
Diagnostics MUST omit private paths, QR payloads and credentials.

Capabilities advertise text, direct and group conversations, store-and-forward,
and fallback replies/threads. Each instance exposes its configured text budget.
Attachments, native mutations, read receipt forwarding, typing notifications,
presence, puppeting and bridge-wide encryption are not advertised.
