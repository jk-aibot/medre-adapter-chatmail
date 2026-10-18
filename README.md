# MEDRE Chatmail adapter

Bridge accepted Chatmail groups and direct conversations through the official
Python RPC client and native core. The provider includes SDK-free discovery,
configuration, plain-text rendering and fake adapters.

Select conversations by local chat ID. Durable ingress checkpoints and deferred
delivery feedback preserve ownership across native queueing and restarts.

See the [provider contract](docs/spec/chatmail.md),
[configuration schema](docs/schemas/config.schema.json), and
[native relay tests](docs/live-testing.md).
