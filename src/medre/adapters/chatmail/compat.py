"""Optional SDK availability without importing its client implementation."""

from importlib.util import find_spec

HAS_CHATMAIL = find_spec("deltachat_rpc_client") is not None
