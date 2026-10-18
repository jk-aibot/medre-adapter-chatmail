"""Lazy discovery through released MEDRE provider contracts."""

from medre.adapter_spec import AdapterSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    return AdapterSpec(
        transport="chatmail",
        config=SymbolRef("medre.adapters.chatmail.config", "ChatmailConfig"),
        runtime_config=None,
        adapter=SymbolRef("medre.adapters.chatmail.adapter", "ChatmailAdapter"),
        fake_adapter=SymbolRef("medre.adapters.chatmail.fake", "FakeChatmailAdapter"),
        renderer_factory=SymbolRef(
            "medre.adapters.chatmail.renderer", "build_renderer"
        ),
        runtime_config_preparer=SymbolRef(
            "medre.adapters.chatmail.config", "prepare_config"
        ),
        dependency_probe=SymbolRef("medre.adapters.chatmail.compat", "HAS_CHATMAIL"),
        distribution="deltachat-rpc-client",
        import_names=("deltachat_rpc_client",),
        sdk_import_roots=("deltachat_rpc_client", "deltachat_rpc_server"),
        native_namespace_reader=SymbolRef(
            "medre.adapters.chatmail.metadata", "native_namespace"
        ),
        versioned_namespace_reader=SymbolRef(
            "medre.adapters.chatmail.metadata", "versioned"
        ),
        attribution_projector=SymbolRef(
            "medre.adapters.chatmail.metadata", "project_attribution"
        ),
        sample_config=SymbolRef("medre.adapters.chatmail.sample", "sample_config"),
        support_endpoint_fields=("account_id", "chats"),
        support_secret_fields=("accounts_dir", "state_dir"),
    )
