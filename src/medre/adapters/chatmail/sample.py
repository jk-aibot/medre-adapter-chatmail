"""Offline sample values; no remote profile is provisioned."""

from medre.adapter_spec import AdapterSample


def sample_config() -> AdapterSample:
    return AdapterSample(
        {"account_id": 1, "chats": [10], "origin_label": "Chatmail"}, channel="10"
    )
