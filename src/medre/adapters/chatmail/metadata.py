"""Versioned native facts and attribution, independent of the SDK."""

from collections.abc import Mapping
from typing import Any

from medre.adapters.metadata import current_namespace, versioned_namespace


def native_namespace(data: Mapping[str, Any]) -> Mapping[str, Any]:
    return current_namespace(data, "chatmail", 1)


def versioned(data: Mapping[str, Any]) -> Mapping[str, Any]:
    return versioned_namespace(data, "chatmail")


def project_attribution(
    data: Mapping[str, Any],
    *,
    source_transport_id: str | None = None,
    compact: bool = False,
) -> dict[str, str | None]:
    native = native_namespace(data)
    sender = source_transport_id
    label = native.get("sender_label") or sender
    if compact and isinstance(label, str):
        label = label.replace(" ", "")
    return {
        "source_sender_id": sender,
        "source_sender_label": label,
        "source_sender_short_label": label,
        "source_sender_handle": sender,
    }
