from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Optional
from uuid import UUID


class HashVerificationError(Exception):
    pass


GENESIS_HASH = "0" * 64
HASH_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


def generate_event_hash(
    previous_hash: Optional[str],
    payload: Dict[str, Any],
    timestamp_ms: int,
    sequence_id: int,
    tenant_id: UUID,
    aggregate_id: UUID,
) -> str:
    hash_input = previous_hash or GENESIS_HASH
    payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    raw = f"{tenant_id}|{aggregate_id}|{hash_input}|{payload_str}|{timestamp_ms}|{sequence_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_chain(
    previous_hash: Optional[str],
    current_hash: str,
    payload: Dict[str, Any],
    timestamp_ms: int,
    sequence_id: int,
    tenant_id: UUID,
    aggregate_id: UUID,
) -> None:
    if previous_hash is not None and not HASH_HEX64_RE.fullmatch(previous_hash):
        raise HashVerificationError(
            "Invalid previous_hash format. Expected 64-char lowercase SHA-256 hex, "
            f"got {len(previous_hash)} chars."
        )

    if not HASH_HEX64_RE.fullmatch(current_hash):
        raise HashVerificationError(
            "Invalid event_hash format. Expected 64-char lowercase SHA-256 hex, "
            f"got {len(current_hash)} chars."
        )

    expected = generate_event_hash(
        previous_hash,
        payload,
        timestamp_ms,
        sequence_id,
        tenant_id,
        aggregate_id,
    )
    if current_hash != expected:
        raise HashVerificationError(
            f"Chain broken at sequence {sequence_id}. Expected {expected}, got {current_hash}."
        )
