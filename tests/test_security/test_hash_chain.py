import re
from uuid import uuid4

import pytest

from app.security.hash_chain import HashVerificationError, generate_event_hash, verify_chain


def test_hash_binds_tenant_and_aggregate_identity():
    payload = {"event_type": "ResourceAllocationRequested", "x": 1}
    timestamp = 1700000000000
    sequence_id = 1
    previous_hash = None
    aggregate_id = uuid4()

    hash_tenant_a = generate_event_hash(
        previous_hash,
        payload,
        timestamp,
        sequence_id,
        tenant_id=uuid4(),
        aggregate_id=aggregate_id,
    )
    hash_tenant_b = generate_event_hash(
        previous_hash,
        payload,
        timestamp,
        sequence_id,
        tenant_id=uuid4(),
        aggregate_id=aggregate_id,
    )

    assert hash_tenant_a != hash_tenant_b


def test_verify_chain_rejects_wrong_stream_identity():
    payload = {"event_type": "ResourceAllocationRequested", "x": 1}
    timestamp = 1700000000000
    sequence_id = 1
    tenant_id = uuid4()
    aggregate_id = uuid4()
    event_hash = generate_event_hash(
        None,
        payload,
        timestamp,
        sequence_id,
        tenant_id=tenant_id,
        aggregate_id=aggregate_id,
    )

    with pytest.raises(HashVerificationError):
        verify_chain(
            previous_hash=None,
            current_hash=event_hash,
            payload=payload,
            timestamp_ms=timestamp,
            sequence_id=sequence_id,
            tenant_id=tenant_id,
            aggregate_id=uuid4(),
        )


def test_generate_event_hash_is_sha256_hex64():
    event_hash = generate_event_hash(
        previous_hash=None,
        payload={"event_type": "ResourceAllocationRequested", "x": 1},
        timestamp_ms=1700000000000,
        sequence_id=1,
        tenant_id=uuid4(),
        aggregate_id=uuid4(),
    )

    assert len(event_hash) == 64
    assert re.fullmatch(r"[0-9a-f]{64}", event_hash)


def test_verify_chain_rejects_legacy_sha1_sized_hash():
    with pytest.raises(HashVerificationError, match="Invalid event_hash format"):
        verify_chain(
            previous_hash=None,
            current_hash="1f25751101b134e4f032b3de149a918c4bb4db2",
            payload={"event_type": "ResourceAllocationRequested", "x": 1},
            timestamp_ms=1700000000000,
            sequence_id=1,
            tenant_id=uuid4(),
            aggregate_id=uuid4(),
        )
