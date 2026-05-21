from __future__ import annotations
import hashlib, json
from typing import Any, Dict, Optional
class HashVerificationError(Exception):
    pass

GENESIS_HASH = "0" * 64


def generate_event_hash(
    previous_hash: Optional[str], payload: Dict[str, Any], timestamp_ms: int, sequence_id: int
) -> str:
    hash_input = previous_hash or GENESIS_HASH
    payload_str=json.dumps(payload,sort_keys=True,separators=(',',':'))
    raw=f"{hash_input}|{payload_str}|{timestamp_ms}|{sequence_id}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

def verify_chain(
    previous_hash: Optional[str],
    current_hash: str,
    payload: Dict[str, Any],
    timestamp_ms: int,
    sequence_id: int,
) -> None:
    expected=generate_event_hash(previous_hash,payload,timestamp_ms,sequence_id)
    if current_hash!=expected:
        raise HashVerificationError(f"Chain broken at sequence {sequence_id}. Expected {expected}, got {current_hash}.")
