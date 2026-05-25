from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SubjectRole(Enum):
    USER = "user"
    SYSTEM = "system"


@dataclass(frozen=True)
class SubjectContext:
    subject_id: str
    tenant_id: str
    role: SubjectRole


@dataclass(frozen=True)
class ResourceContext:
    aggregate_id: str
    tenant_id: str
    reconcile_status: str = "not_applicable"


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: Optional[str] = None
    status_code: int = 403


def evaluate_policy(
    subject: SubjectContext, action: str, resource: ResourceContext
) -> PolicyDecision:
    """Evaluate ingress ABAC rules before domain reducers execute."""
    _ = action

    if subject.role != SubjectRole.SYSTEM and subject.tenant_id != resource.tenant_id:
        return PolicyDecision(
            allowed=False,
            reason="Tenant mismatch. Subject cannot access this resource.",
            status_code=403,
        )

    if resource.reconcile_status.startswith("compensating_via_"):
        if subject.role == SubjectRole.USER:
            return PolicyDecision(
                allowed=False,
                reason=(
                    "Resource is locked for state compensation: "
                    f"{resource.reconcile_status}"
                ),
                status_code=423,
            )

    return PolicyDecision(allowed=True)
