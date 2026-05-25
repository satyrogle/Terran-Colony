from app.security.abac import (
    ResourceContext,
    SubjectContext,
    SubjectRole,
    evaluate_policy,
)


def test_same_tenant_allowed():
    subject = SubjectContext("user-1", "tenant-A", SubjectRole.USER)
    resource = ResourceContext("agg-1", "tenant-A", "healthy")

    decision = evaluate_policy(subject, "mutate", resource)

    assert decision.allowed is True


def test_cross_tenant_mutation_rejected():
    subject = SubjectContext("user-1", "tenant-A", SubjectRole.USER)
    resource = ResourceContext("agg-1", "tenant-B", "healthy")

    decision = evaluate_policy(subject, "mutate", resource)

    assert decision.allowed is False
    assert decision.status_code == 403


def test_saga_locked_aggregate_blocks_user_mutation():
    subject = SubjectContext("user-1", "tenant-A", SubjectRole.USER)
    resource = ResourceContext("agg-1", "tenant-A", "compensating_via_full_revert")

    decision = evaluate_policy(subject, "mutate", resource)

    assert decision.allowed is False
    assert decision.status_code == 423
    assert "locked for state compensation" in decision.reason


def test_system_subject_can_process_compensation():
    subject = SubjectContext("worker-1", "system-tenant", SubjectRole.SYSTEM)
    resource = ResourceContext("agg-1", "tenant-A", "compensating_via_full_revert")

    decision = evaluate_policy(subject, "mutate", resource)

    assert decision.allowed is True


def test_system_subject_bypasses_tenant_check_for_reconciliation():
    subject = SubjectContext("worker-1", "system-tenant", SubjectRole.SYSTEM)
    resource = ResourceContext("agg-1", "tenant-B", "healthy")

    decision = evaluate_policy(subject, "mutate", resource)

    assert decision.allowed is True
