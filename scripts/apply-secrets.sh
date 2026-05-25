#!/bin/bash
set -euo pipefail

ENV_FILE=".env.staging"

find_kubectl() {
  if command -v kubectl.exe >/dev/null 2>&1; then
    echo "kubectl.exe"
    return 0
  fi

  if command -v kubectl >/dev/null 2>&1; then
    echo "kubectl"
    return 0
  fi

  echo "Missing required command: kubectl" >&2
  exit 1
}

KUBECTL_BIN="${KUBECTL_BIN:-$(find_kubectl)}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy .env.staging.example and fill in rotated credentials." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required in ${ENV_FILE}." >&2
  exit 1
fi

if [[ "${DATABASE_URL}" == *"URL_ENCODED_PASSWORD"* || "${DATABASE_URL}" == *"<"* ]]; then
  echo "DATABASE_URL still contains placeholder values." >&2
  exit 1
fi

echo "Injecting secrets into cloudcommander-staging..."
"${KUBECTL_BIN}" create secret generic cloudcommander-secrets \
  --from-literal=DATABASE_URL="${DATABASE_URL}" \
  -n cloudcommander-staging \
  --dry-run=client -o yaml | "${KUBECTL_BIN}" apply -f -

echo "Secrets applied."
