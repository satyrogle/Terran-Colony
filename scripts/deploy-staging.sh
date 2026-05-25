#!/bin/bash
set -euo pipefail

find_command() {
  local preferred="$1"
  local windows_fallback="$2"

  if command -v "${windows_fallback}" >/dev/null 2>&1; then
    echo "${windows_fallback}"
    return 0
  fi

  if command -v "${preferred}" >/dev/null 2>&1; then
    echo "${preferred}"
    return 0
  fi

  echo "Missing required command: ${preferred}" >&2
  exit 1
}

DOCKER_BIN="${DOCKER_BIN:-$(find_command docker docker.exe)}"
KUBECTL_BIN="${KUBECTL_BIN:-$(find_command kubectl kubectl.exe)}"

SHA=$(git rev-parse --short HEAD)
API_IMAGE="jakeyy8/cloudcommander-api:${SHA}"
WORKER_IMAGE="jakeyy8/cloudcommander-worker:${SHA}"

echo "Building images for commit ${SHA}..."
"${DOCKER_BIN}" build -t "${API_IMAGE}" -f Dockerfile.api .
"${DOCKER_BIN}" build -t "${WORKER_IMAGE}" -f Dockerfile.worker .

echo "Pushing images..."
"${DOCKER_BIN}" push "${API_IMAGE}"
"${DOCKER_BIN}" push "${WORKER_IMAGE}"

echo "Patching manifests and deploying..."
sed -i "s|image: jakeyy8/cloudcommander-api:.*|image: ${API_IMAGE}|g" k8s/staging/api-deployment.yaml
sed -i "s|image: jakeyy8/cloudcommander-worker:.*|image: ${WORKER_IMAGE}|g" k8s/staging/worker-deployment.yaml
sed -i "s|image: jakeyy8/cloudcommander-api:.*|image: ${API_IMAGE}|g" k8s/staging/migration-job.yaml

"${KUBECTL_BIN}" apply -f k8s/staging/api-deployment.yaml
"${KUBECTL_BIN}" apply -f k8s/staging/worker-deployment.yaml

echo "Rollout initiated."
"${KUBECTL_BIN}" rollout status deployment/cloudcommander-api -n cloudcommander-staging
"${KUBECTL_BIN}" rollout status deployment/cloudcommander-worker -n cloudcommander-staging
