#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Local CI pipeline: replicate the CI flow on the developer machine.
#
# Steps:
#   1. Ensure a local docker registry runs on 127.0.0.1:5000.
#   2. Run fast quality checks (go vet/test, python compile, node --check,
#      kustomize build).
#   3. Build all service images and push them to the local registry.
#   4. Validate artifacts on an ephemeral kind cluster (smoke test + Kafka
#      order-event E2E).
#   5. Update image tags in gitops/overlays/{staging,production}.
#
# Env overrides:
#   REGISTRY (default 127.0.0.1:5000)
#   TAG      (default short git SHA)
#   CLUSTER  (kind cluster name, default ci-local)

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${REPO_ROOT}"

REGISTRY="${REGISTRY:-127.0.0.1:5000}"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
CLUSTER="${CLUSTER:-ci-local}"

log() { echo "[ci-local] $*" >&2; }

# --- Step 0: ensure local registry ---
ensure_registry() {
  if ! docker ps --format '{{.Names}}' | grep -q '^registry$'; then
    if docker ps -a --format '{{.Names}}' | grep -q '^registry$'; then
      log "starting existing 'registry' container"
      docker start registry
    else
      log "starting new local registry container on ${REGISTRY}"
      docker run -d -p 5000:5000 --name registry registry:2
    fi
  fi
  log "local registry ready at ${REGISTRY}"
}

# --- Step 1: fast quality checks ---
quality_checks() {
  log "quality checks: go vet/test"
  for SERVICE in shippingservice productcatalogservice frontend checkoutservice; do
    (cd "src/${SERVICE}" && gofmt -l . && go vet -copylocks=false ./... && go test ./...)
  done

  log "quality checks: python compile"
  for d in src/emailservice src/recommendationservice src/loadgenerator src/data_processing; do
    (cd "$d" && python3 -m py_compile ./*.py)
  done

  log "quality checks: node --check"
  for d in src/currencyservice src/paymentservice; do
    (cd "$d" && find . -maxdepth 1 -name '*.js' -exec node --check {} \;)
  done

  log "quality checks: kustomize build"
  kubectl kustomize kubernetes-manifests > /dev/null
  kubectl kustomize kustomize > /dev/null
}

# --- Step 2: build + push images ---
build_and_push() {
  log "build + push images to ${REGISTRY} with tag ${TAG}"
  skaffold build --push \
    --file-output=tags.json \
    --default-repo="${REGISTRY}" --tag="${TAG}"
}

# --- Step 3: validate on ephemeral kind cluster ---
validate_kind() {
  if ! command -v kind > /dev/null; then
    log "WARN: kind not installed, skipping E2E validation"
    return 0
  fi
  log "creating kind cluster '${CLUSTER}'"
  if ! kubectl config get-contexts -o name | grep -q "kind-${CLUSTER}"; then
    kind create cluster --name "${CLUSTER}"
  fi

  log "deploying to kind"
  skaffold deploy \
    --build-artifacts=tags.json \
    --kube-context "kind-${CLUSTER}" \
    --default-repo="${REGISTRY}" --tag="${TAG}"

  log "waiting for deployments"
  kubectl --context "kind-${CLUSTER}" wait --for=condition=available --timeout=1200s \
    deployment/redis-cart deployment/adservice deployment/cartservice \
    deployment/checkoutservice deployment/currencyservice deployment/emailservice \
    deployment/frontend deployment/kafka deployment/loadgenerator deployment/paymentservice \
    deployment/productcatalogservice deployment/recommendationservice deployment/shippingservice

  log "creating Kafka topic 'orders'"
  kubectl --context "kind-${CLUSTER}" exec deploy/kafka -- \
    /usr/bin/kafka-topics --bootstrap-server localhost:9092 --create \
    --topic orders --partitions 1 --replication-factor 1 --if-not-exists || true

  log "smoke test (loadgenerator)"
  kubectl --context "kind-${CLUSTER}" delete pod -l app=loadgenerator || true
  REQUEST_COUNT="0"
  while [[ "$REQUEST_COUNT" -lt "50" ]]; do
    sleep 5
    REQUEST_COUNT=$(kubectl --context "kind-${CLUSTER}" logs -l app=loadgenerator | grep Aggregated | awk '{print $2}')
  done
  ERROR_COUNT=$(kubectl --context "kind-${CLUSTER}" logs -l app=loadgenerator | grep Aggregated | awk '{print $3}' | sed "s/[(][^)]*[)]//g")
  log "loadgenerator aggregated requests=$REQUEST_COUNT errors=$ERROR_COUNT"
  if [[ "${ERROR_COUNT}" -gt "0" ]]; then
    log "ERROR: loadgenerator reported errors"
    return 1
  fi

  log "E2E: verify Kafka topic 'orders'"
  kubectl --context "kind-${CLUSTER}" exec deploy/kafka -- \
    /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list | grep -q '^orders$'

  log "E2E: consume an order event"
  MSG=$(kubectl --context "kind-${CLUSTER}" exec deploy/kafka -- \
    /usr/bin/kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic orders \
    --max-messages 1 --timeout-ms 120000 2>/dev/null || true)
  echo "$MSG" | python3 -c '
import sys, json
raw = sys.stdin.read()
assert raw.strip(), "no order event received"
evt = json.loads(raw)
assert evt.get("order_id"), "missing order_id"
assert evt.get("items"), "missing items"
assert evt.get("timestamp"), "missing timestamp"
print("E2E_OK: order event schema valid, order_id=%s" % evt["order_id"])
'
}

# --- Step 4: update gitops overlays tags ---
update_gitops() {
  log "updating gitops/overlays tags to ${TAG}"
  for env in staging production; do
    sed -i.bak "s/PLACEHOLDER_TAG/${TAG}/g" "gitops/overlays/${env}/kustomization.yaml"
    rm -f "gitops/overlays/${env}/kustomization.yaml.bak"
    log "  gitops/overlays/${env} -> ${TAG}"
  done
}

cleanup() {
  if command -v kind > /dev/null; then
    log "deleting kind cluster '${CLUSTER}'"
    kind delete cluster --name "${CLUSTER}" > /dev/null 2>&1 || true
  fi
}

main() {
  ensure_registry
  quality_checks
  build_and_push
  trap cleanup EXIT
  validate_kind
  update_gitops
  log "DONE. Images at ${REGISTRY}, tag ${TAG}, gitops overlays updated."
}

main "$@"
