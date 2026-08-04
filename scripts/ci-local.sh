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
#   1. Run fast quality checks (go vet/test, python compile, node --check if available,
#      kustomize build).
#   2. Build all service images for the host platform into the local docker
#      daemon (no registry push needed; kind loads images directly).
#   3. Validate artifacts on an ephemeral kind cluster (smoke test + Kafka
#      order-event E2E).
#   4. Update image tags in gitops/overlays/{staging,production}.
#
# Env overrides:
#   TAG      (default short git SHA)
#   CLUSTER  (kind cluster name, default ci-local)

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${REPO_ROOT}"

TAG="${TAG:-$(git rev-parse --short HEAD)}"
CLUSTER="${CLUSTER:-ci-local}"

log() { echo "[ci-local] $*" >&2; }

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

  if command -v node > /dev/null; then
    log "quality checks: node --check"
    for d in src/currencyservice src/paymentservice; do
      (cd "$d" && find . -maxdepth 1 -name '*.js' -exec node --check {} \;)
    done
  else
    log "WARN: node not found, skipping node --check"
  fi

  log "quality checks: kustomize build"
  kubectl kustomize kubernetes-manifests > /dev/null
  kubectl kustomize kustomize > /dev/null
}

# --- Step 2: build images ---
build_images() {
  log "build images to local daemon with tag ${TAG} for host platform"
  case "$(uname -m)" in
    x86_64) PLATFORM="linux/amd64" ;;
    arm64|aarch64) PLATFORM="linux/arm64" ;;
    *) PLATFORM="linux/amd64" ;;
  esac
  skaffold build \
    --platform="${PLATFORM}" \
    --file-output=tags.json \
    --default-repo="" --tag="${TAG}"
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

  log "removing control-plane taint from kind node"
  kubectl --context "kind-${CLUSTER}" taint nodes "${CLUSTER}-control-plane" node-role.kubernetes.io/control-plane- || true

  log "loading images into kind cluster"
  IMAGE_TAGS=$(jq -r '.builds[].tag' tags.json)
  for IMAGE in $IMAGE_TAGS; do
    kind load docker-image --name "${CLUSTER}" "$IMAGE"
  done

  log "deploying to kind"
  skaffold deploy \
    --build-artifacts=tags.json \
    --kube-context "kind-${CLUSTER}" \
    --default-repo="" --tag="${TAG}"

  log "deploying loadgenerator to kind"
  skaffold deploy \
    --module=loadgenerator \
    --build-artifacts=tags.json \
    --kube-context "kind-${CLUSTER}" \
    --default-repo="" --tag="${TAG}"

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
  quality_checks
  build_images
  trap cleanup EXIT
  validate_kind
  update_gitops
  log "DONE. Images tagged ${TAG} in local daemon, gitops overlays updated."
}

main "$@"
