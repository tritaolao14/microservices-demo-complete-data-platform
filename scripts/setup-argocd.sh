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

# Set up the cluster for ArgoCD-managed deployments:
#   1. Create the target namespaces.
#   2. Create a docker-registry imagePullSecret (ghcr-secret) for pulling
#      private GHCR images, using the GHCR_PAT environment variable.
#   3. Apply the ArgoCD Applications (gitops/argocd).
#   4. Optionally sync the applications via the argocd CLI.
#
# Env:
#   GHCR_PAT  (required; PAT with read:packages scope)
#   OWNER     (GHCR account, default tritaolao14)
#   TARGET_NAMESPACES (space-separated, default dev+staging+prod)
#   SYNC      (set to 1 to run argocd app sync, default 0)

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${REPO_ROOT}"

OWNER="${OWNER:-tritaolao14}"
TARGET_NAMESPACES="${TARGET_NAMESPACES:-onlineboutique-dev onlineboutique-staging onlineboutique-prod}"

log() { echo "[setup-argocd] $*" >&2; }

# --- Step 1: target namespaces ---
setup_namespaces() {
  for ns in ${TARGET_NAMESPACES}; do
    kubectl create namespace "${ns}" --dry-run=client -o yaml | kubectl apply -f - > /dev/null
    log "namespace ${ns} ready"
  done
}

# --- Step 2: imagePullSecret for private GHCR images ---
setup_pull_secret() {
  if [[ -z "${GHCR_PAT:-}" ]]; then
    log "ERROR: GHCR_PAT is not set (PAT with read:packages scope)"
    return 1
  fi
  for ns in ${TARGET_NAMESPACES}; do
    kubectl -n "${ns}" create secret docker-registry ghcr-secret \
      --dry-run=client \
      --docker-server=ghcr.io \
      --docker-username="${OWNER}" \
      --docker-password="${GHCR_PAT}" \
      -o yaml | kubectl apply -f - > /dev/null
    log "imagePullSecret ghcr-secret ready in ${ns}"
  done
}

# --- Step 3: ArgoCD Applications ---
apply_apps() {
  kubectl apply -k gitops/argocd
    log "ArgoCD Applications applied (dev, staging, production)"
}

# --- Step 4: sync (optional) ---
sync_apps() {
  if [[ "${SYNC:-0}" == "1" ]]; then
    for app in dev staging production; do
      log "syncing app ${app}"
      argocd app sync "${app}" --async
    done
  else
    log "SYNC=0, skipping argocd app sync (run manually: argocd app sync staging --async)"
  fi
}

main() {
  setup_namespaces
  setup_pull_secret
  apply_apps
  sync_apps
  log "DONE."
}

main
