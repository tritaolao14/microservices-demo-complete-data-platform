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

# Build all service images and push them to GitHub Container Registry (GHCR).
#
# The ArgoCD-managed overlays in gitops/overlays/{staging,production} reference
# images at ghcr.io/<OWNER>/<service>, so the images must exist in GHCR before
# ArgoCD can sync. GHCR packages are private by default; authenticate first with:
#
#   docker login ghcr.io -u <OWNER>
#
# Env overrides:
#   OWNER    (GHCR account, default tritaolao14)
#   TAG      (image tag, default demo)
#   PLATFORM (override the host platform, e.g. linux/amd64)

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${REPO_ROOT}"

OWNER="${OWNER:-tritaolao14}"
TAG="${TAG:-demo}"

log() { echo "[push-ghcr] $*" >&2; }

# --- build and push images to GHCR ---
push_images() {
  local PLATFORM
  PLATFORM="${PLATFORM:-$(uname -m)}"
  case "${PLATFORM}" in
    x86_64) PLATFORM="linux/amd64" ;;
    arm64|aarch64) PLATFORM="linux/arm64" ;;
  esac

  log "building images for ${PLATFORM} and pushing to ghcr.io/${OWNER}:${TAG}"
  skaffold build \
    --platform="${PLATFORM}" \
    --file-output=tags.json \
    --default-repo="ghcr.io/${OWNER}" \
    --tag="${TAG}" \
    --push

  log "pushed ghcr.io/${OWNER}/<service>:${TAG}"
  log "note: set package visibility to 'private' if GHCR created them as public"
}

push_images
