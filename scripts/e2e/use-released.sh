#!/usr/bin/env bash
# Swap the "fix" e2e image for the published ghcr.io release so the scenarios
# run against the real artifact (the release is multi-arch: amd64 + arm64).
# Usage: use-released.sh <version, e.g. 1.2.3>
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
VER=${1:?version}
SRC="ghcr.io/osodevops/kafka-backup-operator:$VER"
docker pull -q "$SRC" >/dev/null
docker tag "$SRC" "$IMAGE_REPO:fix"
minikube -p "$PROFILE" image rm "$IMAGE_REPO:fix" >/dev/null 2>&1 || true
minikube -p "$PROFILE" image load "$IMAGE_REPO:fix"
log "$IMAGE_REPO:fix now = $SRC ($(docker image inspect "$SRC" --format '{{.Architecture}}, created {{.Created}}'))"
