#!/usr/bin/env bash
# Build the operator images the scenarios need (native arch, from source) and
# load them into the profile. Skips tags already present unless FORCE=1.
#   1.2.2   released v1.2.2 tag (baseline, unfixed)
#   fix     working tree
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
mkdir -p "$E2E_DIR"
have() { minikube -p "$PROFILE" image ls 2>/dev/null | grep -q "docker.io/${IMAGE_REPO}:$1\$"; }
build() { # <src-dir> <tag>
  local src=$1 tag=$2
  if [ -n "${ONLY:-}" ] && [ "$tag" != "$ONLY" ]; then return; fi
  if [ -z "${FORCE:-}" ] && have "$tag"; then log "image $tag already loaded"; return; fi
  local work="$E2E_DIR/build-$tag"; rm -rf "$work"; mkdir -p "$work"
  rsync -a --exclude target --exclude .e2e --exclude .git "$src/" "$work/"
  docker build -q -t "$IMAGE_REPO:$tag" "$work" >/dev/null
  minikube -p "$PROFILE" image load "$IMAGE_REPO:$tag"
  log "built+loaded $IMAGE_REPO:$tag ($(docker image inspect "$IMAGE_REPO:$tag" --format '{{.Created}}'))"
}
if [ ! -d "$E2E_DIR/src-v1.2.2" ]; then git -C "$ROOT" worktree add "$E2E_DIR/src-v1.2.2" v1.2.2 >/dev/null; fi
build "$E2E_DIR/src-v1.2.2" 1.2.2
build "$ROOT" fix
