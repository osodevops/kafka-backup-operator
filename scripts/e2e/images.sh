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
  local build; build="$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$src" rev-parse --short HEAD 2>/dev/null || echo wt)"
  docker build -q --label "e2e.build=$build" -t "$IMAGE_REPO:$tag" "$work" >/dev/null
  # A same-tag `minikube image load` does not replace an image already on the
  # node: untag it there first, then load, then prove the node has this build
  # (image IDs are re-created on load, so compare the build label instead).
  minikube -p "$PROFILE" ssh -- "docker rmi -f $IMAGE_REPO:$tag" >/dev/null 2>&1 || true
  minikube -p "$PROFILE" image load "$IMAGE_REPO:$tag"
  local have; have=$(minikube -p "$PROFILE" ssh -- "docker image inspect -f '{{index .Config.Labels \"e2e.build\"}}' $IMAGE_REPO:$tag" 2>/dev/null | tr -d '\r')
  [ "$have" = "$build" ] || fail "node has build '$have' for $IMAGE_REPO:$tag, expected '$build'"
  log "built+loaded $IMAGE_REPO:$tag (build $build)"
}
if [ ! -d "$E2E_DIR/src-v1.2.2" ]; then git -C "$ROOT" worktree add "$E2E_DIR/src-v1.2.2" v1.2.2 >/dev/null; fi
build "$E2E_DIR/src-v1.2.2" 1.2.2
build "$ROOT" fix
