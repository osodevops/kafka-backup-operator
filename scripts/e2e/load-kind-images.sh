#!/usr/bin/env bash
# Import only the node's native platform. Docker's containerd store also exports
# manifest-list/attestation descriptors which kind's --all-platforms import may
# try to resolve even though the corresponding content was never pulled.
set -euo pipefail

if (( $# < 2 )); then
  echo "Usage: $0 <kind-cluster-name> <image> [image...]" >&2
  exit 2
fi
cluster=$1
shift
archive_dir=$(mktemp -d)
trap 'rm -rf "$archive_dir"' EXIT

nodes=$(kind get nodes --name "$cluster")
[[ -n "$nodes" ]] || { echo "No nodes in kind cluster $cluster" >&2; exit 1; }
while IFS= read -r node; do
  case "$(docker exec "$node" uname -m)" in
    aarch64) arch=arm64 ;;
    x86_64) arch=amd64 ;;
    *) echo "Unsupported architecture on $node" >&2; exit 1 ;;
  esac
  archive="$archive_dir/$arch.tar"
  if [[ ! -f "$archive" ]]; then
    docker image save --platform "linux/$arch" -o "$archive" "$@"
    # Older containerd versions also try to unpack attestation descriptors,
    # even with --platform. Keep only runnable native-platform descriptors in
    # the import index; image manifests, configurations and layers are unchanged.
    python3 - "$archive" "$arch" <<'PY'
import json
import os
import sys
import tarfile
from io import BytesIO

archive, arch = sys.argv[1:]
with tarfile.open(archive) as source, tarfile.open(archive + ".native", "w") as target:
    for member in source:
        data = source.extractfile(member) if member.isfile() else None
        if member.name == "index.json":
            index = json.load(data)
            index["manifests"] = [
                item for item in index["manifests"]
                if item.get("platform", {}).get("os") == "linux"
                and item.get("platform", {}).get("architecture") == arch
            ]
            if not index["manifests"]:
                raise ValueError(f"No linux/{arch} image manifests in export")
            encoded = json.dumps(index).encode()
            member.size = len(encoded)
            data = BytesIO(encoded)
        target.addfile(member, data)
os.replace(archive + ".native", archive)
PY
  fi
  docker exec -i "$node" ctr --namespace=k8s.io images import \
    --platform "linux/$arch" --digests --snapshotter=overlayfs - < "$archive"
  for image in "$@"; do
    docker exec "$node" crictl inspecti "$image" > /dev/null
  done
done <<< "$nodes"
