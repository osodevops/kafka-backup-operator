#!/usr/bin/env bash
# Run every scenario on the fixed build (scenario 1 only with BASELINE=1).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
[ -n "${BASELINE:-}" ] && bash scenario-01-baseline.sh
for s in 02 03 04 05 06 07 08; do bash scenario-$s-*.sh; done
echo "all scenarios passed"
