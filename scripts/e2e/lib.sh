#!/usr/bin/env bash
# Shared helpers for the minikube e2e scenarios (issue #79).
# Every kubectl/helm call is pinned to the dedicated profile: never run unpinned.
set -euo pipefail

PROFILE="${PROFILE:-kbo-e2e}"
NS_OP="${NS_OP:-kbo}"
NS_KAFKA="${NS_KAFKA:-kafka}"
RELEASE="${RELEASE:-kafka-backup-operator}"
IMAGE_REPO="${IMAGE_REPO:-kbo-e2e/operator}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_DIR="${E2E_DIR:-$ROOT/.e2e}"
EVID="${EVID:-$E2E_DIR/evidence}"
SCEN="${SCEN:-misc}"
CHART_FIX="$ROOT/deploy/helm/kafka-backup-operator"
CHART_OLD="$E2E_DIR/src-v1.2.2/deploy/helm/kafka-backup-operator"
BACKUP_DIR="/data/kafka-backup-storage/backups"
mkdir -p "$EVID/$SCEN"

k() { kubectl --context "$PROFILE" "$@"; }
h() { helm --kube-context "$PROFILE" "$@"; }
stop_watchers() {
  pkill -f "get lease ${RELEASE}-leader -w" 2>/dev/null || true
  pkill -f "get pods -l app.kubernetes.io/name=${RELEASE} -w" 2>/dev/null || true
  pkill -f "scripts/e2e/watch.py" 2>/dev/null || true
}
trap 'stop_watchers' EXIT
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$EVID/$SCEN/log.md" >&2; }
fail() { log "FAIL: $*"; exit 1; }
pass() { log "PASS: $*"; }

op_pods() { k -n "$NS_OP" get pods -l app.kubernetes.io/name="$RELEASE" -o json; }
op_pod_names() { op_pods | python3 "$ROOT/scripts/e2e/jsonq.py" pod-names; }
pod_times() { op_pods | python3 "$ROOT/scripts/e2e/jsonq.py" pod-times; }
lease_state() { k -n "$NS_OP" get lease "${RELEASE}-leader" -o jsonpath='{.spec.holderIdentity}{"\t"}{.spec.leaseTransitions}{"\t"}{.spec.acquireTime}{"\t"}{.spec.renewTime}{"\t"}{.metadata.resourceVersion}{"\n"}' 2>/dev/null || echo "<no lease>"; }
lease_holder() { k -n "$NS_OP" get lease "${RELEASE}-leader" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true; }
lease_transitions() { k -n "$NS_OP" get lease "${RELEASE}-leader" -o jsonpath='{.spec.leaseTransitions}' 2>/dev/null || echo 0; }
metrics_of() { k get --raw "/api/v1/namespaces/$NS_OP/pods/$1:8080/proxy/metrics"; }
readyz_of() { k get --raw "/api/v1/namespaces/$NS_OP/pods/$1:8080/proxy/readyz" 2>&1 || true; }
leader_pods() { for p in $(op_pod_names); do metrics_of "$p" 2>/dev/null | grep -Eq 'kafka_backup_operator_leader\{[^}]*\} 1$' && echo "$p"; done; true; }
strategy_type() { k -n "$NS_OP" get deploy "$RELEASE" -o jsonpath='{.spec.strategy.type}'; }
op_logs() { k -n "$NS_OP" logs "$1" --timestamps 2>/dev/null || true; }
save_logs() { for p in $(op_pod_names); do op_logs "$p" > "$EVID/$SCEN/$1-$p.log" 2>/dev/null || true; done; }
backup_status() { k -n "$NS_OP" get kafkabackup sched -o jsonpath='{.status.phase}{"\t"}{.status.lastScheduleTime}{"\t"}{.status.lastBackupId}{"\n"}' 2>/dev/null || true; }
# Backup runs as seen on the PVC: one directory per backup_id (<name>-YYYYmmdd-HHMMSS).
backup_ids() { local p; p=$(op_pod_names | head -1); [ -n "$p" ] && k -n "$NS_OP" exec "$p" -- sh -c "ls $BACKUP_DIR 2>/dev/null" 2>/dev/null | grep -E '^sched-[0-9]{8}-[0-9]{6}$' | sort || true; }
# "Starting backup execution" lines with pod name and timestamp, across all current pods (and their previous containers).
backup_starts() { for p in $(op_pod_names); do for prev in "" "--previous"; do k -n "$NS_OP" logs "$p" $prev --timestamps 2>/dev/null | grep -F '"Starting backup execution"' | sed -E "s/^(\S+) .*/\1\t$p/"; done; done | sort; }

# wait_for <budget-seconds> <command...>
wait_for() { local budget=$1; shift; local t0=$(date +%s); until "$@" >/dev/null 2>&1; do if (( $(date +%s) - t0 > budget )); then return 1; fi; sleep 0.5; done; echo "ok after $(( $(date +%s) - t0 ))s"; }
leader_count_is() { [ "$(leader_pods | wc -l | tr -d ' ')" = "$1" ]; }
restarts_ge() { local n; n=$(pod_times | grep "^$1" | grep -o 'restarts=[0-9]*' | cut -d= -f2); [ -n "$n" ] && [ "$n" -ge "$2" ]; }
ready_replicas_is() { [ "$(k -n "$NS_OP" get deploy "$RELEASE" -o jsonpath='{.status.readyReplicas}')" = "$1" ]; }
holder_is() { [ "$(lease_holder)" = "$1" ]; }
has_holder() { [ -n "$(lease_holder)" ]; }
readyz_is() { [ "$(readyz_of "$1")" = "$2" ]; }
readyz_unavailable() { readyz_of "$1" | grep -Eq "leader election pending|ServiceUnavailable|503"; }
phase_is() { [ "$(backup_status | cut -f1)" = "$1" ]; }

# Signal the operator process of a pod from the node: inside the container it
# is PID 1, which cannot be stopped/killed from its own PID namespace.
signal_operator() { # <pod> <SIG>
  local cid; cid=$(k -n "$NS_OP" get pod "$1" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's#^docker://##')
  [ -n "$cid" ] || return 1
  minikube -p "$PROFILE" ssh -- "sudo kill -$2 \$(docker inspect -f '{{.State.Pid}}' $cid)" >/dev/null 2>&1
}

watch_pods_bg() { k -n "$NS_OP" get pods -l app.kubernetes.io/name="$RELEASE" -w --output-watch-events -o json 2>/dev/null | python3 -u "$ROOT/scripts/e2e/watch.py" pods > "$1" 2>/dev/null & echo $!; }
watch_lease_bg() { k -n "$NS_OP" get lease "${RELEASE}-leader" -w --output-watch-events -o json 2>/dev/null | python3 -u "$ROOT/scripts/e2e/watch.py" lease > "$1" 2>/dev/null & echo $!; }

evidence() { { echo; echo "## $1 ($(date -u +%FT%TZ))"; echo "status: $(backup_status)"; echo "pods:"; pod_times | sed 's/^/  /'; echo "lease: $(lease_state)"; echo "backup ids on PVC: $(backup_ids | wc -l | tr -d ' ')"; } | tee -a "$EVID/$SCEN/log.md"; }

# operator_install <chart-dir> <image-tag> [extra helm args...]
operator_install() { local chart=$1 tag=$2; shift 2
  h upgrade --install "$RELEASE" "$chart" -n "$NS_OP" --create-namespace \
    --set image.repository="$IMAGE_REPO" --set image.tag="$tag" --set image.pullPolicy=IfNotPresent \
    --set logging.level="info,kafka_backup_operator=debug" \
    --set 'extraVolumes[0].name=backups' --set 'extraVolumes[0].persistentVolumeClaim.claimName=kafka-backup-storage' \
    --set 'extraVolumeMounts[0].name=backups' --set 'extraVolumeMounts[0].mountPath=/data/kafka-backup-storage' \
    "$@" --wait --timeout 3m >/dev/null
  log "installed $RELEASE chart=$(basename "$(dirname "$chart")")/$(basename "$chart") tag=$tag args=[$*] strategy=$(strategy_type)"; }
operator_uninstall() { h uninstall "$RELEASE" -n "$NS_OP" --ignore-not-found --wait >/dev/null 2>&1 || true; k -n "$NS_OP" delete lease "${RELEASE}-leader" --ignore-not-found >/dev/null 2>&1 || true; }
apply_cr() { k apply -f "$ROOT/manifests/e2e/kafkabackup-sched.yaml" >/dev/null; }
delete_cr() { k -n "$NS_OP" delete kafkabackup sched --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
touch_cr() { k -n "$NS_OP" annotate kafkabackup sched e2e/touch="$(date +%s%N)" --overwrite >/dev/null; }
# Tick bucket (10s) of a backup id or an ISO timestamp, for duplicate detection.
tick_of() { python3 - "$1" <<'PY'
import sys,re,datetime
s=sys.argv[1]
m=re.search(r'(\d{8})-(\d{6})$', s)
if m:
    t=datetime.datetime.strptime(m.group(1)+m.group(2), "%Y%m%d%H%M%S")
else:
    t=datetime.datetime.fromisoformat(s.replace("Z","+00:00")).replace(tzinfo=None)
print(t.strftime("%Y%m%d-%H%M") + str(t.second//10))
PY
}
