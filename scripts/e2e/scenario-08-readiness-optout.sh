#!/usr/bin/env bash
# Scenario 8 — helm upgrade --wait completes for every topology; opt-out keeps
# the old behaviour (no lease, no LEADER_ELECTION env, RBAC unchanged).
export SCEN=08-readiness-optout; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
timed() { local t0=$(date +%s); "$@"; echo $(( $(date +%s) - t0 )); }
operator_uninstall; delete_cr
operator_install "$CHART_FIX" fix; apply_cr
t=$(timed operator_install "$CHART_FIX" fix --set podAnnotations.e2e/roll=a); log "default upgrade --wait: ${t}s"
t=$(timed operator_install "$CHART_FIX" fix --set replicaCount=2); log "replicas=2 upgrade --wait: ${t}s"
t=$(timed operator_install "$CHART_FIX" fix --set replicaCount=2 --set updateStrategy.rollingUpdate.maxSurge=1 --set updateStrategy.rollingUpdate.maxUnavailable=0); log "maxSurge 1, replicas=2 upgrade --wait: ${t}s"
for p in $(op_pod_names); do case "$(readyz_of "$p")" in leader|standby) ;; *) fail "$p readyz: $(readyz_of "$p")";; esac; done
pass "readiness / helm --wait"
operator_uninstall
operator_install "$CHART_FIX" fix --set leaderElection.enabled=false
k -n "$NS_OP" get lease "${RELEASE}-leader" >/dev/null 2>&1 && fail "lease must not exist when opted out"
k -n "$NS_OP" get deploy "$RELEASE" -o jsonpath='{.spec.template.spec.containers[0].env[*].name}' | grep -q LEADER_ELECTION && fail "LEADER_ELECTION env rendered when opted out"
helm template x "$CHART_OLD" | python3 -c 'import sys,yaml; docs=[d for d in yaml.safe_load_all(sys.stdin) if d and d.get("kind")=="ClusterRole"]; print(sorted(str(r) for r in docs[0]["rules"]))' > "$EVID/$SCEN/rules-old.txt"
helm template x "$CHART_FIX" --set leaderElection.enabled=false | python3 -c 'import sys,yaml; docs=[d for d in yaml.safe_load_all(sys.stdin) if d and d.get("kind")=="ClusterRole"]; print(sorted(str(r) for r in docs[0]["rules"]))' > "$EVID/$SCEN/rules-new.txt"
diff "$EVID/$SCEN/rules-old.txt" "$EVID/$SCEN/rules-new.txt" >/dev/null || fail "ClusterRole rules differ from 1.2.2 when opted out"
pod=$(op_pod_names | head -1)
for _ in $(seq 1 20); do op_logs "$pod" | grep -q 'Leader election disabled' && break; sleep 1; done
op_logs "$pod" | grep -q 'Leader election disabled' || fail "missing 'Leader election disabled' log"
[ "$(readyz_of "$pod")" = leader ] || fail "readyz opted out: $(readyz_of "$pod")"
wait_for 60 phase_is Completed >/dev/null || fail "no backup when opted out"
pass "opt-out compatibility"
