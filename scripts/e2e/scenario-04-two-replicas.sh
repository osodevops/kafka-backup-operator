#!/usr/bin/env bash
# Scenario 4 — two replicas: one leader executes, the standby never does;
# graceful failover on pod delete.
export SCEN=04-two-replicas; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dup_ticks() { backup_ids | while read -r id; do tick_of "$id"; done | sort | uniq -d | wc -l | tr -d ' '; }
operator_uninstall; delete_cr
operator_install "$CHART_FIX" fix --set replicaCount=2; apply_cr
wait_for 30 ready_replicas_is 2 >/dev/null || fail "both replicas must become Ready"
wait_for 60 phase_is Completed >/dev/null || fail "no backup completed"
wait_for 30 leader_count_is 1 >/dev/null || fail "expected exactly one leader: $(leader_pods)"
leader=$(leader_pods); standby=$(op_pod_names | grep -v "^$leader$" | head -1)
[ "$(lease_holder)" = "$leader" ] || fail "lease holder $(lease_holder) != gauge leader $leader"
[ "$(readyz_of "$leader")" = leader ] && [ "$(readyz_of "$standby")" = standby ] || fail "readyz: $(readyz_of "$leader") / $(readyz_of "$standby")"
sleep 25
op_logs "$standby" | grep -q '"Starting backup execution"' && fail "standby executed a backup"
[ "$(op_logs "$leader" | grep -c '"Starting backup execution"')" -ge 2 ] || fail "leader did not execute scheduled backups"
[ "$(dup_ticks)" = 0 ] || fail "duplicate ticks with two replicas"
evidence "steady state: leader=$leader standby=$standby, $(backup_ids | wc -l | tr -d ' ') backups, 0 duplicate ticks"
LEASELOG=$(watch_lease_bg "$EVID/$SCEN/lease-failover.jsonl"); sleep 1
tr_before=$(lease_transitions); dups0=$(dup_ticks)
k -n "$NS_OP" delete pod "$leader" --wait=false >/dev/null
holder_changed() { local h; h=$(lease_holder); [ -n "$h" ] && [ "$h" != "$leader" ]; }
r=$(wait_for 20 holder_changed) || fail "no other replica acquired within 20s"
new_leader=$(lease_holder); sleep 25; kill "$LEASELOG" 2>/dev/null || true
summary=$(python3 "$ROOT/scripts/e2e/analyze.py" lease "$EVID/$SCEN/lease-failover.jsonl"); log "failover lease: $summary"
handover=$(echo "$summary" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("handover_s"))')
python3 -c "import sys; sys.exit(0 if float('$handover') <= 5 else 1)" || fail "graceful failover took ${handover}s"
[ "$(lease_transitions)" = "$((tr_before+1))" ] || fail "leaseTransitions $tr_before -> $(lease_transitions)"
[ "$(op_logs "$new_leader" | grep -c '"Starting backup execution"')" -ge 1 ] || fail "new leader did not execute"
[ "$(dup_ticks)" = "$dups0" ] || fail "a tick executed twice around the failover"
evidence "after failover: new leader $new_leader, handover ${handover}s, no duplicate ticks"
pass "two replicas + graceful failover (${handover}s)"
