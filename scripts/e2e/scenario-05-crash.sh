#!/usr/bin/env bash
# Scenario 5 — leader frozen (SIGSTOP): standby takes over after leaseDuration;
# on SIGCONT the ex-leader sees the new holder and exits.
export SCEN=05-crash; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
operator_uninstall; delete_cr
operator_install "$CHART_FIX" fix --set replicaCount=2; apply_cr
wait_for 60 leader_count_is 1 >/dev/null || fail "no single leader"
leader=$(lease_holder); other=$(op_pod_names | grep -v "^$leader$" | head -1); tr1=$(lease_transitions); r1=$(pod_times | grep "^$leader" | grep -o 'restarts=[0-9]*' | cut -d= -f2)
LEASELOG=$(watch_lease_bg "$EVID/$SCEN/lease.jsonl"); sleep 1
signal_operator "$leader" STOP || fail "could not signal $leader"; t_stop=$(date +%s)
sleep 1; last_renew=$(lease_state | cut -f4)
sleep 4; [ "$(lease_state | cut -f4)" = "$last_renew" ] || fail "renewTime kept moving after SIGSTOP"
holder_changed() { local h; h=$(lease_holder); [ -n "$h" ] && [ "$h" != "$leader" ]; }
wait_for 40 holder_changed >/dev/null || fail "no takeover within 40s"
took=$(( $(date +%s) - t_stop )); sleep 2; kill "$LEASELOG" 2>/dev/null || true
[ "$took" -ge 13 ] && [ "$took" -le 22 ] || fail "takeover after ${took}s, expected ~leaseDuration"
[ "$(lease_holder)" = "$other" ] || fail "expected $other to take over"
[ "$(lease_transitions)" = "$((tr1+1))" ] || fail "leaseTransitions $tr1 -> $(lease_transitions)"
evidence "SIGSTOP: takeover by $other after ${took}s"
pass "expiry takeover after ${took}s"
signal_operator "$leader" CONT; t_cont=$(date +%s)
wait_for 40 restarts_ge "$leader" "$((r1+1))" >/dev/null || fail "resumed ex-leader did not exit"
k -n "$NS_OP" logs "$leader" --previous 2>/dev/null > "$EVID/$SCEN/ex-leader-previous.log" || true
grep -q "leadership lost" "$EVID/$SCEN/ex-leader-previous.log" || fail "previous log lacks 'leadership lost'"
[ "$(lease_holder)" = "$other" ] || fail "holder changed after the ex-leader resumed"
wait_for 30 readyz_is "$leader" standby >/dev/null || fail "restarted ex-leader readyz: $(readyz_of "$leader")"
evidence "SIGCONT: ex-leader exited $(( $(date +%s) - t_cont ))s after resuming, now standby"
pass "lost leadership -> exit -> standby"
