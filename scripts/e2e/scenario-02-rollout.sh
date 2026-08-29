#!/usr/bin/env bash
# Scenario 2 — fixed chart: rollouts (real 1.2.2 -> fix, fixed -> fixed with
# default maxSurge 0, and Recreate on a fresh install) never execute a tick
# twice, and the lease moves exactly once per rollout.
export SCEN=02-rollout; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
N=${N:-3}
dup_ticks() { backup_ids | while read -r id; do tick_of "$id"; done | sort | uniq -d | wc -l | tr -d ' '; }
dup_starts() { python3 "$ROOT/scripts/e2e/analyze.py" dupstarts "$EVID/$SCEN"/$1-*.log | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["started_in_two_pods"]))'; }
resumed_after() { local t; t=$(backup_status | cut -f2); [ -n "$t" ] && [ "$(python3 -c "import sys,datetime as d; p=lambda s: d.datetime.fromisoformat(s.replace('Z','+00:00')); print(int(p('$t') > p('$1')))")" = 1 ]; }
roll_and_check() { # <label> <overlap yes|no|any> <helm args...>
  local label=$1 overlap=$2; shift 2
  local dups0; dups0=$(dup_ticks); local t_roll; t_roll=$(date -u +%FT%TZ)
  PODLOG=$(watch_pods_bg "$EVID/$SCEN/pods-$label.jsonl"); LEASELOG=$(watch_lease_bg "$EVID/$SCEN/lease-$label.jsonl")
  local holder0; holder0=$(lease_holder)
  operator_install "$CHART_FIX" fix --set podAnnotations.e2e/roll="$label-$(date +%s)" "$@"
  sleep 25; kill "$PODLOG" "$LEASELOG" 2>/dev/null || true
  save_logs "$label"
  local ov; ov=$(python3 "$ROOT/scripts/e2e/analyze.py" overlap "$EVID/$SCEN/pods-$label.jsonl" | tee -a "$EVID/$SCEN/log.md" | grep -o "overlap=.*")
  [ "$overlap" = any ] || [ "$ov" = "overlap=$overlap" ] || fail "$label: $ov, expected overlap=$overlap"
  local dups1; dups1=$(dup_ticks); [ "$dups1" = "$dups0" ] || fail "$label: a tick executed twice ($dups0 -> $dups1 duplicate ticks)"
  [ "$(dup_starts "$label")" = 0 ] || fail "$label: a backup id was started by two pods"
  if [ -n "$holder0" ]; then
    local summary; summary=$(python3 "$ROOT/scripts/e2e/analyze.py" lease "$EVID/$SCEN/lease-$label.jsonl"); log "$label lease: $summary"
    [ "$(echo "$summary" | python3 -c 'import sys,json; print(json.load(sys.stdin)["non_empty_holder_changes"])')" = 1 ] || fail "$label: expected exactly one holder change"
  fi
  wait_for 60 resumed_after "$t_roll" >/dev/null || fail "$label: no backup scheduled after the rollout ($(backup_status))"
  evidence "$label: $ov, duplicate ticks $dups1, holder $holder0 -> $(lease_holder)"
  pass "$label"
}
operator_uninstall; delete_cr
operator_install "$CHART_OLD" 1.2.2; apply_cr
wait_for 60 phase_is Completed >/dev/null || fail "precondition"
roll_and_check real-upgrade any
[ "$(k -n "$NS_OP" get deploy "$RELEASE" -o jsonpath='{.spec.strategy.type}/{.spec.strategy.rollingUpdate.maxSurge}/{.spec.strategy.rollingUpdate.maxUnavailable}')" = "RollingUpdate/0/1" ] || fail "default strategy"
for i in $(seq 1 "$N"); do roll_and_check "fixed-$i" any; done
operator_uninstall
operator_install "$CHART_FIX" fix --set updateStrategy.type=Recreate
wait_for 60 phase_is Completed >/dev/null || fail "precondition (recreate)"
roll_and_check recreate-1 no --set updateStrategy.type=Recreate
roll_and_check recreate-2 no --set updateStrategy.type=Recreate
