#!/usr/bin/env bash
# Scenario 3 — surge rollout (maxSurge 1 / maxUnavailable 0) + leader election:
# the incoming pod stands by (Ready) and only executes once the outgoing leader
# has released the lease; no tick runs twice.
export SCEN=03-surge-le; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
N=${N:-2}
SURGE=(--set updateStrategy.rollingUpdate.maxSurge=1 --set updateStrategy.rollingUpdate.maxUnavailable=0)
dup_ticks() { backup_ids | while read -r id; do tick_of "$id"; done | sort | uniq -d | wc -l | tr -d ' '; }
dup_starts() { python3 "$ROOT/scripts/e2e/analyze.py" dupstarts "$EVID/$SCEN"/$1-*.log | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["started_in_two_pods"]))'; }
operator_uninstall; delete_cr
operator_install "$CHART_FIX" fix "${SURGE[@]}"; apply_cr
wait_for 60 phase_is Completed >/dev/null || fail "precondition"
wait_for 20 has_holder >/dev/null || fail "no lease holder"
for i in $(seq 1 "$N"); do
  old_pod=$(lease_holder); tr_before=$(lease_transitions); dups0=$(dup_ticks)
  PODLOG=$(watch_pods_bg "$EVID/$SCEN/pods-$i.jsonl"); LEASELOG=$(watch_lease_bg "$EVID/$SCEN/lease-$i.jsonl")
  operator_install "$CHART_FIX" fix "${SURGE[@]}" --set podAnnotations.e2e/roll="$i-$(date +%s)"
  sleep 25; kill "$PODLOG" "$LEASELOG" 2>/dev/null || true
  new_pod=$(lease_holder); [ -n "$new_pod" ] && [ "$new_pod" != "$old_pod" ] || fail "run $i: holder did not change"
  summary=$(python3 "$ROOT/scripts/e2e/analyze.py" lease "$EVID/$SCEN/lease-$i.jsonl"); log "run $i lease: $summary"
  changes=$(echo "$summary" | python3 -c 'import sys,json; print(json.load(sys.stdin)["non_empty_holder_changes"])')
  handover=$(echo "$summary" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("handover_s"))')
  acquired=$(echo "$summary" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("acquired_at"))')
  [ "$changes" = 1 ] || fail "run $i: expected exactly one holder change, got $changes"
  [ "$(lease_transitions)" = "$((tr_before+1))" ] || fail "run $i: leaseTransitions $tr_before -> $(lease_transitions)"
  python3 -c "import sys; sys.exit(0 if float('$handover') <= 5 else 1)" || fail "run $i: handover took ${handover}s"
  [ "$(python3 "$ROOT/scripts/e2e/analyze.py" ready_before "$EVID/$SCEN/pods-$i.jsonl" "$new_pod" "$acquired")" = yes ] || fail "run $i: new pod was not Ready (standby) before acquiring"
  save_logs "run$i"
  [ "$(dup_ticks)" = "$dups0" ] || fail "run $i: a tick executed twice"
  [ "$(dup_starts "run$i")" = 0 ] || fail "run $i: a backup id was started by two pods"
  python3 "$ROOT/scripts/e2e/analyze.py" overlap "$EVID/$SCEN/pods-$i.jsonl" | tee -a "$EVID/$SCEN/log.md" | grep -q "overlap=yes" || log "note: no pod overlap in run $i"
  evidence "run $i: holder $old_pod -> $new_pod, handover ${handover}s, no duplicate ticks"
  pass "run $i"
done
