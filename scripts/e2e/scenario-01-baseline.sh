#!/usr/bin/env bash
# Scenario 1 — reproduce #79 on the unfixed v1.2.2 chart/binary: with two
# operator pods alive (RollingUpdate + preStop sleep keeps the old pod up after
# the new one is Ready) a 10-second cron tick executes in BOTH pods.
export SCEN=01-baseline; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
N=${N:-3}
operator_uninstall; delete_cr
operator_install "$CHART_OLD" 1.2.2
[ "$(strategy_type)" = "RollingUpdate" ] || fail "expected RollingUpdate on the old chart"
k -n "$NS_OP" patch deploy "$RELEASE" --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/lifecycle","value":{"preStop":{"exec":{"command":["sh","-c","sleep 25"]}}}}]' >/dev/null
k -n "$NS_OP" rollout status deploy/"$RELEASE" --timeout=120s >/dev/null
apply_cr
wait_for 60 phase_is Completed >/dev/null || fail "first scheduled backup did not complete"
evidence "start (1.2.2, preStop sleep 25, schedule every 10s)"
dup_total=0
for i in $(seq 1 "$N"); do
  PODLOG=$(watch_pods_bg "$EVID/$SCEN/pods-$i.jsonl")
  before=$(backup_ids | wc -l | tr -d ' ')
  # Force a rollout without changing behaviour: bump a pod annotation.
  k -n "$NS_OP" patch deploy "$RELEASE" -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"e2e/roll\":\"$i-$(date +%s)\"}}}}}" >/dev/null
  # While both pods are alive, make them evaluate the schedule at the same
  # instant: a CR annotation change is delivered to both watches at once.
  ( for _ in $(seq 1 30); do touch_cr; sleep 1.5; done ) & TOUCH=$!
  k -n "$NS_OP" rollout status deploy/"$RELEASE" --timeout=180s >/dev/null
  wait $TOUCH || true
  sleep 15; kill "$PODLOG" 2>/dev/null || true
  save_logs "run$i"
  python3 "$ROOT/scripts/e2e/analyze.py" overlap "$EVID/$SCEN/pods-$i.jsonl" | tee -a "$EVID/$SCEN/log.md" | grep -q "overlap=yes" || log "note: pods did not overlap in run $i"
  # The race: the same backup_id (second-resolution) started by BOTH pods —
  # two engines writing the same directory, which is why the PVC shows no
  # extra entry. Detect it from the pods' logs.
  summary=$(python3 "$ROOT/scripts/e2e/analyze.py" dupstarts "$EVID/$SCEN"/run$i-*.log); log "run $i: $summary"
  dups=$(echo "$summary" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["started_in_two_pods"]))')
  log "run $i: backups on PVC $before -> $(backup_ids | wc -l | tr -d ' '), backup ids executed by two pods: $dups"
  dup_total=$((dup_total + dups))
  evidence "after run $i"
done
log "baseline result: $dup_total backup id(s) executed by two pods at once across $N rollouts"
[ "$dup_total" -ge 1 ] && pass "reproduced #79 ($dup_total backup(s) executed twice)" || log "WARN: could not reproduce the duplicate execution in $N rollouts"
