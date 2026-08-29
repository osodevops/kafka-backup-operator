#!/usr/bin/env bash
# Scenario 7 — a long-running backup is stopped gracefully on SIGTERM (engine
# finalizes: manifest + offsets) and the lease is released only afterwards.
export SCEN=07-graceful-stop; source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
operator_uninstall; delete_cr
# A long backup on its own topic: ~1.5M perf-test records (~300 MB) compressed
# with zstd level 19 by a single worker keeps one run going for minutes.
# (The CR's rateLimiting.recordsPerSec is not mapped to the backup engine — #81.)
k apply -f "$ROOT/manifests/e2e/kafka-big-topic.yaml" >/dev/null
k -n "$NS_KAFKA" wait kafkatopic/backup-test-big --for=condition=Ready --timeout=3m >/dev/null
k -n "$NS_KAFKA" get job seed-backup-test-big >/dev/null 2>&1 || sed -e 's/name: seed-records/name: seed-backup-test-big/' -e 's/value: "50000"/value: "1500000"/' -e 's/--topic backup-test-topic/--topic backup-test-big/' "$ROOT/manifests/e2e/producer-job.yaml" | k apply -f - >/dev/null
k -n "$NS_KAFKA" wait job/seed-backup-test-big --for=condition=complete --timeout=15m >/dev/null
operator_install "$CHART_FIX" fix
sed -e 's/schedule: .*/schedule: "0 *\/2 * * * * *"/' -e 's/compressionLevel: 3/compressionLevel: 19/' -e 's/maxConcurrentPartitions: 2/maxConcurrentPartitions: 1/' -e 's/- backup-test-topic/- backup-test-big/' "$ROOT/manifests/e2e/kafkabackup-sched.yaml" | k apply -f - >/dev/null
wait_for 150 phase_is Running >/dev/null || fail "backup did not start ($(backup_status))"
sleep 15; phase_is Running || fail "backup finished too quickly to test a graceful stop ($(backup_status))"
leader=$(lease_holder); LEASELOG=$(watch_lease_bg "$EVID/$SCEN/lease.jsonl"); sleep 1
k -n "$NS_OP" logs -f "$leader" > "$EVID/$SCEN/leader.log" 2>/dev/null & LOGF=$!; sleep 1
t0=$(date +%s); k -n "$NS_OP" delete pod "$leader" --wait=false >/dev/null
k -n "$NS_OP" wait pod "$leader" --for=delete --timeout=90s >/dev/null 2>&1 || true
gone=$(( $(date +%s) - t0 )); sleep 3; kill "$LEASELOG" "$LOGF" 2>/dev/null || true
grep -q "stopping the running backup engine" "$EVID/$SCEN/leader.log" || fail "engine was not asked to stop"
grep -q "Shutdown signal received" "$EVID/$SCEN/leader.log" || fail "engine did not observe the shutdown"
grep -q "releasing the leader lease" "$EVID/$SCEN/leader.log" || fail "lease was not released after the drain"
summary=$(python3 "$ROOT/scripts/e2e/analyze.py" lease "$EVID/$SCEN/lease.jsonl"); log "lease: $summary"
[ "$gone" -le 60 ] || fail "pod took ${gone}s to stop (> terminationGracePeriodSeconds)"
python3 - "$EVID/$SCEN/leader.log" <<'PY' || fail "lease released before the engine finalized"
import sys,re,json
stop=rel=None
for l in open(sys.argv[1], errors="replace"):
    try: d=json.loads(l.split(" ",1)[1] if l[:4].isdigit() else l)
    except Exception: continue
    m=d.get("fields",{}).get("message","")
    if "Shutdown signal received" in m or "finalizing" in m: stop=stop or d["timestamp"]
    if "releasing the leader lease" in m: rel=d["timestamp"]
sys.exit(0 if stop and rel and stop <= rel else 1)
PY
wait_for 120 has_holder >/dev/null || fail "no successor"
evidence "graceful stop: pod gone after ${gone}s, engine finalized before release, successor $(lease_holder)"
pass "graceful stop of a running backup"
