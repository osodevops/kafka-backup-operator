# Changelog

## 1.8.0 - 2026-11-10

Uses `kafka-backup-core` `v0.28.0` to prevent successful backups with unread data after empty Fetch responses (kafka-backup#256).

- Add `KafkaBackup.spec.emptyFetchRetrySecs` (default 30, range 0–3600; zero fails immediately) and shared `kafkaCluster.connection.fetchMaxWaitMs` (default 500, range 0–30000), with admission bounds and adapter coverage.
- Bound stalled Fetch retries across backoff and in-flight requests; preserve durable progress on failure and close interrupted connections.
- Upgrade the CRDs before the operator. Helm users must apply the CRD bundle explicitly because Helm does not upgrade installed CRDs.

## 1.7.1 - 2026-11-07

Uses `kafka-backup-core` `v0.27.3`. CRD schemas are unchanged.

### Fixed

- Restore logs now report payload bytes and throughput for both ordinary and repartitioned restores ([kafka-backup#251](https://github.com/osodevops/kafka-backup/issues/251)). The final summary uses restore counters and shows segments read instead of unmeasured backup compression and write latencies. Bytes mean restored key plus value lengths, excluding headers and protocol overhead; failed batches are excluded. This also fixes the performance collector on checkpoint-tail reconciliation.

## 1.7.0 - 2026-11-04

Uses `kafka-backup-core` `v0.27.0` (from `v0.26.1`: 0.26.2 keeps storage credentials out of `Debug` output and validation reports; 0.27.0 adds backup rate limits). The CRD changes only in field descriptions.

### Fixed

- `KafkaBackup.spec.rateLimiting.recordsPerSec` and `bytesPerSec` now limit backups ([#81](https://github.com/osodevops/kafka-backup-operator/issues/81)). They had no effect: core's backup engine had no rate limit, so only `maxConcurrentPartitions` reached it. Core 0.27.0 adds one token bucket per backup, shared by every partition, so each limit is the backup's total rate (bytes are counted as each record's encoded size, as for restores). It is an average: a fetch waits until the previous one fits the rate. `0` still means unlimited. Reproduced in minikube: 50 000 records with `recordsPerSec: 500` took about 3 s on 1.6.5 and about 100 s on 1.7.0.

### Upgrade note

- Backups that set `rateLimiting.recordsPerSec` or `bytesPerSec` were never throttled and now are. Check that the values you set are the ones you want before upgrading; set them to `0` (or remove them) for unlimited.

## 1.6.5 - 2026-11-04

Uses `kafka-backup-core` `v0.26.1`. CRD schemas are unchanged.

### Fixed

- A run interrupted by an operator restart (OOMKilled, evicted, rolled) is now failed with condition reason `Interrupted` instead of staying `Running` for ever ([#98](https://github.com/osodevops/kafka-backup-operator/issues/98)). Runs execute inside the operator process, so after a restart nothing runs them, but a `KafkaRestore` kept `phase: Running` / "Restore in progress" while the controller requeued it every 5 seconds, and nothing retried it or reported a failure. Reproduced in minikube on 1.6.3: SIGKILL during a restore, then `Running` for as long as we watched; with 1.6.5 the restore is `Failed` / `Interrupted` within seconds of the restart, and the records restored before the interruption are kept.
- The same applies to `KafkaBackup` without a schedule (one-shot or continuous), which was never looked at again once `Running`; `KafkaBackupValidation`, which was requeued every 30 seconds (a scheduled one now runs again at its next tick); `KafkaOffsetRollback`; and `KafkaOffsetReset`, which was failed only 30 minutes after it started. Scheduled backups already run again at their next tick.
- The operator tells its own runs from interrupted ones by recording every run it starts (kind, uid, generation): reconciles of one object are serialised, so a `Running` status this process wrote is only the cache catching up with its final status. An interrupted run is not resumed: restores are not checkpointed, so a resumed restore would produce its records again, and an interruption caused by the run itself (an OOM kill) would turn into a crash loop. To run it again, change the spec.

## 1.6.4 - 2026-11-04

Uses `kafka-backup-core` `v0.26.1`. CRD schemas are unchanged.

### Fixed

- A run killed mid-way (OOMKilled, SIGKILL) no longer breaks every later run of a resource with TLS client credentials ([#99](https://github.com/osodevops/kafka-backup-operator/issues/99)). The TLS files are removed when a run ends, but a killed process never gets there, and `/tmp` is an emptyDir that survives the container restart: the next run could not overwrite the leftover read-only `client.key` and failed with `Failed to create file ".../client.key": Permission denied` until the resource was renamed. Each file is now removed before it is written, and the operator clears `/tmp/kafka-backup-tls` when it starts, since no run is in progress then. Reproduced in minikube: SIGKILL during an mTLS backup (exit 137), then every new run with the same name failed on 1.6.3.
- Resources no longer share TLS files. The directory was named after the resource only, so a `KafkaBackup` and a `KafkaRestore` (or `KafkaOffsetReset`/`KafkaOffsetRollback`), or two namespaces, with the same name wrote and deleted each other's client key. It is now `/tmp/kafka-backup-tls/<kind>/<namespace>/<name>`.
- The client key is created owner read-only (0400) instead of being written world-readable and restricted afterwards, and the TLS directory is 0700.

## 1.6.3 - 2026-11-04

Uses `kafka-backup-core` `v0.26.1`. CRD schemas are unchanged.

### Fixed

- Scheduled `KafkaBackupValidation`s now run on their schedule. Once a validation had run for its current spec, the controller only requeued it until the next cron tick and never ran it again, so `schedule: "0 0 2 * * *"` ran once at creation and kept the same `lastValidationTime` from then on. A run is now due when a cron tick has passed since the last run started (`status.startTime`), after `Completed` and after `Failed` alike. A process-local guard stops a reconcile that reads a stale cache from running the same tick twice, as the `KafkaBackup` scheduler does. Reproduced in minikube with a 15-second schedule: on 1.6.2, 5 schedule wakeups and 1 run; on 1.6.3, one run per tick with its own evidence report and no duplicate runs.
- `status.nextScheduledValidation` is set after each run of a scheduled validation (the field existed but was never written).
- Setting `suspend: true` no longer runs the validation one last time. A spec change runs a validation at once, and suspending is a spec change; a suspended validation now records the new generation without running.

## 1.6.2 - 2026-11-04

Uses `kafka-backup-core` `v0.26.1`. The `KafkaBackupValidation` CRD gains status fields: apply the updated CRDs before upgrading (`kubectl apply --server-side -f deploy/crds/all.yaml`; Helm does not upgrade CRDs). On the old CRD the API server prunes the new fields from status.

### Fixed

- `KafkaBackupValidation` now writes the evidence reports `spec.evidence` asks for ([#93](https://github.com/osodevops/kafka-backup-operator/issues/93)). The evidence step was a placeholder: a run reported `Completed` with no report in storage and no path in status, whatever `formats`, `signing` and `storage` said. Reports are built and stored through kafka-backup-core's evidence pipeline, the one the CLI uses, so the JSON, its SHA-256, the signature envelope and the PDF follow the same contract. They go to `spec.evidence.storage` (or the backup's storage when unset) under `evidence-reports/<reportId>/<yyyy>/<mm>/`, and the run fails with reason `EvidenceEmissionFailed` when they cannot be stored.
- Evidence signing passed the key's PEM content to core where core expects a file path, so signing could never have worked. The key from the Secret is now written to a private temporary file for the duration of signing. Core parses PKCS#8 keys only (`-----BEGIN PRIVATE KEY-----`); a SEC1 key (`-----BEGIN EC PRIVATE KEY-----`, as `openssl ecparam -genkey` writes) is rejected with the command that converts it.
- `spec.notifications` (Slack, PagerDuty) were resolved and never sent. They now receive each run's result and evidence location; a delivery failure is reported in the status message without failing the run.
- Validation rejects an empty `evidence.formats` and formats other than `json` and `pdf`, which were dropped silently.

### Added

- Status fields `evidenceReportId`, `evidencePdfPath`, `evidenceSignaturePath` and `evidenceReportSha256` (lowercase hex SHA-256 of the stored JSON bytes), next to `evidenceReportPath` (the JSON report) and `evidenceReportSigned`. Paths are keys relative to the evidence storage and its prefix.
- The report records the storage location without credentials (for example `azure://account/container/prefix`), names `kafka-backup-operator` and its version as the tool, and records `kafka-backup-operator:<namespace>/<name>` as the trigger.

## 1.6.1 - 2026-11-03

Uses `kafka-backup-core` `v0.26.1`. CRD schemas are unchanged.

### Fixed

- Restores over TLS no longer hang on 60-second response timeouts. A request write could return with its last TLS records still buffered when the socket was momentarily full, so the broker waited for the rest of the request and the operator for the response; core now flushes every request ([kafka-backup#242](https://github.com/osodevops/kafka-backup/issues/242)). Reproduced in minikube against Redpanda over TLS: on 1.6.0 a `KafkaRestore` failed after 14 such timeouts without restoring a record; on 1.6.1 it completed in 16 s.
- A `KafkaRestore` with offset recovery (`autoConsumerGroups` or `offsetReset.enabled`) no longer holds the whole offset mapping report in memory as text while writing it. The report has one entry per restored record, so a 60 M-record restore needed about 7 GB extra and was OOM-killed before consumer group offsets were applied ([kafka-backup#244](https://github.com/osodevops/kafka-backup/issues/244)). It is now streamed to disk as compact JSON and replaced atomically, so a failed write leaves no truncated report.
- `KafkaOffsetReset` with `resetStrategy: from-mapping` parses the report straight from the file instead of reading it into a string first, which needed the file's size again in memory. It reads both the compact reports written from 1.6.1 and the pretty-printed ones of earlier releases.

## 1.6.0 - 2026-11-03

Uses `kafka-backup-core` `v0.26.0`.

### Added

- `kafkaCluster.connection.produceApiVersion` (7 or 8, unset: 8) sets the Produce API version the operator sends. Only Produce requests follow it, so it matters for `KafkaRestore`; the other resources accept and ignore it. The client does not negotiate API versions yet, so set it for a target broker whose maximum is lower: Redpanda advertises Produce 0..7 and drops the connection on 8, and every topic of the restore then fails with `early eof` (the restore error now says so and names the setting). Versions below 7 are rejected at admission and by core: every batch the client produces is zstd-compressed, which brokers accept from Produce version 7.

## 1.5.0 - 2026-11-03

### Added

- `KafkaBackup.spec.excludeTopics` leaves topics out of a backup, by name or glob (`*`, `?`). With `topics: ["*"]` every topic created later is backed up unless it is excluded ([#95](https://github.com/osodevops/kafka-backup-operator/issues/95)). The CRD schema changes: apply the updated CRDs before upgrading (`kubectl apply --server-side -f deploy/crds/all.yaml`; Helm does not upgrade CRDs). On the old CRD the field is rejected by strict clients and silently dropped by lenient ones.
- Validation rejects an `excludeTopics` list with a blank entry, or one that cancels every entry of `topics` (for example `topics: ["*"]` with `excludeTopics: ["*"]`): such a backup completed "successfully" having archived nothing.
- A literal `topics` entry that `excludeTopics` also matches is left out of the selection handed to core. Before, a missing one failed the backup as "topic not found" (or was reported as missing under `onMissingTopic: warn`) although the exclusion asked for exactly that.

## 1.4.6 - 2026-11-03

### Fixed

- Scheduled backups with `checkpoint.enabled: true` (the documented
  incremental scheduled backup configuration) are now incremental. The
  operator generated a new `{name}-{YYYYMMDD-HHMMSS}` backup ID on every run,
  but the engine resumes from `{backupId}/offsets.db` and merges into
  `{backupId}/manifest.json`, so no run could find the previous run's
  checkpoint and every run was a full backup from the earliest offset. A
  non-continuous `KafkaBackup` with checkpointing enabled now writes every run
  into one backup set named after the resource and resumes from its stored
  checkpoint: `status.backupId` equals the resource name, the manifest lists
  the segments of all runs, and `status.resumable` (defined but never set
  before) reports the mode. Backups without a `checkpoint` block (or with
  `enabled: false`) and continuous backups keep a timestamped backup set per
  run, as before
  ([#94](https://github.com/osodevops/kafka-backup-operator/issues/94)).
- Retention always keeps the stable backup set (it is the current set of
  every run, so neither `maxAgeDays` nor `keepLast` prunes it); timestamped
  sets written by earlier full runs stay owned by the resource and age out
  under the policy.

## 1.4.5 - 2026-10-14

### Fixed

- Update the embedded core from 0.23.8 to 0.25.2. Backups now use read-committed isolation, excluding aborted transaction records and control markers. Durable checkpoints stay behind persisted data, and restores retry transient partition-leader readiness errors.
- Enable broker de-duplication for restores with the default `produceAcks: -1`; explicit acks 0/1 keep their existing behaviour. Azure authentication fixes and the current transport security patches are included. The Prometheus/Protobuf dependency and locked bytes version are updated to clear their vulnerability findings.

The operator remains the open-source engine. Enterprise Kafka OAuth and Schema Registry recovery run through the Enterprise image and chart, and are not operator CRD features.

## 1.4.4 - 2026-10-09

### Fixed

- A continuous `KafkaBackup` with `consumerGroupSnapshot: true` no longer
  wipes its consumer-group snapshot when the storage read fails. Each cycle
  checks whether a snapshot already exists before writing an empty one; a
  failed check (S3 403, network error) was read as "no snapshot" and the
  snapshot was replaced with an empty one. The (non-fatal) snapshot step now
  fails and the stored snapshot is kept
  ([kafka-backup#218](https://github.com/osodevops/kafka-backup/issues/218)).
- Backup manifest saves fail instead of overwriting `manifest.json` when the
  stored manifest can't be read (only a missing manifest is a first write),
  and retention reports a failed manifest read instead of skipping silently.
- Missing S3 objects are reported as "Object not found" rather than a
  generic backend error, and the restore preflight reports an unreadable
  consumer-groups snapshot as `unreadable` instead of `missing`.

### Changed

- `kafka-backup-core` 0.23.3 → 0.23.8.

## 1.4.3 - 2026-10-09

### Fixed

- `KafkaOffsetRollback` reports its verification in `status.verification`
  (`allMatched`, `totalGroups`, `matchedGroups`, `mismatchedGroups`). The
  reconciler wrote a `verified` field the CRD doesn't define, so the API
  server pruned it: the status looked the same whether verification passed,
  failed or was skipped (`verifyAfterRollback: false`). Found while checking
  [kafka-backup#220](https://github.com/osodevops/kafka-backup/issues/220).
- `KafkaOffsetRollback` goes to `Failed` (`Ready=False`, reason
  `VerificationFailed`) when verification finds groups whose offsets don't
  match the snapshot after the rollback, e.g. a consumer still committing. It
  used to go to `Completed`. The message lists the mismatched groups.
- `KafkaOffsetRollback` records `status.startTime` and
  `status.completionTime`. It wrote a `duration` field the CRD doesn't
  define, which was pruned the same way.
- With `verifyAfterRollback: false` the message says the rollback was not
  verified, and a verification left by an earlier run is cleared.

No CRD change: `status.verification`, `startTime` and `completionTime` were
already in the schema, so this works with CRDs installed by earlier charts.

## 1.4.2 - 2026-10-07

### Fixed

- `KafkaOffsetReset` and `KafkaOffsetRollback` work on multi-broker clusters
  where the bootstrap broker is not the consumer group's coordinator
  ([kafka-backup#224](https://github.com/osodevops/kafka-backup/issues/224)).
  Offsets were read from and committed to the bootstrap broker, which answers
  `NOT_COORDINATOR` for every other group, so:
  - `to-earliest` / `to-latest` / `to-timestamp` / `to-offset` resets saw no
    committed offsets and completed as `No-op: offsets already at target`
    without resetting anything;
  - `from-mapping` resets and rollbacks committed nothing, yet the reset
    reported the groups as reset and the rollback as `Completed`, with
    verification passing.
  Fixed in `kafka-backup-core` 0.23.3, which routes OffsetFetch and
  OffsetCommit to the group coordinator.
- `KafkaOffsetReset` fails a group when the broker rejects any partition of
  its offset commit, instead of reporting it as reset.
- `KafkaOffsetRollback` goes to `Failed` when any group fails to roll back,
  instead of `Completed`.
- `to-earliest` / `to-latest` / `to-timestamp` resets look offsets up on each
  partition's leader. They asked the bootstrap broker, which fails with
  `NOT_LEADER_OR_FOLLOWER` (error code 6) for partitions it does not lead.

### Changed

- `kafka-backup-core` 0.23.1 → 0.23.3.

## 1.4.1 - 2026-10-06

### Fixed

- Consumer offsets are reset on the restored topic when a `KafkaRestore` uses
  `topicMapping` with `offsetReset.consumerGroups`. The groups' committed
  offsets name the source topic, but the restore keys its offset mapping by the
  target topic, so Phase 3 skipped every partition (`No target offset mapping
  … - skipping`) and reset nothing. Fixed in `kafka-backup-core` 0.23.1
  ([kafka-backup#214](https://github.com/osodevops/kafka-backup/issues/214)).
- `KafkaOffsetReset` with `resetStrategy: from-mapping` and
  `offsetMappingRef.restoreName` translates the group's commits through that
  restore's `topicMapping` / `partitionMapping` the same way; it failed with
  `No target offsets found in mapping` before. Commits already on a restored
  topic and repartitioned topics are left unchanged, so a re-run is a no-op.
- `KafkaOffsetReset` with a `topics` list and `to-earliest` / `to-latest` /
  `to-offset` / `to-timestamp` reset nothing: the group's offsets were fetched
  for the named topics without partition indexes, which the broker answers
  with nothing, and the reset was reported as `No-op: offsets already at
  target`. The operator now fetches all of the group's committed offsets and
  filters them by topic.
- A `KafkaOffsetReset` that references a `KafkaRestore` which is missing or
  still running waits in `Pending` (requeued every 15s) instead of failing with
  `does not expose status.offsetMappingPath`; it fails with a clear message if
  the restore failed. Both resources can now be applied together.

### Changed

- `kafka-backup-core` 0.23.0 → 0.23.1.

### Documentation

- `KafkaRestore.spec.rollback` (`snapshotBeforeRestore`,
  `autoRollbackOnFailure`) is documented as not implemented yet — it was
  accepted and logged but never took a snapshot or rolled back; the README
  example no longer presents it as a safety net.
  `KafkaOffsetReset.spec.snapshotBeforeReset` is documented accurately: the
  snapshot is taken and its ID recorded in `status.snapshotId`, but it is not
  saved, so `KafkaOffsetRollback` cannot restore from it. CRD descriptions
  only, no schema change.
- README: `from-mapping` offset reset example, `headerPreflight` /
  `offsetReset` in the restore example, `onMissingTopic`, the
  `kafka_backup_missing_topics` metric. minikube README: the scripts act on the
  current kubectl context.

## 1.4.0 - 2026-10-06

Builds against `kafka-backup-core` 0.23.0
([#84](https://github.com/osodevops/kafka-backup-operator/issues/84)).

### Added

- `KafkaBackup.spec.onMissingTopic` (`fail` default, `warn`): skip literal
  topics absent from the cluster, record them in the manifest's
  `missing_topics` and the `kafka_backup_missing_topics` gauge.
- `KafkaRestore.spec.headerPreflight` (`auto` default, `full`, `skip`): the
  Phase 1 header preflight core 0.20 runs before a restore that recovers
  consumer offsets; `skip` is the escape hatch its error message points to.

### Fixed

- The operator no longer panics at startup when built against core 0.23.
  object_store 0.14 (via reqwest 0.13) links rustls' aws-lc-rs backend next to
  ring, so rustls could not pick a process-default crypto provider and the
  Kubernetes client panicked; the operator now installs ring explicitly.
- `spec.circuitBreaker` on `KafkaBackup` / `KafkaRestore` reaches core (it was
  silently ignored). `operationTimeoutMs` has no core counterpart.
- No core deprecation warning on every backup: the operator stops setting
  `offset_storage.s3_key` (ignored by core; the offset database always lived at
  `{backup_id}/offsets.db`) and `backup.checkpoint_interval_secs` (no effect).
  The offset database keeps syncing on `backup.sync_interval_secs`, driven by
  `checkpoint.intervalSecs`.
- Retention builds its storage backend from the same conversion as backups, so
  an explicit S3 `pathStyle` is honoured there too (core 0.22 started honouring
  it for backups).

### Changed

- `kafka-backup-core` 0.19.2 → 0.23.0: topic-config capture and restore,
  header preflight before offset recovery, missing-topic handling, tunable
  circuit breaker, restore lock-contention fixes.
- Docker builder `rust:1.88` → `rust:1.96` (new dependencies need ≥ 1.89).

## 1.3.0 - 2026-08-30

### Added

- `KafkaRestore.spec.includeOriginalOffsetHeader` (default `true`, the behaviour
  the operator always had) and `KafkaRestore.spec.stripOffsetHeaders` (default
  `false`). The latter removes the `x-original-*` / `x-source-*` headers
  kafka-backup added at backup time so restored records match the source
  header-for-header
  ([kafka-backup#154](https://github.com/osodevops/kafka-backup/issues/154)).
  Set `includeOriginalOffsetHeader: false` together with it for a verbatim
  restore. Header-based consumer offset recovery re-injects the restore-side
  headers regardless.

### Changed

- `kafka-backup-core` 0.19.0 → 0.19.2: compressed legacy (pre-binary-format)
  JSON segments are readable again on restore.
## 1.2.3 - 2026-08-29

### Fixed

- Two operator pods can no longer run the same `KafkaBackup` at once
  ([#79](https://github.com/osodevops/kafka-backup-operator/issues/79)).
  The Deployment had no update strategy, so during `helm upgrade` — and on
  node drain, eviction or preemption — the old and the new pod ran side by
  side; each decided schedules from its own cache and its own in-process
  duplicate-fire guard, so a cron tick due in the overlap could execute in
  both (two engines writing the same `backup_id`), and a backup already
  running in the old pod was cut off when it was asked to stop.
  Three layers now prevent and heal this: the chart rolls out with
  `maxSurge: 0` / `maxUnavailable: 1` (`updateStrategy` value; the outgoing
  pod is deleted before its replacement is created), the operator runs
  leader election so only the lease holder runs the controllers, and a
  running backup or restore is asked to stop gracefully on SIGTERM — it
  checkpoints, syncs its offset database and writes its manifest — before
  the lease is released to the successor.

### Added

- Lease-based leader election (`coordination.k8s.io/v1`, client-go
  semantics): the chart's `leaderElection.*` values and the
  `LEADER_ELECTION_ENABLED` / `LEADER_ELECTION_LEASE_DURATION` /
  `LEADER_ELECTION_RENEW_DEADLINE` / `LEADER_ELECTION_RETRY_PERIOD` /
  `LEADER_ELECTION_LEASE_NAME` / `OPERATOR_NAMESPACE` environment variables
  were rendered but never read; they are honoured now. Only the holder of the
  `<release>-leader` Lease runs the controllers; other replicas stand by, so
  `replicaCount: 2` gives a warm standby that takes over within seconds of the
  leader stopping. Lease expiry is judged on the observing pod's own clock and
  every lease request is bounded to half the renew deadline.
- `/readyz` reports `leader` or `standby` (200) once the replica has observed
  the lease and `leader election pending` (503) before, so an install whose
  ServiceAccount lacks the leases rule fails loudly. With leader election
  disabled it is ready immediately, as before.
- `kafka_backup_operator_leader{identity}` gauge (1 on the leader).
- `terminationGracePeriodSeconds` value (default 60, was a hard-coded 30) so a
  stopping backup has time to finalize.
- `scripts/e2e/` + `manifests/e2e/`: minikube-based end-to-end scenarios for
  operator upgrades and leader election (not run in CI).

### Changed

- `leaderElection.enabled` now defaults to `true` (the chart renders the
  `coordination.k8s.io/leases` ClusterRole rule accordingly). Set it to
  `false` to opt out; the `maxSurge: 0` rollout still covers plain upgrades.
- When the leader cannot renew its lease within `renewDeadline`, or finds the
  lease held by another replica, the process exits non-zero and the kubelet
  restarts it as a candidate.
- Shutdown is sequenced: controllers drain (a running backup finalizes), the
  lease is released, then the process exits — instead of dropping everything
  on the first signal.
- Logs are written through a lossy non-blocking writer: with a CPU limit the
  runtime has a single worker thread, and a blocking write to a backed-up
  container stdout used to freeze probes, lease renewals and the running
  backup together.

## 1.2.2 - 2026-08-29

### Fixed

- A record header whose value is **null** is now backed up and restored as
  null instead of an empty value
  ([kafka-backup#155](https://github.com/osodevops/kafka-backup/issues/155)).
  Kafka distinguishes the two and consumers branch on the difference; the
  loss happened on the backup path, so archives written by earlier versions
  store such headers as empty and must be re-taken where the distinction
  matters. Fixed by updating `kafka-backup-core` from 0.17.4 to 0.19.0.

### Changed

- The `KafkaBackup` CRD description for `includeOffsetHeaders` now states
  its default (`true`) and what the option adds to every archived record
  ([kafka-backup#154](https://github.com/osodevops/kafka-backup/issues/154)).
  Core 0.19.0 also adds `restore.strip_offset_headers`; the operator does
  not expose it yet and keeps restoring with its existing header behaviour.

## 1.2.1 - 2026-08-20

### Fixed

- Backups no longer fail with `failed to decompress raw snappy bytes …
  snappy: corrupt input` on snappy-compressed messages from clients that
  emit raw (non-xerial-framed) snappy, e.g. franz-go
  ([kafka-backup#152](https://github.com/osodevops/kafka-backup/issues/152)),
  and no longer misdecode record batches whose records' timestamps span more
  than ~24.8 days
  ([kafka-backup#150](https://github.com/osodevops/kafka-backup/issues/150)).
  Both were bugs in the embedded `kafka-protocol` dependency, fixed by
  updating `kafka-backup-core` from v0.15.12 to v0.17.4 (kafka-protocol
  0.18.0). The update also brings the core fixes from kafka-backup
  v0.16.0–v0.17.4: compacted-topic stall recovery, `OFFSET_OUT_OF_RANGE`
  recovery with retention-gap records, connection-loss classification with
  fetch retry, and offset resets applied after restore when configured.

### Changed

- `kafka-backup-core` is now consumed from crates.io (`0.17.4`) instead of a
  git tag, since core publishing has resumed.

## 1.2.0 - 2026-07-28

### Breaking Changes

- Per-resource operator metrics now carry a `resource_namespace` label instead
  of `namespace`. A Prometheus scrape attaches its own `namespace` target label,
  so the old name collided and was rewritten to `exported_namespace`, hiding
  those series from the obvious query. Dashboards and alerts selecting on
  `namespace` for `kafka_backup_operator_backups_total`,
  `kafka_backup_operator_backup_size_bytes`,
  `kafka_backup_operator_backup_records_total`,
  `kafka_backup_operator_restores_total`,
  `kafka_backup_operator_validations_total`, or
  `kafka_backup_operator_offset_resets_total` must be updated.

### Fixed

- Exported kafka-backup-core runtime metrics (`kafka_backup_*`: lag, throughput,
  storage IO, snapshot progress) on the operator's `/metrics` endpoint. The
  operator built a core `MetricsConfig` from `spec.metrics` but never started a
  collector for it, so these families were unreachable — `spec.metrics.port` was
  refused by every connection. Core metrics now share the operator's endpoint
  and its single ServiceMonitor target. (#73)
- Populated `kafka_backup_operator_backup_duration_seconds`,
  `kafka_backup_operator_restore_duration_seconds`,
  `kafka_backup_operator_validation_duration_seconds`, and
  `kafka_backup_operator_managed_resources`. All four were registered but never
  written, so they never appeared in a scrape.
- Corrected the README metrics table, which documented `kafka_backup_*` names
  the operator has never emitted; the operator's own families are prefixed
  `kafka_backup_operator_`.

### Added

- `kafka_backup_operator_build_info`, labelled with the operator version, so a
  healthy idle operator always serves a non-empty `/metrics` body.
- `metrics.maxPartitionLabels` Helm value (`METRICS_MAX_PARTITION_LABELS`),
  capping the topic/partition label sets core metrics may expose. Defaults to
  100; `0` disables the cap.

### Changed

- Updated `kafka-backup-core` from `v0.15.11` to `v0.15.12`, which labels
  storage write metrics with the backend actually written to instead of a
  hardcoded `filesystem`, and drops the doubled `_total_total` counter suffix.
- Documented `spec.metrics.port`, `bindAddress`, `path`, `updateIntervalMs`, and
  `maxPartitionLabels` as ignored. Backups run in the operator process, so there
  is no per-backup metrics server; only `spec.metrics.enabled` has an effect.
- Kept the operator image, Helm chart version, and Helm `appVersion` aligned at
  `1.2.0`.

## 1.1.1 - 2026-07-19

### Changed

- Updated the embedded `kafka-backup-core` dependency from `v0.15.7` to
  `v0.15.11`, incorporating the latest security refresh, pipelined segment
  uploads, one-shot metrics keep-alive support, and corrected progress metric
  cardinality accounting.
- Clarified that `metrics.maxPartitionLabels` limits unique topic/partition
  series and that `0` explicitly enables unlimited series.
- Kept the operator image, Helm chart version, and Helm `appVersion` aligned at
  `1.1.1`.

## 1.0.0 - 2026-04-13

### Breaking Changes

- `checkpoint.enabled` no longer implicitly enables continuous backup mode. Set `continuous: true` explicitly for streaming backups.

### Added

- Bumped `kafka-backup-core` to `v0.12.0`.
- Added `KafkaBackup` CRD fields for snapshot and streaming controls: `continuous`, `stopAtCurrentOffsets`, `segmentMaxBytes`, `segmentMaxIntervalMs`, `includeOffsetHeaders`, `sourceClusterId`, `pollIntervalMs`, and `consumerGroupSnapshot`.
- Added `KafkaRestore` CRD fields for restore tuning and issue #67 fixes: `repartitioning`, `produceBatchSize`, `produceAcks`, `produceTimeoutMs`, `purgeTopics`, and `autoConsumerGroups`.
- Added S3-compatible endpoint controls from PR #22: `storage.s3.pathStyle` and `storage.s3.allowHttp`, including a warning log when HTTP is enabled.
- Added Helm chart extension points from PR #22: `extraVolumes`, `extraVolumeMounts`, and `extraEnv` for custom CA bundles and environment-specific settings.
- Added shared Kafka connection tuning via `kafkaCluster.connection`, including `connectionsPerBroker`.
- Regenerated raw and Helm CRD bundles.

### Fixed

- Aligned Azure storage validation with the adapter-supported authentication methods: workload identity, service principal, SAS token, account key, and default credential fallback.
- Updated README examples for 7-field cron schedules, current offset-reset fields, and validation CRD coverage.
