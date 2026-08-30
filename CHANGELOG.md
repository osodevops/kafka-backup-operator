# Changelog

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
