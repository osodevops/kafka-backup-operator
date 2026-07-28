# Changelog

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
