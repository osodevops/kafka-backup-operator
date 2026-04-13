# Changelog

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
