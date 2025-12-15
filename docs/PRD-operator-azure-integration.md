# Product Requirements Document: Operator Azure Storage Integration Fix

**Document Version:** 1.0
**Date:** 2025-12-12
**Author:** StreamPlus Platform Team
**Status:** Draft
**Effort Estimate:** Small (1-2 days)

---

## Executive Summary

The `kafka-backup-operator` has CRD definitions for Azure Blob Storage but the integration layer contains a stub that doesn't pass configuration to the core library. The `kafka-backup-core` library **already fully supports Azure Blob Storage** (verified via CLI testing). This PRD outlines the changes needed in the operator to enable Azure storage support.

---

## Problem Statement

### Current Behaviour

When a `KafkaBackup` CR specifies Azure storage:

```yaml
storage:
  storageType: azure
  azure:
    accountName: sastreamplusnonprod
    container: non-prod-kafka-backups
    prefix: backups
    credentialsSecret:
      name: azure-storage-credentials
      accountKeyKey: AZURE_STORAGE_KEY
```

The operator fails with:
```
Configuration error: Bucket is required for S3 backend
```

### Root Cause

In `src/adapters/core_integration.rs:148-162`, the Azure storage configuration is not mapped to the core library:

```rust
ResolvedStorage::Azure(_azure) => {
    // TODO: Map Azure to S3-compatible or extend core library
    StorageConfig {
        backend: StorageBackendType::S3,  // Wrong backend type
        path: None,
        endpoint: None,
        bucket: None,                      // Azure container discarded
        access_key: None,
        secret_key: None,
        prefix: None,
        region: None,
    }
}
```

### Evidence of Core Library Support

The `kafka-backup-core` library fully supports Azure Blob Storage. Verified via CLI:

```bash
./target/release/kafka-backup backup --config config/streamplus-nonprod-azure-test.yaml
```

**Result:** Successfully backed up 2,738 records to Azure Blob Storage in 1.23 seconds.

---

## Scope

### In Scope

1. Update `core_integration.rs` to map Azure storage configuration
2. Add `StorageBackendType::Azure` handling (if not present)
3. Support both account key and Workload Identity authentication
4. Update `storage_config.rs` to handle empty/missing credentials for Workload Identity
5. Unit tests for the new mapping logic

### Out of Scope

- Changes to `kafka-backup-core` library (already works)
- Changes to CRD definitions (already correct)
- GCS storage support (separate effort)
- New authentication methods beyond account key and Workload Identity

---

## Requirements

### FR-1: Storage Configuration Mapping

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1.1 | Map `StorageBackendType::Azure` (or equivalent) in core integration | P0 |
| FR-1.2 | Pass `account_name` from CR to core config | P0 |
| FR-1.3 | Pass `container` from CR to core config as `container_name` | P0 |
| FR-1.4 | Pass `prefix` from CR to core config | P0 |
| FR-1.5 | Pass `account_key` from resolved credentials to core config | P0 |

### FR-2: Workload Identity Support

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-2.1 | When `account_key` is empty, set `use_workload_identity: true` | P0 |
| FR-2.2 | Allow credential resolution to return empty key for Workload Identity | P0 |
| FR-2.3 | Operator pod must have Workload Identity environment variables | P0 |

### FR-3: CRD Enhancement (Optional)

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-3.1 | Add optional `useWorkloadIdentity` field to Azure storage spec | P1 |
| FR-3.2 | Make `credentialsSecret` optional when `useWorkloadIdentity: true` | P1 |

---

## Technical Specification

### File Changes Required

#### 1. `src/adapters/core_integration.rs`

**Current code (lines 148-162):**
```rust
ResolvedStorage::Azure(_azure) => {
    // TODO: Map Azure to S3-compatible or extend core library
    StorageConfig {
        backend: StorageBackendType::S3,
        path: None,
        endpoint: None,
        bucket: None,
        access_key: None,
        secret_key: None,
        prefix: None,
        region: None,
    }
}
```

**Proposed fix:**
```rust
ResolvedStorage::Azure(azure) => StorageConfig {
    backend: StorageBackendType::Azure,
    path: None,
    endpoint: azure.endpoint.clone(),
    bucket: None,
    access_key: None,
    secret_key: None,
    prefix: azure.prefix.clone(),
    region: None,
    // Azure-specific fields
    azure_account_name: Some(azure.account_name.clone()),
    azure_container_name: Some(azure.container.clone()),
    azure_account_key: if azure.account_key.is_empty() {
        None
    } else {
        Some(azure.account_key.clone())
    },
    azure_use_workload_identity: Some(azure.account_key.is_empty()),
},
```

#### 2. `src/adapters/storage_config.rs`

**Current code (lines 127-133):**
```rust
let account_key = get_azure_credentials(
    client,
    namespace,
    &azure.credentials_secret.name,
    &azure.credentials_secret.account_key_key,
)
.await?;
```

**Proposed fix:**
```rust
// Allow empty credentials for Workload Identity
let account_key = match get_azure_credentials(
    client,
    namespace,
    &azure.credentials_secret.name,
    &azure.credentials_secret.account_key_key,
).await {
    Ok(key) => key,
    Err(_) if azure.use_workload_identity.unwrap_or(false) => String::new(),
    Err(e) => return Err(e),
};
```

#### 3. `src/adapters/storage_config.rs` - AzureStorageConfig struct

**Add optional endpoint field:**
```rust
#[derive(Debug, Clone)]
pub struct AzureStorageConfig {
    pub container: String,
    pub account_name: String,
    pub account_key: String,  // Empty string signals Workload Identity
    pub prefix: Option<String>,
    pub endpoint: Option<String>,  // For sovereign clouds
}
```

#### 4. `src/crd/kafka_backup.rs` (Optional - P1)

**Add Workload Identity field to AzureStorageSpec:**
```rust
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AzureStorageSpec {
    pub account_name: String,
    pub container: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    pub credentials_secret: AzureCredentialsSecretRef,
    #[serde(default)]
    pub use_workload_identity: Option<bool>,  // New field
}
```

---

## Test Plan

### Unit Tests

| Test Case | Description |
|-----------|-------------|
| `test_azure_storage_mapping` | Verify Azure config maps correctly to core StorageConfig |
| `test_azure_workload_identity_mapping` | Verify empty account_key sets use_workload_identity |
| `test_azure_with_account_key` | Verify account_key is passed through correctly |
| `test_azure_with_prefix` | Verify prefix is mapped correctly |

### Integration Tests

| Test Case | Description |
|-----------|-------------|
| `test_azure_backup_with_account_key` | Full backup to Azure using account key |
| `test_azure_backup_with_workload_identity` | Full backup using Workload Identity (AKS required) |

### Manual Verification

1. Deploy operator with fix to AKS non-prod cluster
2. Apply `KafkaBackup-nonprod-cml-changelogs.yaml`
3. Verify backup runs successfully
4. Verify blobs appear in Azure storage container

---

## Deployment Plan

### Phase 1: Development
1. Implement changes in `core_integration.rs`
2. Update `storage_config.rs` for Workload Identity fallback
3. Add unit tests
4. Local testing with CLI parity check

### Phase 2: Testing
1. Build new operator image
2. Push to ACR: `acrstreamplusnonprodeuw.azurecr.io/osodevops/kafka-backup-operator:azure-fix`
3. Deploy to non-prod AKS cluster
4. Test with `KafkaBackup-nonprod-cml-changelogs.yaml`

### Phase 3: Release
1. Merge to main branch
2. Tag release (e.g., `v0.1.2`)
3. Update Helm chart version
4. Update documentation

---

## Dependencies

| Dependency | Status |
|------------|--------|
| kafka-backup-core Azure support | Complete |
| AKS Workload Identity | Configured |
| Azure storage container | Created |
| Managed Identity role assignment | Complete |

---

## Acceptance Criteria

1. `KafkaBackup` CR with `storageType: azure` creates backups in Azure Blob Storage
2. Both account key and Workload Identity authentication methods work
3. Backup segments appear at correct path: `{prefix}/{backup_id}/topics/{topic}/partition={n}/`
4. No regression in S3 or PVC storage backends
5. Operator logs show "Created Azure backend for account: ..." on successful init

---

## Appendix

### A. Verified Core Library Config Format

From `kafka-backup/config/example-backup-azure.yaml`:

```yaml
storage:
  backend: azure
  account_name: mystorageaccount
  container_name: kafka-backups
  prefix: production/
  account_key: ${AZURE_STORAGE_KEY}
  # OR for Workload Identity:
  # use_workload_identity: true
```

### B. Test Results

```
$ ./target/release/kafka-backup backup --config config/streamplus-nonprod-azure-test.yaml

INFO  kafka_backup_core::storage::azure: Created Azure backend for account: sastreamplusnonprod, container: non-prod-kafka-backups
INFO  kafka_backup_core::backup::engine: Backing up 2 topics
...
INFO  kafka_backup_core::backup::engine: === Performance Metrics ===
Duration: 1.23s
Records processed: 2738
Segments written: 20
Errors: 0
INFO  kafka_backup_core::backup::engine: Backup completed successfully
```

### C. Related Files

| File | Purpose |
|------|---------|
| `src/adapters/core_integration.rs` | Maps operator types to core library types |
| `src/adapters/storage_config.rs` | Resolves storage credentials from K8s secrets |
| `src/crd/kafka_backup.rs` | CRD type definitions |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-12 | StreamPlus Platform Team | Initial draft |
