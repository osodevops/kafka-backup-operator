# OSO Kafka Backup Kubernetes Operator PRD

**Language:** Rust (kubebuilder-equivalent using `kube-rs`)  
**Date:** 2025-11-30  
**Version:** 1.0  
**Scope:** Lightweight Kubernetes Operator for managing OSO Kafka Backup (OSS & Enterprise)

---

## Executive Summary

This PRD defines a **minimal-footprint Kubernetes Operator** for OSO Kafka Backup, enabling GitOps-style backup/restore workflows on Kubernetes. The operator:

- Watches `KafkaBackup` Custom Resources (CRDs)
- Orchestrates backup scheduling, restoration, and offset management declaratively
- Supports both OSS and Enterprise editions
- Lightweight: ~50MB container, minimal memory/CPU overhead
- Uses **Rust + `kube-rs`** for performance and safety (not Go)

---

## Part 1: Why Rust + kube-rs (Not Go)

### Trade-off Analysis

| Aspect | Go + kubebuilder | Rust + kube-rs |
|--------|------------------|----------------|
| **Build Time** | Fast (~5 sec) | Slower (~30 sec) |
| **Runtime Memory** | ~20MB base | ~5MB base |
| **Cold Start** | ~50ms | ~30ms |
| **CPU Under Load** | Higher (GC overhead ~10%) | Lower (no GC) |
| **Binary Size** | ~10MB | ~8MB |
| **Async Support** | Good (goroutines) | Better (tokio, native async) |
| **Type Safety** | Good | Excellent (compile-time guarantees) |

### Decision: Use Rust + kube-rs

**Reasoning:**

1. **Consistency:** Core + CLI + Enterprise all written in Rust. Single language stack reduces maintenance burden.
2. **Performance:** Lightweight operator must not consume resources competing with Kafka. Rust's zero-cost abstractions win here.
3. **Async I/O:** Heavy async workloads (parallel backups, multiple reconciliation loops). Rust's native async/await + tokio is superior.
4. **Compile-time Safety:** Operator bugs lead to data loss. Rust catches entire classes of errors at compile time.

**Framework Choice:**

- **Use:** `kube-rs` (Kubernetes Rust client) + `tokio` + custom reconciler
- **Not:** kubebuilder (Go-only). Strimzi uses Java, but we have Rust everywhere.
- **Reference:** Velero uses Go but is 200MB+. We can do better with Rust.

---

## Part 2: Architecture

### 2.1 High-Level Design

```
┌─────────────────────────────────────────────┐
│      Kubernetes Cluster                      │
├─────────────────────────────────────────────┤
│                                              │
│  ┌────────────────────────────────────┐     │
│  │  OSO Kafka Backup Operator         │     │
│  │  (Single Pod, ~5-50MB)             │     │
│  │                                    │     │
│  │  - Watches KafkaBackup CRD        │     │
│  │  - Watches KafkaRestore CRD       │     │
│  │  - Watches KafkaOffsetReset CRD   │     │
│  │                                    │     │
│  │  Reconcilers:                      │     │
│  │  - BackupScheduler (cron jobs)     │     │
│  │  - RestoreExecutor                 │     │
│  │  - OffsetManager                   │     │
│  └────────────────────────────────────┘     │
│                  ↓                           │
│  Kafka Backup CLI Tool (subprocess)         │
│  - Runs backup/restore/offset logic         │
│  - Reuses existing OSS binary               │
│  - Mounted via ConfigMap or Pod volumes     │
│                                              │
└─────────────────────────────────────────────┘
        ↓          ↓           ↓
    Kafka      S3/Azure      PostgreSQL
   Cluster     (backups)      (metadata)
```

### 2.2 Custom Resource Definitions (CRDs)

#### KafkaBackup

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: my-cluster-daily
  namespace: kafka-system
spec:
  # Target Kafka cluster (must be accessible)
  kafkaCluster:
    bootstrapServers: "kafka-broker-1:9092,kafka-broker-2:9092"
  
  # What to backup
  topics:
    - "orders"
    - "payments"
  
  # Backup schedule (cron format)
  schedule: "0 2 * * *"  # Daily at 2 AM
  
  # Storage destination
  storage:
    s3:
      bucket: "kafka-backups"
      region: "us-east-1"
      credentialsSecret: "aws-creds"  # K8s Secret name
      prefix: "backups/"
  
  # Retention policy
  retention:
    backups: 30           # Keep 30 daily backups
    days: 90              # Or keep for 90 days
  
  # Compression
  compression: "zstd"
  
  # License (OSS or Enterprise)
  license: "oss"          # or "enterprise"
  
  # Optional: custom backup command
  customArgs:
    - "--parallel-uploads=5"

status:
  lastBackupTime: "2025-11-30T02:00:00Z"
  lastBackupStatus: "success"
  backupCount: 45
  nextScheduledBackup: "2025-12-01T02:00:00Z"
  lastBackupSize: "10.2Gi"
```

#### KafkaRestore

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaRestore
metadata:
  name: restore-payment-bug
  namespace: kafka-system
spec:
  # Source backup
  backupRef:
    name: "my-cluster-daily"
    timestamp: "2025-11-29T20:00:00Z"  # PITR timestamp
  
  # Target cluster (can be different from backup source)
  kafkaCluster:
    bootstrapServers: "kafka-restore-cluster:9092"
  
  # What to restore
  topics:
    - "payments"  # Restore only this topic
  
  # Offset recovery
  offsetRecovery:
    enabled: true
    groups:
      - "payment-processor"  # Reset this group to restore time
  
  # Rollback config
  rollback:
    enabled: true
    snapshotRef: "offset-snapshot-before-restore"
  
  license: "oss"

status:
  restoreStatus: "in-progress"
  startTime: "2025-11-30T10:00:00Z"
  messagesRestored: 1500000
  estimatedCompletion: "2025-11-30T10:45:00Z"
```

#### KafkaOffsetReset

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaOffsetReset
metadata:
  name: reset-groups-bulk
  namespace: kafka-system
spec:
  # Target cluster
  kafkaCluster:
    bootstrapServers: "kafka:9092"
  
  # Consumer groups to reset
  consumerGroups:
    - "payment-processor"
    - "order-processor"
    - "analytics-consumer"
  
  # How to reset (to-offset, to-timestamp, to-earliest, to-latest)
  resetStrategy: "to-timestamp"
  resetTimestamp: "2025-11-29T14:30:00Z"
  
  # Parallel execution
  parallelism: 50  # Concurrent group resets
  
  # Dry-run mode (validate but don't execute)
  dryRun: false
  
  license: "oss"

status:
  resetStatus: "completed"
  groupsReset: 3
  groupsFailed: 0
  duration: "2m30s"
```

---

## Part 3: Operator Components

### 3.1 Directory Structure

```
kafka-backup-operator/
├── Cargo.toml
├── Cargo.lock
├── src/
│   ├── main.rs                    # Entry point
│   ├── lib.rs
│   ├── crd/
│   │   ├── mod.rs
│   │   ├── kafka_backup.rs        # KafkaBackup CRD
│   │   ├── kafka_restore.rs       # KafkaRestore CRD
│   │   └── kafka_offset_reset.rs  # KafkaOffsetReset CRD
│   ├── reconcilers/
│   │   ├── mod.rs
│   │   ├── backup_reconciler.rs   # Backup scheduling & execution
│   │   ├── restore_reconciler.rs  # Restore orchestration
│   │   └── offset_reset_reconciler.rs
│   ├── controllers/
│   │   ├── mod.rs
│   │   ├── backup_controller.rs
│   │   ├── restore_controller.rs
│   │   └── offset_controller.rs
│   ├── executor/
│   │   ├── mod.rs
│   │   └── kafka_backup_cli.rs    # Invokes OSS CLI binary
│   ├── storage/
│   │   ├── mod.rs
│   │   └── s3_client.rs           # S3 interaction for license checks
│   ├── metrics/
│   │   ├── mod.rs
│   │   └── prometheus.rs
│   └── config.rs                  # Config loading
├── deploy/
│   ├── crd/
│   │   ├── kafkabackup_crd.yaml
│   │   ├── kafkarestore_crd.yaml
│   │   └── kafkaoffsetreset_crd.yaml
│   ├── rbac/
│   │   ├── serviceaccount.yaml
│   │   ├── role.yaml
│   │   └── rolebinding.yaml
│   ├── operator/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── configmap.yaml
│   └── helm/
│       └── Chart.yaml
├── tests/
│   ├── integration_tests.rs
│   └── e2e_tests.rs
└── Dockerfile
```

### 3.2 Core Reconciler Logic (Rust)

**File:** `src/reconcilers/backup_reconciler.rs`

```rust
use kube::{
    api::{Patch, PatchParams},
    Client, Api,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use chrono::{DateTime, Utc};
use cron_schedule::CronSchedule;

use crate::crd::KafkaBackup;
use crate::executor::KafkaBackupExecutor;

pub struct BackupReconciler {
    client: Client,
    executor: Arc<KafkaBackupExecutor>,
}

impl BackupReconciler {
    pub async fn reconcile(&self, backup: KafkaBackup, ns: &str) -> Result<()> {
        let api: Api<KafkaBackup> = Api::namespaced(self.client.clone(), ns);
        
        // Parse cron schedule
        let schedule = CronSchedule::parse(&backup.spec.schedule)
            .map_err(|e| anyhow::anyhow!("Invalid cron: {}", e))?;
        
        // Check if backup should run now
        let now = Utc::now();
        let next_run = schedule.next_after(now);
        
        // If next run is in past (within 1 minute), execute
        let time_until_run = next_run.signed_duration_since(now);
        if time_until_run.num_seconds() <= 60 && time_until_run.num_seconds() >= -60 {
            // Execute backup
            match self.executor.backup(&backup).await {
                Ok(result) => {
                    // Update status
                    let patch = json!({
                        "status": {
                            "lastBackupTime": now.to_rfc3339(),
                            "lastBackupStatus": "success",
                            "lastBackupSize": result.size_gb,
                            "nextScheduledBackup": next_run.to_rfc3339(),
                        }
                    });
                    
                    api.patch(
                        &backup.metadata.name.unwrap(),
                        &PatchParams::apply("operator"),
                        &Patch::Merge(patch),
                    ).await?;
                    
                    tracing::info!("Backup {} completed successfully", backup.metadata.name.unwrap());
                }
                Err(e) => {
                    tracing::error!("Backup failed: {}", e);
                    
                    let patch = json!({
                        "status": {
                            "lastBackupStatus": "failed",
                            "lastBackupError": e.to_string(),
                        }
                    });
                    
                    api.patch(
                        &backup.metadata.name.unwrap(),
                        &PatchParams::apply("operator"),
                        &Patch::Merge(patch),
                    ).await?;
                }
            }
        }
        
        Ok(())
    }
}
```

**File:** `src/executor/kafka_backup_cli.rs`

```rust
use std::process::Command;
use serde::{Deserialize, Serialize};
use crate::crd::KafkaBackup;

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupResult {
    pub backup_id: String,
    pub size_gb: f64,
    pub messages_count: u64,
    pub duration_sec: u64,
}

pub struct KafkaBackupExecutor {
    cli_path: String,  // Path to kafka-backup binary in pod
}

impl KafkaBackupExecutor {
    pub fn new(cli_path: String) -> Self {
        Self { cli_path }
    }
    
    pub async fn backup(&self, backup_spec: &KafkaBackup) -> Result<BackupResult, Box<dyn std::error::Error>> {
        let args = self.build_backup_args(backup_spec)?;
        
        // Invoke OSS kafka-backup binary
        let output = Command::new(&self.cli_path)
            .args(&args)
            .output()?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Backup failed: {}", stderr).into());
        }
        
        // Parse JSON output from CLI
        let result: BackupResult = serde_json::from_slice(&output.stdout)?;
        Ok(result)
    }
    
    fn build_backup_args(&self, backup_spec: &KafkaBackup) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut args = vec!["backup".to_string()];
        
        // Add bootstrap servers
        args.push("--bootstrap-servers".to_string());
        args.push(backup_spec.spec.kafka_cluster.bootstrap_servers.clone());
        
        // Add topics
        args.push("--topics".to_string());
        args.push(backup_spec.spec.topics.join(","));
        
        // Add S3 config
        if let Some(s3) = &backup_spec.spec.storage.s3 {
            args.push("--destination-s3-bucket".to_string());
            args.push(s3.bucket.clone());
            args.push("--destination-s3-region".to_string());
            args.push(s3.region.clone());
        }
        
        // Add compression
        args.push("--compression".to_string());
        args.push(backup_spec.spec.compression.clone());
        
        // Add JSON output
        args.push("--output-format".to_string());
        args.push("json".to_string());
        
        Ok(args)
    }
}
```

### 3.3 License Verification

**File:** `src/config.rs`

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum License {
    #[serde(rename = "oss")]
    OSS,
    #[serde(rename = "enterprise")]
    Enterprise,
}

pub fn verify_license(license: &License) -> Result<(), String> {
    match license {
        License::OSS => {
            // OSS: no license check needed
            Ok(())
        }
        License::Enterprise => {
            // Enterprise: check for license secret
            // This would be mounted as K8s Secret
            let license_key = std::env::var("OSO_LICENSE_KEY")
                .map_err(|_| "Enterprise license: OSO_LICENSE_KEY not found".to_string())?;
            
            // Validate license key (call licensing service or validate offline)
            validate_enterprise_license(&license_key)?;
            
            Ok(())
        }
    }
}

fn validate_enterprise_license(license_key: &str) -> Result<(), String> {
    // Implementation: could check expiry, signature, etc.
    // For now, simple non-empty check
    if license_key.is_empty() {
        return Err("License key is empty".to_string());
    }
    Ok(())
}
```

---

## Part 4: Deployment

### 4.1 Dockerfile

```dockerfile
# Build stage
FROM rust:1.75-slim as builder

WORKDIR /workspace

# Copy source
COPY . .

# Build operator binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install minimal dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy operator binary from builder
COPY --from=builder /workspace/target/release/kafka-backup-operator /usr/local/bin/operator

# Copy OSS kafka-backup CLI binary (or download at runtime)
# For now, we assume it's available in the container or mounted

WORKDIR /

ENTRYPOINT ["/usr/local/bin/operator"]
```

**Final Image Size:** ~50-80MB (Debian base + Rust binary)

### 4.2 Kubernetes Deployment

**File:** `deploy/operator/deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-backup-operator
  namespace: kafka-system
spec:
  replicas: 1  # Single operator instance
  selector:
    matchLabels:
      app: kafka-backup-operator
  template:
    metadata:
      labels:
        app: kafka-backup-operator
    spec:
      serviceAccountName: kafka-backup-operator
      containers:
      - name: operator
        image: oso/kafka-backup-operator:0.1.0
        imagePullPolicy: Always
        resources:
          requests:
            memory: "32Mi"
            cpu: "50m"
          limits:
            memory: "128Mi"
            cpu: "200m"
        env:
        - name: RUST_LOG
          value: "info"
        - name: OSO_KAFKA_CLI_PATH
          value: "/usr/local/bin/kafka-backup"
        - name: OSO_LICENSE
          valueFrom:
            configMapKeyRef:
              name: oso-operator-config
              key: license
              optional: true
        - name: OSO_LICENSE_KEY
          valueFrom:
            secretKeyRef:
              name: oso-license
              key: license-key
              optional: true
        volumeMounts:
        - name: kafka-backup-cli
          mountPath: /usr/local/bin/kafka-backup
          subPath: kafka-backup
          readOnly: true
      volumes:
      - name: kafka-backup-cli
        configMap:
          name: kafka-backup-cli-binary
          defaultMode: 0755
```

### 4.3 Helm Chart

**File:** `deploy/helm/Chart.yaml`

```yaml
apiVersion: v2
name: kafka-backup-operator
description: Kubernetes Operator for OSO Kafka Backup (OSS & Enterprise)
type: application
version: 0.1.0
appVersion: "0.1.0"

keywords:
  - kafka
  - backup
  - operator
  - disaster-recovery

maintainers:
  - name: OSO Team
    email: dev@oso.sh
```

**Usage:**

```bash
# Install OSS version
helm install kafka-backup-operator ./deploy/helm \
  --namespace kafka-system \
  --create-namespace \
  --set license=oss

# Install Enterprise version
helm install kafka-backup-operator ./deploy/helm \
  --namespace kafka-system \
  --create-namespace \
  --set license=enterprise \
  --set licenseKey=$(cat license-key.txt)
```

---

## Part 5: Features

### OSS Features (Always Available)

- ✅ `KafkaBackup` CRD for scheduled backups
- ✅ `KafkaRestore` CRD for PITR restores
- ✅ `KafkaOffsetReset` CRD for consumer offset management
- ✅ Cron-based scheduling
- ✅ S3/Azure blob storage integration
- ✅ Status tracking and events
- ✅ Prometheus metrics
- ✅ Dry-run mode

### Enterprise Features (License Key Required)

- 🔐 RBAC-based access control (who can backup/restore)
- 🔐 Audit logging (all operations logged to PostgreSQL)
- 🔐 Encryption at rest (AES-256 for S3 uploads)
- 🔐 GDPR compliance (right-to-be-forgotten CRD)
- 🔐 Webhook validations (advanced schema checks)
- 🔐 Support & SLA

---

## Part 6: Operators (GitOps Workflows)

### Example 1: Daily Backup

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: prod-backup-daily
spec:
  kafkaCluster:
    bootstrapServers: "kafka-prod:9092"
  topics:
    - "orders"
    - "payments"
    - "events"
  schedule: "0 2 * * *"
  storage:
    s3:
      bucket: "kafka-backups"
      region: "us-east-1"
      credentialsSecret: "aws-creds"
  retention:
    days: 90
  license: "oss"
```

### Example 2: Disaster Recovery (PITR + Rollback)

```yaml
---
# Step 1: Restore data to a specific time
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaRestore
metadata:
  name: restore-bug-fix
spec:
  backupRef:
    name: prod-backup-daily
    timestamp: "2025-11-29T14:00:00Z"
  kafkaCluster:
    bootstrapServers: "kafka-prod:9092"
  topics:
    - "orders"
  offsetRecovery:
    enabled: true
    groups:
      - "order-processor"
  rollback:
    enabled: true
  license: "oss"
---
# Step 2: Reset offsets after restore
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaOffsetReset
metadata:
  name: reset-orders-group
spec:
  kafkaCluster:
    bootstrapServers: "kafka-prod:9092"
  consumerGroups:
    - "order-processor"
  resetStrategy: "to-timestamp"
  resetTimestamp: "2025-11-29T14:00:00Z"
  parallelism: 50
  license: "oss"
```

---

## Part 7: Metrics & Observability

### Prometheus Metrics

```rust
// expose_metrics() in src/metrics/prometheus.rs

// Counter: successful backups
kafka_backup_operator_backups_total{outcome="success"} 

// Counter: failed backups
kafka_backup_operator_backups_total{outcome="failure"} 

// Gauge: backup size in bytes
kafka_backup_operator_backup_size_bytes 

// Histogram: backup duration
kafka_backup_operator_backup_duration_seconds 

// Gauge: current offset lag
kafka_backup_operator_offset_lag_total 

// Counter: restore operations
kafka_backup_operator_restores_total{outcome="success|failure"} 
```

### Example ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-backup-operator
spec:
  selector:
    matchLabels:
      app: kafka-backup-operator
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
```

---

## Part 8: Testing

### Integration Tests

```rust
#[tokio::test]
async fn test_backup_reconciliation() {
    // Create mock KafkaBackup CRD
    // Verify reconciler creates Job
    // Verify metrics updated
}

#[tokio::test]
async fn test_restore_with_offset_recovery() {
    // Create KafkaRestore CRD
    // Verify offset snapshot created
    // Verify offsets reset correctly
}

#[tokio::test]
async fn test_license_verification() {
    // Test OSS: should allow all operations
    // Test Enterprise without license: should deny
    // Test Enterprise with valid license: should allow
}
```

---

## Part 9: Operator Maturity Levels

### Level 1: Basic Lifecycle Management (v0.1.0)

- [ ] Install operator
- [ ] Create backup CRD
- [ ] Schedule backups
- [ ] Restore from backups

### Level 2: Advanced Lifecycle (v0.2.0)

- [ ] Offset management
- [ ] Rollback capability
- [ ] Multi-cluster support
- [ ] Retention policies

### Level 3: Production-Ready (v0.3.0)

- [ ] Deep insights (web dashboard)
- [ ] Automated failover
- [ ] Backup validation
- [ ] Cross-cluster replication

---

## Part 10: Acceptance Criteria

✅ **Single operator pod** <100MB memory, <100m CPU  
✅ **Sub-10 second reconciliation** loop  
✅ **CRD-based workflow** (no CLI required from users)  
✅ **OSS & Enterprise support** via single binary  
✅ **Helm chart** for easy installation  
✅ **Prometheus metrics** for monitoring  
✅ **Integration tests** with real K8s cluster  
✅ **No external dependencies** (except Kafka and S3)  

---

## Implementation Timeline

| Milestone | Features | Estimate |
|-----------|----------|----------|
| **v0.1** | KafkaBackup CRD, basic reconciler | 3 weeks |
| **v0.2** | KafkaRestore, offset management | 2 weeks |
| **v0.3** | KafkaOffsetReset, license checks | 2 weeks |
| **v0.4** | Helm chart, docs, examples | 2 weeks |
| **v1.0** | Enterprise features, production hardening | 4 weeks |

**Total: ~13 weeks to production-ready v1.0**

---

## Why Rust (Final Summary)

1. **Consistency:** Entire stack (core, CLI, enterprise, operator) in one language.
2. **Performance:** Lightweight operator critical for Kubernetes resource constraints.
3. **Safety:** Operator bugs = data loss. Rust prevents entire classes of errors.
4. **Async:** Heavy concurrency workload. Rust's async/await + tokio is best-in-class.
5. **Shipping:** Binary is self-contained, no runtime required (unlike Go's 20MB base or Java's 100MB+).
