# Kafka Backup Operator

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.26%2B-326CE5.svg)](https://kubernetes.io/)

A Kubernetes operator for automated Kafka backup and disaster recovery. Built with Rust using [kube-rs](https://kube.rs/) for high performance and reliability.

## Features

- **Scheduled Backups** - Cron-based automatic backups with configurable retention
- **Point-in-Time Recovery (PITR)** - Restore data to any specific timestamp
- **Multi-Cloud Storage** - Support for PVC, S3, Azure Blob Storage, and GCS
- **Azure Workload Identity** - Secure, secretless authentication for Azure
- **Compression** - LZ4 and Zstd compression support with configurable levels
- **Checkpointing** - Resumable backups that survive pod restarts
- **Rate Limiting** - Control backup/restore throughput to minimize cluster impact
- **Circuit Breaker** - Automatic failure detection and recovery
- **Topic Mapping** - Restore to different topic names or partitions
- **Consumer Offset Management** - Reset and rollback consumer group offsets
- **Prometheus Metrics** - Full observability with built-in metrics endpoint

## Quick Start

### Prerequisites

- Kubernetes cluster (1.26+)
- Helm 3.x
- A running Kafka cluster

### Installation

```bash
# Add the OSO DevOps Helm repository
helm repo add oso https://osodevops.github.io/helm-charts/
helm repo update

# Install the operator
helm install kafka-backup-operator oso/kafka-backup-operator \
  --namespace kafka-backup-system \
  --create-namespace
```

### Create Your First Backup

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: my-backup
  namespace: kafka
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
  topics:
    - orders
    - events
  storage:
    storageType: pvc
    pvc:
      claimName: kafka-backups
  schedule: "0 */6 * * *"  # Every 6 hours
  compression: zstd
```

```bash
kubectl apply -f backup.yaml
```

## Custom Resource Definitions

The operator provides four CRDs for managing Kafka backup and restore operations:

| CRD | Short Name | Description |
|-----|------------|-------------|
| `KafkaBackup` | `kb` | Define backup schedules and configurations |
| `KafkaRestore` | `kr` | Trigger restore operations from backups |
| `KafkaOffsetReset` | `kor` | Reset consumer group offsets |
| `KafkaOffsetRollback` | `korb` | Rollback offsets after failed restores |

## Configuration Examples

### Backup to Azure Blob Storage (with Workload Identity)

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: production-backup
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
    securityProtocol: SASL_SSL
    tlsSecret:
      name: kafka-tls
    saslSecret:
      name: kafka-sasl
      mechanism: SCRAM-SHA-512
  topics:
    - orders
    - inventory
    - events
  storage:
    storageType: azure
    azure:
      container: kafka-backups
      accountName: mystorageaccount
      prefix: production
      useWorkloadIdentity: true
  schedule: "0 2 * * *"  # Daily at 2 AM
  compression: zstd
  compressionLevel: 3
  checkpoint:
    enabled: true
    intervalSecs: 30
```

### Backup to S3

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: s3-backup
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka:9092
  topics:
    - my-topic
  storage:
    storageType: s3
    s3:
      bucket: my-kafka-backups
      region: eu-west-1
      prefix: backups
      credentialsSecret:
        name: aws-credentials
        accessKeyIdKey: AWS_ACCESS_KEY_ID
        secretAccessKeyKey: AWS_SECRET_ACCESS_KEY
  schedule: "0 */4 * * *"
```

### Restore from Backup

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaRestore
metadata:
  name: restore-orders
spec:
  backupRef:
    name: production-backup
    backupId: "production-backup-20251210-020000"  # Optional: specific backup
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
  topics:
    - orders
  # Optional: Point-in-time recovery
  pitr:
    endTime: "2025-12-10T12:00:00Z"
  # Optional: Restore to different topic
  topicMapping:
    orders: orders-restored
  # Safety: Create snapshot before restore
  rollback:
    snapshotBeforeRestore: true
    autoRollbackOnFailure: true
```

### Reset Consumer Offsets

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaOffsetReset
metadata:
  name: reset-consumer
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
  consumerGroup: my-consumer-group
  topics:
    - orders
  resetStrategy: earliest  # earliest, latest, timestamp, offset
```

## Helm Values

Key configuration options for the Helm chart:

```yaml
# values.yaml
replicaCount: 1

image:
  repository: ghcr.io/osodevops/kafka-backup-operator
  tag: ""  # Defaults to appVersion

serviceAccount:
  create: true
  annotations: {}

# Azure Workload Identity
azureWorkloadIdentity:
  enabled: false
  clientId: ""

# Logging
logging:
  level: "info,kafka_backup_operator=debug"

# Metrics
metrics:
  enabled: true
  serviceMonitor:
    enabled: false
    interval: 30s

# Resources
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

## Azure Workload Identity Setup

For secure, secretless authentication to Azure Blob Storage:

```bash
# Enable Workload Identity on AKS
az aks update --resource-group myRG --name myAKS \
  --enable-oidc-issuer --enable-workload-identity

# Create managed identity
az identity create --resource-group myRG --name kafka-backup-identity

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee-object-id $(az identity show -g myRG -n kafka-backup-identity --query principalId -o tsv) \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/.../storageAccounts/mystorageaccount

# Create federated credential
az identity federated-credential create \
  --resource-group myRG \
  --identity-name kafka-backup-identity \
  --name kafka-backup-fedcred \
  --issuer $(az aks show -g myRG -n myAKS --query oidcIssuerProfile.issuerUrl -o tsv) \
  --subject system:serviceaccount:kafka-backup-system:kafka-backup-operator \
  --audience api://AzureADTokenExchange

# Install with Workload Identity enabled
helm install kafka-backup-operator oso/kafka-backup-operator \
  --namespace kafka-backup-system \
  --set azureWorkloadIdentity.enabled=true \
  --set azureWorkloadIdentity.clientId=$(az identity show -g myRG -n kafka-backup-identity --query clientId -o tsv)
```

See [docs/azure-workload-identity.md](docs/azure-workload-identity.md) for detailed setup instructions.

## Monitoring

The operator exposes Prometheus metrics on port 8080:

| Metric | Description |
|--------|-------------|
| `kafka_backup_reconciliations_total` | Total reconciliation attempts |
| `kafka_backup_reconcile_duration_seconds` | Reconciliation duration histogram |
| `kafka_backup_backups_total` | Total backups by status |
| `kafka_backup_backup_size_bytes` | Backup size in bytes |
| `kafka_backup_backup_records` | Records processed |
| `kafka_backup_restores_total` | Total restores by status |

### ServiceMonitor (Prometheus Operator)

```yaml
# Enable in Helm values
metrics:
  serviceMonitor:
    enabled: true
    interval: 30s
    labels:
      release: prometheus
```

## Disaster Recovery Workflow

1. **Normal Operation**: `KafkaBackup` runs on schedule, storing backups to cloud storage
2. **Disaster Occurs**: Kafka cluster fails or data is corrupted
3. **Recovery**:
   - Identify the backup to restore from: `kubectl get kafkabackup my-backup -o yaml`
   - Create a `KafkaRestore` resource pointing to the backup
   - Monitor progress: `kubectl get kafkarestore -w`
4. **Post-Recovery**: Optionally reset consumer offsets with `KafkaOffsetReset`

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              kafka-backup-operator                        │  │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐ │  │
│  │  │   Backup    │ │   Restore   │ │   Offset Reset/     │ │  │
│  │  │ Controller  │ │ Controller  │ │   Rollback Ctrl     │ │  │
│  │  └──────┬──────┘ └──────┬──────┘ └──────────┬──────────┘ │  │
│  │         │               │                    │            │  │
│  │         └───────────────┼────────────────────┘            │  │
│  │                         │                                 │  │
│  │                         ▼                                 │  │
│  │              ┌─────────────────────┐                      │  │
│  │              │  kafka-backup-core  │                      │  │
│  │              │     (Rust lib)      │                      │  │
│  │              └─────────────────────┘                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              ▼                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Kafka Cluster                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Cloud Storage                            │
│      ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│      │   S3     │    │  Azure   │    │   GCS    │              │
│      │          │    │   Blob   │    │          │              │
│      └──────────┘    └──────────┘    └──────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/osodevops/kafka-backup-operator.git
cd kafka-backup-operator

# Build
cargo build --release

# Generate CRDs
cargo run --bin crdgen > deploy/crds/all.yaml

# Run tests
cargo test
```

### Local Development with Minikube

See [minikube/README.md](minikube/README.md) for local development setup with Confluent for Kubernetes.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on the process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/osodevops/kafka-backup-operator/issues)
- **Discussions**: [GitHub Discussions](https://github.com/osodevops/kafka-backup-operator/discussions)

## Related Projects

- [kafka-backup-core](https://github.com/osodevops/kafka-backup) - The core backup library
- [OSO DevOps Helm Charts](https://github.com/osodevops/helm-charts) - Helm chart repository
