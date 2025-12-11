# Kafka Backup Operator - Minikube Testing Environment

This directory contains a complete local testing environment for validating the kafka-backup-operator using Minikube and Confluent for Kubernetes (CFK).

## Prerequisites

- [Minikube](https://minikube.sigs.k8s.io/docs/start/) installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed
- [kustomize](https://kubectl.docs.kubernetes.io/installation/kustomize/) (or kubectl with kustomize support)
- Docker or another container runtime
- Minimum 8GB RAM available for Minikube

## Quick Start

### 1. Start the Test Environment

```bash
cd minikube
./scripts/setup.sh
```

This script will:
1. Start Minikube (if not already running)
2. Install Confluent for Kubernetes CRDs
3. Deploy the Confluent Operator
4. Install Kafka Backup Operator CRDs and RBAC
5. Deploy the Kafka Backup Operator
6. Deploy Zookeeper and Kafka (single node, no auth)
7. Create a test topic (`backup-test-topic`)
8. Start a producer that continuously writes messages
9. Create a `KafkaBackup` resource to backup the test topic

### 2. Verify the Setup

```bash
# Check all pods are running
kubectl get pods -n confluent
kubectl get pods -n kafka-backup-system

# Check the KafkaBackup status
kubectl get kafkabackup -n confluent

# Run the verification script
./scripts/verify-backup.sh
```

### 3. Monitor the Producer

```bash
kubectl logs -f deployment/kafka-producer -n confluent
```

### 4. Consume Messages (Verify Data)

```bash
./scripts/consume-messages.sh
```

### 5. Teardown

```bash
./scripts/teardown.sh
```

Or for a complete reset:

```bash
minikube delete && minikube start --memory 8192 --cpus 4
```

## Directory Structure

```
minikube/
├── base/
│   ├── cfk-operator/          # Confluent for Kubernetes operator
│   │   ├── kustomization.yaml
│   │   ├── namespace.yaml
│   │   ├── serviceaccount.yaml
│   │   ├── clusterrole.yaml
│   │   ├── clusterrolebinding.yaml
│   │   └── deployment.yaml
│   └── confluent-platform/     # Kafka + Zookeeper components
│       ├── kustomization.yaml
│       ├── zookeeper.yaml
│       ├── kafka.yaml
│       └── kafka-topic.yaml
├── overlays/
│   └── test/                   # Test environment overlay
│       ├── kustomization.yaml
│       ├── producer.yaml
│       ├── backup-pvc.yaml
│       └── kafka-backup.yaml
├── scripts/
│   ├── setup.sh               # Full environment setup
│   ├── teardown.sh            # Environment cleanup
│   ├── verify-backup.sh       # Backup verification
│   └── consume-messages.sh    # View topic messages
└── README.md
```

## Test Scenario

The test environment creates:

1. **Kafka Cluster**: Single-node Kafka with PLAINTEXT (no authentication)
2. **Test Topic**: `backup-test-topic` with 3 partitions
3. **Producer**: Continuously produces JSON messages every 10 seconds (100 messages per batch)
4. **KafkaBackup**: Backs up the test topic to a PVC using zstd compression

### Sample Message Format

```json
{
  "id": 1,
  "timestamp": "2024-01-15T10:30:00Z",
  "message": "Test message 1",
  "source": "kafka-producer"
}
```

## Configuration

### Kafka Configuration

The test environment uses a simplified single-node Kafka setup with:
- No authentication (PLAINTEXT)
- Single replica (for local testing)
- Auto topic creation enabled

### KafkaBackup Configuration

The `kafka-backup.yaml` creates a backup with:
- **Storage**: PVC (`kafka-backup-storage`)
- **Compression**: zstd (level 3)
- **Checkpointing**: Enabled (30-second intervals)
- **Rate Limiting**: 2 concurrent partitions
- **Circuit Breaker**: Enabled (5 failure threshold)

## Troubleshooting

### Pods not starting

```bash
# Check pod events
kubectl describe pod <pod-name> -n confluent

# Check operator logs
kubectl logs deployment/confluent-operator -n confluent
kubectl logs deployment/kafka-backup-operator -n kafka-backup-system
```

### Kafka not ready

Kafka requires Zookeeper to be healthy first:

```bash
kubectl wait --for=condition=Ready pod/zookeeper-0 -n confluent --timeout=300s
kubectl wait --for=condition=Ready pod/kafka-0 -n confluent --timeout=300s
```

### Backup not running

Check the KafkaBackup status and operator logs:

```bash
kubectl describe kafkabackup backup-test-topic -n confluent
kubectl logs deployment/kafka-backup-operator -n kafka-backup-system -f
```

### Resource constraints

If pods are being OOMKilled or stuck pending, increase Minikube resources:

```bash
minikube delete
minikube start --memory 12288 --cpus 6
```

## Manual Testing

### Create a manual backup

```bash
# Trigger a backup by updating the resource
kubectl patch kafkabackup backup-test-topic -n confluent --type merge -p '{"spec":{"suspend":false}}'
```

### Check backup files

```bash
kubectl run backup-viewer --rm -it --restart=Never --image=busybox -n confluent \
  --overrides='{"spec":{"containers":[{"name":"backup-viewer","image":"busybox","command":["sh"],"stdin":true,"tty":true,"volumeMounts":[{"name":"backup-storage","mountPath":"/backup"}]}],"volumes":[{"name":"backup-storage","persistentVolumeClaim":{"claimName":"kafka-backup-storage"}}]}}'
```

### Test a restore

```bash
# First, create a KafkaRestore resource
cat <<EOF | kubectl apply -f -
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaRestore
metadata:
  name: restore-test
  namespace: confluent
spec:
  backupRef:
    name: backup-test-topic
  kafkaCluster:
    bootstrapServers:
      - kafka.confluent.svc.cluster.local:9071
    securityProtocol: PLAINTEXT
  topics:
    - backup-test-topic
  topicMapping:
    backup-test-topic: restored-topic
  dryRun: true
EOF

# Check restore status
kubectl get kafkarestore -n confluent
kubectl describe kafkarestore restore-test -n confluent
```

## Adding Authentication

To test with SASL authentication, create an overlay that includes credentials:

```yaml
# overlays/test-auth/kafka-credentials.yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: confluent
type: Opaque
stringData:
  plain-users.json: |
    {
      "kafka": "kafka-secret",
      "backup": "backup-secret"
    }
  plain.txt: |
    username=kafka
    password=kafka-secret
---
apiVersion: v1
kind: Secret
metadata:
  name: backup-kafka-credentials
  namespace: confluent
type: Opaque
stringData:
  username: backup
  password: backup-secret
```

Then update the Kafka and KafkaBackup resources to use SASL_PLAINTEXT.

## Contributing

When modifying this test environment:

1. Update the kustomization files if adding new resources
2. Update this README if changing the setup process
3. Test the full setup/teardown cycle before committing
