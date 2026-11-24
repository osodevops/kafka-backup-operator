# Kafka Backup Operator - Minikube Testing Environment

This directory contains a complete local testing environment for validating the kafka-backup-operator using Minikube and Confluent for Kubernetes (CFK).

## Prerequisites

For release upgrade testing with kind instead of Minikube, see
[Release upgrade and rollback test](#release-upgrade-and-rollback-test).

- [Minikube](https://minikube.sigs.k8s.io/docs/start/) installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed
- [kustomize](https://kubectl.docs.kubernetes.io/installation/kustomize/) (or kubectl with kustomize support)
- Docker or another container runtime
- Minimum 8GB RAM available for Minikube

## Release upgrade and rollback test

Requires Docker, kind, kubectl, Helm, Python 3.12+ and the previous release tag
in Git. Run from the repository root. The test uses an explicit kubeconfig,
creates its own namespace, and leaves resources and JSON evidence for diagnosis.

```bash
kind create cluster --name kbo-upgrade --image kindest/node:v1.34.0 \
  --kubeconfig /tmp/kbo-upgrade.kubeconfig
docker build -t kafka-backup-operator:upgrade-test .
docker pull ghcr.io/osodevops/kafka-backup-operator:1.9.1
docker pull apache/kafka:4.1.1
bash scripts/e2e/load-kind-images.sh kbo-upgrade kafka-backup-operator:upgrade-test \
  ghcr.io/osodevops/kafka-backup-operator:1.9.1 apache/kafka:4.1.1
kubectl --kubeconfig /tmp/kbo-upgrade.kubeconfig apply --server-side -f deploy/crds/all.yaml
KUBECONFIG=/tmp/kbo-upgrade.kubeconfig \
  python3 scripts/test_gcs_auth_admission.py --context kind-kbo-upgrade
python3 scripts/e2e/upgrade-compatibility.py \
  --kubeconfig /tmp/kbo-upgrade.kubeconfig --context kind-kbo-upgrade \
  --image kafka-backup-operator:upgrade-test --evidence .e2e/upgrade
kind delete cluster --name kbo-upgrade
```

The image loader imports only the node's native architecture, avoiding missing
manifest or attestation content with Docker's containerd image store. Run the
admission script before installing an operator, using the explicit test
kubeconfig, or uninstall the test release first to avoid update conflicts.

The test produces keyed UTF-8 records across three partitions and runs a real
backup, restore, validation, offset reset and snapshot rollback on v1.9.1. It upgrades the
CRDs and operator, checks that all five stored CR kinds keep their UID, spec and
status without replay, restores the old archive with the candidate, exercises
scheduled backups and validations, and checks leader handover with two replicas.
Finally, it rolls back the operator and restores a candidate archive with the
previous release. Restored keys, values, exact end offsets and consumer group
offsets must match. New records are appended after upgrade and downgrade;
incremental backups must resume the other version's SQLite checkpoint and
archive without duplicating existing records. A fixture supplies the offset
snapshot because automatic snapshot persistence is not implemented. Cloud storage authentication and TLS
require their separate integration environments.

## Quick Start

> **The scripts run `kubectl` and `helm` against your current kubectl context.** `setup.sh` only
> switches to `minikube` when it has to start the cluster; if minikube is already running and your
> context points elsewhere (e.g. a production cluster), everything is installed *there*. Check
> `kubectl config current-context` first, or use a minikube-only kubeconfig for the whole shell,
> which leaves `~/.kube/config` untouched:
>
> ```bash
> export KUBECONFIG="$HOME/.kube/minikube-only"
> minikube start          # writes and selects the minikube context in that file
> ./scripts/setup.sh
> ```

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

```text
minikube/
├── base/
│   ├── cfk-operator/          # Confluent for Kubernetes operator
│   │   ├── kustomization.yaml
│   │   ├── namespace.yaml
│   │   ├── serviceaccount.yaml
│   │   ├── clusterrole.yaml
│   │   ├── clusterrolebinding.yaml
│   │   └── deployment.yaml
│   ├── confluent-platform/     # Kafka + Zookeeper components
│   │   ├── kustomization.yaml
│   │   ├── zookeeper.yaml
│   │   ├── kafka.yaml
│   │   └── kafka-topic.yaml
│   └── strimzi-kafka/          # Strimzi Kafka with TLS + KafkaUser
│       ├── kustomization.yaml
│       ├── namespace.yaml
│       ├── kafka.yaml
│       ├── kafka-user.yaml
│       └── kafka-topic.yaml
├── overlays/
│   ├── test/                   # CFK PLAINTEXT test
│   │   ├── kustomization.yaml
│   │   ├── producer.yaml
│   │   ├── backup-pvc.yaml
│   │   └── kafka-backup.yaml
│   └── strimzi-tls/            # Strimzi mTLS test (split CA/client secrets)
│       ├── kustomization.yaml
│       ├── producer.yaml
│       ├── backup-pvc.yaml
│       ├── kafka-backup.yaml
│       └── kafka-restore.yaml
├── scripts/
│   ├── setup.sh               # CFK test setup
│   ├── teardown.sh            # CFK test cleanup
│   ├── verify-backup.sh       # CFK backup verification
│   ├── consume-messages.sh    # View topic messages
│   ├── setup-strimzi-tls.sh   # Strimzi TLS test setup
│   ├── teardown-strimzi-tls.sh # Strimzi TLS test cleanup
│   └── verify-strimzi-tls.sh  # Strimzi TLS verification
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

## Strimzi TLS E2E Test (Split Secrets)

This test validates the `caSecret` / `tlsSecret` split-secret feature with a real Strimzi cluster.

### What It Tests

Strimzi generates TLS certificates in separate Kubernetes secrets:

- **`my-cluster-cluster-ca-cert`** — the cluster CA certificate (`ca.crt`)
- **`backup-user`** — per-user client cert and key (`user.crt`, `user.key`)

The test deploys a `KafkaBackup` and `KafkaRestore` that reference these as separate secrets using the `caSecret` field, validating the full produce → backup → restore flow over mTLS.

### Running the Test

```bash
cd minikube

# 1. Setup: Strimzi + Kafka + operator + producer + backup
./scripts/setup-strimzi-tls.sh

# 2. Verify backup is running and using split secrets
./scripts/verify-strimzi-tls.sh

# 3. Once backup completes, apply the restore
kubectl apply -f overlays/strimzi-tls/kafka-restore.yaml

# 4. Verify restore completed to 'restored-topic'
./scripts/verify-strimzi-tls.sh

# 5. Teardown
./scripts/teardown-strimzi-tls.sh
```

### Test Flow

1. Strimzi operator creates a Kafka cluster with TLS listener on port 9093
2. Strimzi creates a `KafkaUser` (`backup-user`) with mTLS authentication and ACLs
3. A producer writes messages to `backup-test-topic` over mTLS
4. `KafkaBackup` resource uses split secrets (`caSecret` + `tlsSecret`) to backup the topic
5. `KafkaRestore` restores to `restored-topic` using the same split-secret config
6. Verification script checks message counts match between original and restored topics

## Contributing

When modifying this test environment:

1. Update the kustomization files if adding new resources
2. Update this README if changing the setup process
3. Test the full setup/teardown cycle before committing
