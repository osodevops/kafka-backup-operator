#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIKUBE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$MINIKUBE_DIR")"

echo "============================================"
echo "Kafka Backup Operator - Minikube Test Setup"
echo "============================================"

# Check if minikube is running
if ! minikube status &>/dev/null; then
    echo "Starting minikube..."
    minikube start --memory 8192 --cpus 4 --disk-size 20g --driver=docker
else
    echo "Minikube is already running"
fi

# Enable addons
echo "Enabling minikube addons..."
minikube addons enable storage-provisioner
minikube addons enable default-storageclass

echo ""
echo "Step 1: Adding Helm repositories..."
echo "------------------------------------"
helm repo add confluentinc https://packages.confluent.io/helm || true
helm repo update

echo ""
echo "Step 2: Installing Confluent for Kubernetes via Helm..."
echo "--------------------------------------------------------"
kubectl create namespace confluent --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
    --namespace confluent \
    --wait \
    --timeout 5m

echo ""
echo "Step 3: Installing Kafka Backup Operator CRDs..."
echo "-------------------------------------------------"
kubectl apply -f "$REPO_ROOT/deploy/crds/all.yaml"

echo ""
echo "Step 4: Building and loading operator image into minikube..."
echo "-------------------------------------------------------------"
# Build the image using minikube's Docker daemon
eval $(minikube docker-env)
docker build -t ghcr.io/osodevops/kafka-backup-operator:latest -f "$REPO_ROOT/Dockerfile" "$REPO_ROOT"

echo ""
echo "Step 5: Installing Kafka Backup Operator via Helm..."
echo "-----------------------------------------------------"
# Install using Helm with custom values for local testing
helm upgrade --install kafka-backup-operator "$REPO_ROOT/deploy/helm/kafka-backup-operator" \
    --namespace confluent \
    --set image.pullPolicy=Never \
    --set securityContext.readOnlyRootFilesystem=false \
    --set extraVolumes[0].name=backup-storage \
    --set extraVolumes[0].persistentVolumeClaim.claimName=kafka-backup-storage \
    --set extraVolumeMounts[0].name=backup-storage \
    --set extraVolumeMounts[0].mountPath=/data/kafka-backup-storage \
    --wait \
    --timeout 2m

echo ""
echo "Step 6: Deploying Confluent Platform (ZK + Kafka)..."
echo "-----------------------------------------------------"
kubectl apply -k "$MINIKUBE_DIR/overlays/test"

echo ""
echo "Waiting for Zookeeper to be ready..."
kubectl wait --for=condition=Ready pod/zookeeper-0 -n confluent --timeout=300s || true

echo ""
echo "Waiting for Kafka to be ready..."
kubectl wait --for=condition=Ready pod/kafka-0 -n confluent --timeout=300s || true

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "To check the status of all components:"
echo "  kubectl get pods -n confluent"
echo ""
echo "To check the KafkaBackup status:"
echo "  kubectl get kafkabackup -n confluent"
echo "  kubectl describe kafkabackup backup-test-topic -n confluent"
echo ""
echo "To view producer logs:"
echo "  kubectl logs -f deployment/kafka-producer -n confluent"
echo ""
echo "To view operator logs:"
echo "  kubectl logs -f deployment/kafka-backup-operator -n confluent"
echo ""
echo "To verify backup data:"
echo "  ./scripts/verify-backup.sh"
