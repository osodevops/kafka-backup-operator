#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIKUBE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$MINIKUBE_DIR")"

echo "============================================"
echo "Kafka Backup Operator - Strimzi TLS E2E Test"
echo "============================================"
echo ""
echo "This test validates the split caSecret/tlsSecret"
echo "feature with Strimzi-managed TLS certificates."
echo ""

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
echo "Step 1: Installing Strimzi Operator via Helm..."
echo "-------------------------------------------------"
helm repo add strimzi https://strimzi.io/charts/ || true
helm repo update
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
    --namespace kafka \
    --set watchNamespaces="{kafka}" \
    --wait \
    --timeout 5m

echo ""
echo "Step 2: Installing Kafka Backup Operator CRDs..."
echo "-------------------------------------------------"
kubectl apply -f "$REPO_ROOT/deploy/crds/all.yaml"

echo ""
echo "Step 3: Building and loading operator image into minikube..."
echo "-------------------------------------------------------------"
eval $(minikube docker-env)
docker build -t ghcr.io/osodevops/kafka-backup-operator:latest -f "$REPO_ROOT/Dockerfile" "$REPO_ROOT"

echo ""
echo "Step 4: Installing Kafka Backup Operator via Helm..."
echo "-----------------------------------------------------"
helm upgrade --install kafka-backup-operator "$REPO_ROOT/deploy/helm/kafka-backup-operator" \
    --namespace kafka \
    --set image.pullPolicy=Never \
    --set securityContext.readOnlyRootFilesystem=false \
    --set extraVolumes[0].name=backup-storage \
    --set extraVolumes[0].persistentVolumeClaim.claimName=kafka-backup-storage \
    --set extraVolumeMounts[0].name=backup-storage \
    --set extraVolumeMounts[0].mountPath=/data/kafka-backup-storage \
    --wait \
    --timeout 2m

echo ""
echo "Step 5: Deploying Strimzi Kafka cluster + KafkaUser..."
echo "-------------------------------------------------------"
kubectl apply -k "$MINIKUBE_DIR/overlays/strimzi-tls"

echo ""
echo "Waiting for Zookeeper to be ready..."
kubectl wait kafka/my-cluster --for=condition=Ready -n kafka --timeout=600s 2>/dev/null || {
    echo "Waiting for Kafka pods directly..."
    kubectl wait --for=condition=Ready pod -l strimzi.io/name=my-cluster-zookeeper -n kafka --timeout=300s || true
    kubectl wait --for=condition=Ready pod -l strimzi.io/name=my-cluster-kafka -n kafka --timeout=300s || true
}

echo ""
echo "Step 6: Waiting for KafkaUser secret to be created by Strimzi..."
echo "------------------------------------------------------------------"
echo "Strimzi will generate the 'backup-user' secret with user.crt and user.key"
for i in $(seq 1 60); do
    if kubectl get secret backup-user -n kafka &>/dev/null; then
        echo "KafkaUser secret 'backup-user' is ready!"
        break
    fi
    echo "  Waiting for KafkaUser secret... ($i/60)"
    sleep 5
done

echo ""
echo "Step 7: Verifying Strimzi secrets exist..."
echo "--------------------------------------------"
echo "Cluster CA secret:"
kubectl get secret my-cluster-cluster-ca-cert -n kafka -o jsonpath='{.data}' | python3 -c "import sys,json; print(list(json.load(sys.stdin).keys()))" 2>/dev/null || echo "  NOT FOUND"

echo "KafkaUser secret:"
kubectl get secret backup-user -n kafka -o jsonpath='{.data}' | python3 -c "import sys,json; print(list(json.load(sys.stdin).keys()))" 2>/dev/null || echo "  NOT FOUND"

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "The KafkaBackup resource uses split secrets:"
echo "  caSecret:  my-cluster-cluster-ca-cert (Strimzi cluster CA)"
echo "  tlsSecret: backup-user (Strimzi KafkaUser client cert)"
echo ""
echo "To check the status of all components:"
echo "  kubectl get pods -n kafka"
echo ""
echo "To check the KafkaBackup status:"
echo "  kubectl get kafkabackup -n kafka"
echo "  kubectl describe kafkabackup backup-test-topic -n kafka"
echo ""
echo "To view producer logs:"
echo "  kubectl logs -f deployment/kafka-producer -n kafka"
echo ""
echo "To view operator logs:"
echo "  kubectl logs -f deployment/kafka-backup-operator -n kafka"
echo ""
echo "Once backup completes, run the restore test:"
echo "  kubectl apply -f $MINIKUBE_DIR/overlays/strimzi-tls/kafka-restore.yaml"
echo "  kubectl get kafkarestore -n kafka"
echo ""
echo "To verify the restore, consume from the restored topic:"
echo "  ./scripts/verify-strimzi-tls.sh"
