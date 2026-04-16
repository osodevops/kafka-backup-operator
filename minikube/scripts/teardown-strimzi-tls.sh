#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIKUBE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$MINIKUBE_DIR")"

echo "============================================"
echo "Tearing down Strimzi TLS E2E test env"
echo "============================================"

echo ""
echo "Deleting KafkaRestore (if exists)..."
kubectl delete -f "$MINIKUBE_DIR/overlays/strimzi-tls/kafka-restore.yaml" --ignore-not-found=true

echo ""
echo "Deleting test resources..."
kubectl delete -k "$MINIKUBE_DIR/overlays/strimzi-tls" --ignore-not-found=true

echo ""
echo "Deleting Kafka Backup Operator..."
helm uninstall kafka-backup-operator -n kafka 2>/dev/null || true
kubectl delete -f "$REPO_ROOT/deploy/crds/all.yaml" --ignore-not-found=true

echo ""
echo "Deleting Strimzi Operator..."
helm uninstall strimzi-kafka-operator -n kafka 2>/dev/null || true

echo ""
echo "Deleting namespace..."
kubectl delete namespace kafka --ignore-not-found=true

echo ""
echo "============================================"
echo "Teardown complete!"
echo "============================================"
echo ""
echo "To completely reset, run:"
echo "  minikube delete && minikube start --memory 8192 --cpus 4"
