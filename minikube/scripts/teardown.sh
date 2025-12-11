#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIKUBE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$MINIKUBE_DIR")"

echo "============================================"
echo "Tearing down Kafka Backup Operator test env"
echo "============================================"

echo ""
echo "Deleting test resources..."
kubectl delete -k "$MINIKUBE_DIR/overlays/test" --ignore-not-found=true

echo ""
echo "Deleting Kafka Backup Operator..."
kubectl delete -f "$REPO_ROOT/deploy/operator/deployment.yaml" --ignore-not-found=true
kubectl delete -f "$REPO_ROOT/deploy/rbac/rolebinding.yaml" --ignore-not-found=true
kubectl delete -f "$REPO_ROOT/deploy/rbac/role.yaml" --ignore-not-found=true
kubectl delete -f "$REPO_ROOT/deploy/rbac/serviceaccount.yaml" --ignore-not-found=true
kubectl delete -f "$REPO_ROOT/deploy/crds/all.yaml" --ignore-not-found=true
kubectl delete -f "$REPO_ROOT/deploy/operator/namespace.yaml" --ignore-not-found=true

echo ""
echo "Deleting Confluent Operator..."
kubectl delete -k "$MINIKUBE_DIR/base/cfk-operator" --ignore-not-found=true

echo ""
echo "============================================"
echo "Teardown complete!"
echo "============================================"
echo ""
echo "To completely reset, run:"
echo "  minikube delete && minikube start --memory 8192 --cpus 4"
