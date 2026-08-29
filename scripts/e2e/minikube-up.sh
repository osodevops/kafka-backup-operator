#!/usr/bin/env bash
# Dedicated minikube profile with Strimzi Kafka (plain listener, no auth), a
# seeded topic, and the operator namespace + backup PVC. Idempotent.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
mkdir -p "$E2E_DIR"
[ -f "$E2E_DIR/prev-context" ] || kubectl config current-context > "$E2E_DIR/prev-context" 2>/dev/null || true
if ! minikube status -p "$PROFILE" >/dev/null 2>&1; then
  minikube start -p "$PROFILE" --driver=docker --cpus=4 --memory=8192 --kubernetes-version=v1.33.1
  kubectl config use-context "$(cat "$E2E_DIR/prev-context")" >/dev/null 2>&1 || true
fi
helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
helm repo update strimzi >/dev/null
h upgrade --install strimzi strimzi/strimzi-kafka-operator --version 0.46.1 -n "$NS_KAFKA" --create-namespace \
  --set "watchNamespaces={$NS_KAFKA}" --wait --timeout 5m >/dev/null
k apply -f "$ROOT/manifests/e2e/kafka.yaml" >/dev/null
k -n "$NS_KAFKA" wait kafka/my-cluster --for=condition=Ready --timeout=10m
k -n "$NS_KAFKA" wait kafkatopic/backup-test-topic --for=condition=Ready --timeout=3m
k get ns "$NS_OP" >/dev/null 2>&1 || k create ns "$NS_OP" >/dev/null
k apply -f "$ROOT/manifests/e2e/backup-pvc.yaml" >/dev/null
k apply -f "$ROOT/deploy/crds/all.yaml" >/dev/null
k -n "$NS_KAFKA" get job seed-records >/dev/null 2>&1 || k apply -f "$ROOT/manifests/e2e/producer-job.yaml" >/dev/null
k -n "$NS_KAFKA" wait job/seed-records --for=condition=complete --timeout=10m >/dev/null
echo "stack ready on profile $PROFILE"
