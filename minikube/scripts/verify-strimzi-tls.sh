#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIKUBE_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "Strimzi TLS E2E Verification"
echo "============================================"
echo ""
echo "This verifies the split caSecret/tlsSecret"
echo "feature works end-to-end with Strimzi."
echo ""

echo "1. Checking Strimzi secrets (split CA + client certs)..."
echo "----------------------------------------------------------"
echo "Cluster CA secret (my-cluster-cluster-ca-cert):"
kubectl get secret my-cluster-cluster-ca-cert -n kafka -o jsonpath='{.data}' 2>/dev/null | python3 -c "import sys,json; print('  Keys:', list(json.load(sys.stdin).keys()))" 2>/dev/null || echo "  NOT FOUND"

echo "KafkaUser secret (backup-user):"
kubectl get secret backup-user -n kafka -o jsonpath='{.data}' 2>/dev/null | python3 -c "import sys,json; print('  Keys:', list(json.load(sys.stdin).keys()))" 2>/dev/null || echo "  NOT FOUND"

echo ""
echo "2. Checking KafkaBackup status..."
echo "-----------------------------------"
kubectl get kafkabackup -n kafka -o wide 2>/dev/null || echo "No KafkaBackup resources found"

echo ""
echo "3. Detailed KafkaBackup status..."
echo "-----------------------------------"
kubectl describe kafkabackup backup-test-topic -n kafka 2>/dev/null || echo "KafkaBackup not found"

echo ""
echo "4. Checking operator logs for TLS errors..."
echo "----------------------------------------------"
OPERATOR_POD=$(kubectl get pod -l app.kubernetes.io/name=kafka-backup-operator -n kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$OPERATOR_POD" ]; then
    echo "Last 30 lines of operator logs:"
    kubectl logs "$OPERATOR_POD" -n kafka --tail=30 2>/dev/null
    echo ""
    echo "TLS-related errors (if any):"
    kubectl logs "$OPERATOR_POD" -n kafka 2>/dev/null | grep -i -E "tls|ssl|cert|secret|handshake" | tail -10 || echo "  None found"
else
    echo "Operator pod not found"
fi

echo ""
echo "5. Checking topics in Kafka..."
echo "---------------------------------"
kubectl exec my-cluster-kafka-0 -n kafka -c kafka -- \
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || echo "Cannot connect to Kafka"

echo ""
echo "6. Counting messages in backup-test-topic..."
echo "-----------------------------------------------"
kubectl exec my-cluster-kafka-0 -n kafka -c kafka -- \
    bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic backup-test-topic 2>/dev/null || echo "Cannot get offsets"

echo ""
echo "7. Checking backup storage..."
echo "-------------------------------"
kubectl run backup-checker --rm -i --restart=Never --image=busybox -n kafka \
    --overrides='{"spec":{"containers":[{"name":"backup-checker","image":"busybox","command":["sh","-c","echo \"Backup files:\" && ls -laR /backup 2>/dev/null || echo \"No backup files yet\""],"volumeMounts":[{"name":"backup-storage","mountPath":"/backup"}]}],"volumes":[{"name":"backup-storage","persistentVolumeClaim":{"claimName":"kafka-backup-storage"}}]}}' 2>/dev/null || echo "Could not check backup files"

echo ""
echo "8. Checking KafkaRestore status (if applied)..."
echo "--------------------------------------------------"
kubectl get kafkarestore -n kafka -o wide 2>/dev/null || echo "No KafkaRestore resources found"
kubectl describe kafkarestore restore-test -n kafka 2>/dev/null || true

echo ""
echo "9. Checking restored topic (if restore completed)..."
echo "------------------------------------------------------"
kubectl exec my-cluster-kafka-0 -n kafka -c kafka -- \
    bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic restored-topic 2>/dev/null || echo "Restored topic not found yet"

echo ""
echo "============================================"
echo "Verification complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  - If backup completed, apply the restore:"
echo "    kubectl apply -f $MINIKUBE_DIR/overlays/strimzi-tls/kafka-restore.yaml"
echo "  - Then re-run this script to check restore status"
