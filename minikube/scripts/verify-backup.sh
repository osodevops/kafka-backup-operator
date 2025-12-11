#!/bin/bash
set -e

echo "============================================"
echo "Kafka Backup Verification"
echo "============================================"

echo ""
echo "1. Checking KafkaBackup status..."
echo "-----------------------------------"
kubectl get kafkabackup -n confluent -o wide

echo ""
echo "2. Detailed KafkaBackup status..."
echo "-----------------------------------"
kubectl describe kafkabackup backup-test-topic -n confluent 2>/dev/null || echo "KafkaBackup not found"

echo ""
echo "3. Checking Kafka Backup Operator logs..."
echo "-------------------------------------------"
kubectl logs deployment/kafka-backup-operator -n kafka-backup-system --tail=50 2>/dev/null || echo "Operator not ready"

echo ""
echo "4. Checking backup storage PVC..."
echo "-----------------------------------"
kubectl get pvc kafka-backup-storage -n confluent 2>/dev/null || echo "PVC not found"

echo ""
echo "5. Checking topic exists in Kafka..."
echo "--------------------------------------"
kubectl exec kafka-0 -n confluent -- kafka-topics --bootstrap-server kafka:9071 --list 2>/dev/null || echo "Cannot connect to Kafka"

echo ""
echo "6. Counting messages in backup-test-topic..."
echo "----------------------------------------------"
kubectl exec kafka-0 -n confluent -- kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9071 \
  --topic backup-test-topic 2>/dev/null || echo "Cannot get offsets"

echo ""
echo "7. Viewing backup files (if any)..."
echo "--------------------------------------"
# Create a temporary pod to check the backup storage
kubectl run backup-checker --rm -i --restart=Never --image=busybox -n confluent -- \
  sh -c 'ls -la /backup 2>/dev/null || echo "No backup directory found"' \
  --overrides='{"spec":{"containers":[{"name":"backup-checker","image":"busybox","command":["sh","-c","ls -laR /backup 2>/dev/null || echo No backup files yet"],"volumeMounts":[{"name":"backup-storage","mountPath":"/backup"}]}],"volumes":[{"name":"backup-storage","persistentVolumeClaim":{"claimName":"kafka-backup-storage"}}]}}' 2>/dev/null || echo "Could not check backup files"

echo ""
echo "============================================"
echo "Verification complete!"
echo "============================================"
