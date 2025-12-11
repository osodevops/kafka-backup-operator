#!/bin/bash
# Consume messages from the backup-test-topic to verify data

echo "Consuming messages from backup-test-topic..."
echo "Press Ctrl+C to stop"
echo ""

kubectl exec -it kafka-0 -n confluent -- kafka-console-consumer \
  --bootstrap-server kafka:9071 \
  --topic backup-test-topic \
  --from-beginning \
  --max-messages 10
