# GCS authentication with GKE Workload Identity

`storage.gcs.useWorkloadIdentity: true` tells the operator not to read a service-account JSON key. [kafka-backup-core](https://github.com/osodevops/kafka-backup) then leaves `service_account_path` unset and uses Application Default Credentials. On GKE that is the metadata server token for the GCP service account bound to the operator's Kubernetes service account.

Backups and restores both run in the operator process, so the binding is on the operator ServiceAccount, not on a separate Job.

`credentialsSecret` stays required when the flag is false. A backup that forgets its secret must not silently use the node identity. Setting the flag and a secret together is rejected.

## Bind the operator ServiceAccount

```bash
export PROJECT_ID="<project>"
export GSA_NAME="kafka-backup"
export NAMESPACE="kafka-backup-system"
export KSA_NAME="kafka-backup-operator"

gcloud iam service-accounts create "$GSA_NAME" --project "$PROJECT_ID"

# Incremental backups list and read existing objects. objectAdmin on this bucket is enough.
gcloud storage buckets add-iam-policy-binding "gs://my-kafka-backups" \
  --member "serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role "roles/storage.objectAdmin"

gcloud iam service-accounts add-iam-policy-binding \
  "${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project "$PROJECT_ID" \
  --role "roles/iam.workloadIdentityUser" \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]"
```

The cluster must already have Workload Identity enabled. Helm already creates the ServiceAccount; set the annotation:

```bash
helm upgrade --install kafka-backup-operator oso/kafka-backup-operator \
  --namespace "$NAMESPACE" \
  --set "serviceAccount.annotations.iam\.gke\.io/gcp-service-account=${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

## Backup

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: gcs-backup
  namespace: kafka
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka:9092
  topics:
    - orders
  storage:
    storageType: gcs
    gcs:
      bucket: my-kafka-backups
      prefix: production
      useWorkloadIdentity: true
  schedule: "0 0 2 * * *"
  stopAtCurrentOffsets: true
```

Use the same `gcs` block on a `KafkaRestore` (or omit `backupRef.storage` so the restore reads storage from the `KafkaBackup`).

Operator-managed `retention` is not implemented for GCS. Keep a bucket lifecycle rule if objects should expire.

## Upgrade

This adds a CRD field and stops requiring `credentialsSecret`. Apply the CRDs before upgrading the operator. Helm does not upgrade CRDs:

```bash
kubectl apply --server-side -f deploy/crds/all.yaml
```

Existing backups that set `credentialsSecret` and leave the flag unset keep using the JSON key.
