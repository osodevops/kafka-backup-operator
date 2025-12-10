# Azure Workload Identity Setup for Kafka Backup Operator

This guide explains how to configure Azure Workload Identity for the Kafka Backup Operator, enabling secure, secretless authentication to Azure Blob Storage.

## Prerequisites

- AKS cluster with Workload Identity enabled
- Azure CLI installed and authenticated
- `kubectl` configured for your AKS cluster
- Helm 3.x installed

## Overview

Azure Workload Identity allows Kubernetes pods to authenticate to Azure services using federated identity tokens instead of storing credentials in Kubernetes secrets. This provides:

- **Enhanced security**: No secrets to rotate or manage
- **Short-lived tokens**: Automatic token refresh
- **Azure RBAC integration**: Fine-grained access control

## Setup Steps

### 1. Set Environment Variables

```bash
# Your Azure subscription and resource group
export SUBSCRIPTION_ID="<your-subscription-id>"
export RESOURCE_GROUP="<your-resource-group>"
export LOCATION="<azure-region>"  # e.g., westeurope

# AKS cluster details
export CLUSTER_NAME="<your-aks-cluster>"

# Identity and storage details
export IDENTITY_NAME="kafka-backup-operator-identity"
export STORAGE_ACCOUNT_NAME="<your-storage-account>"
export STORAGE_CONTAINER_NAME="kafka-backups"

# Kubernetes namespace where operator will be deployed
export NAMESPACE="kafka-backup-system"
export SERVICE_ACCOUNT_NAME="kafka-backup-operator"
```

### 2. Enable Workload Identity on AKS (if not already enabled)

```bash
# Enable OIDC issuer and workload identity on existing cluster
az aks update \
  --resource-group $RESOURCE_GROUP \
  --name $CLUSTER_NAME \
  --enable-oidc-issuer \
  --enable-workload-identity

# Get the OIDC issuer URL
export OIDC_ISSUER=$(az aks show \
  --resource-group $RESOURCE_GROUP \
  --name $CLUSTER_NAME \
  --query "oidcIssuerProfile.issuerUrl" \
  --output tsv)

echo "OIDC Issuer: $OIDC_ISSUER"
```

### 3. Create Azure Managed Identity

```bash
# Create user-assigned managed identity
az identity create \
  --resource-group $RESOURCE_GROUP \
  --name $IDENTITY_NAME \
  --location $LOCATION

# Get the identity client ID and principal ID
export IDENTITY_CLIENT_ID=$(az identity show \
  --resource-group $RESOURCE_GROUP \
  --name $IDENTITY_NAME \
  --query "clientId" \
  --output tsv)

export IDENTITY_PRINCIPAL_ID=$(az identity show \
  --resource-group $RESOURCE_GROUP \
  --name $IDENTITY_NAME \
  --query "principalId" \
  --output tsv)

echo "Identity Client ID: $IDENTITY_CLIENT_ID"
echo "Identity Principal ID: $IDENTITY_PRINCIPAL_ID"
```

### 4. Create Storage Account and Container

```bash
# Create storage account (if not exists)
az storage account create \
  --resource-group $RESOURCE_GROUP \
  --name $STORAGE_ACCOUNT_NAME \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access false

# Create container for backups
az storage container create \
  --account-name $STORAGE_ACCOUNT_NAME \
  --name $STORAGE_CONTAINER_NAME \
  --auth-mode login
```

### 5. Assign Storage Blob Data Contributor Role

```bash
# Get storage account resource ID
export STORAGE_ACCOUNT_ID=$(az storage account show \
  --resource-group $RESOURCE_GROUP \
  --name $STORAGE_ACCOUNT_NAME \
  --query "id" \
  --output tsv)

# Assign Storage Blob Data Contributor role to the managed identity
az role assignment create \
  --assignee-object-id $IDENTITY_PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ACCOUNT_ID

echo "Role assignment created for identity $IDENTITY_NAME on storage account $STORAGE_ACCOUNT_NAME"
```

### 6. Create Federated Credential

```bash
# Create the federated credential linking the Kubernetes SA to the Azure identity
az identity federated-credential create \
  --resource-group $RESOURCE_GROUP \
  --identity-name $IDENTITY_NAME \
  --name "kafka-backup-operator-fedcred" \
  --issuer $OIDC_ISSUER \
  --subject "system:serviceaccount:${NAMESPACE}:${SERVICE_ACCOUNT_NAME}" \
  --audience api://AzureADTokenExchange
```

### 7. Deploy Kafka Backup Operator with Helm

```bash
# Create namespace
kubectl create namespace $NAMESPACE

# Install/upgrade the operator with Workload Identity enabled
helm upgrade --install kafka-backup-operator \
  ./deploy/helm/kafka-backup-operator \
  --namespace $NAMESPACE \
  --set azureWorkloadIdentity.enabled=true \
  --set azureWorkloadIdentity.clientId=$IDENTITY_CLIENT_ID \
  --set serviceAccount.name=$SERVICE_ACCOUNT_NAME
```

### 8. Create KafkaBackup Resource with Workload Identity

```yaml
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: production-backup
  namespace: kafka
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
  topics:
    - orders
    - events
  storage:
    storageType: azure
    azure:
      container: kafka-backups
      accountName: <your-storage-account>
      prefix: production
      useWorkloadIdentity: true  # Enable Workload Identity authentication
      # Note: credentialsSecret is NOT required when useWorkloadIdentity is true
  schedule: "0 */6 * * *"  # Every 6 hours
  compression: zstd
```

## Verification

### Verify Service Account Annotation

```bash
kubectl get sa $SERVICE_ACCOUNT_NAME -n $NAMESPACE -o yaml
```

Expected output should include:
```yaml
metadata:
  annotations:
    azure.workload.identity/client-id: "<your-identity-client-id>"
```

### Verify Pod Labels

```bash
kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=kafka-backup-operator -o yaml
```

Expected output should include:
```yaml
metadata:
  labels:
    azure.workload.identity/use: "true"
```

### Verify Environment Variables

```bash
kubectl exec -it -n $NAMESPACE deployment/kafka-backup-operator -- env | grep AZURE
```

Expected output:
```
AZURE_CLIENT_ID=<identity-client-id>
AZURE_TENANT_ID=<tenant-id>
AZURE_FEDERATED_TOKEN_FILE=/var/run/secrets/azure/tokens/azure-identity-token
AZURE_AUTHORITY_HOST=https://login.microsoftonline.com/
```

### Test Backup

```bash
# Apply a test backup resource
kubectl apply -f - <<EOF
apiVersion: kafka.oso.sh/v1alpha1
kind: KafkaBackup
metadata:
  name: test-workload-identity
  namespace: kafka
spec:
  kafkaCluster:
    bootstrapServers:
      - kafka-bootstrap:9092
  topics:
    - test-topic
  storage:
    storageType: azure
    azure:
      container: $STORAGE_CONTAINER_NAME
      accountName: $STORAGE_ACCOUNT_NAME
      useWorkloadIdentity: true
EOF

# Check backup status
kubectl get kafkabackup test-workload-identity -n kafka -o yaml
```

## Troubleshooting

### Common Issues

1. **"AADSTS70021: No matching federated identity record found"**
   - Verify the federated credential subject matches exactly: `system:serviceaccount:<namespace>:<service-account-name>`
   - Check the OIDC issuer URL is correct

2. **"AuthorizationFailed" when accessing storage**
   - Verify the role assignment is correct
   - Ensure the identity has "Storage Blob Data Contributor" role
   - Wait a few minutes for RBAC propagation

3. **Environment variables not injected**
   - Verify the pod has label `azure.workload.identity/use: "true"`
   - Verify the service account has annotation `azure.workload.identity/client-id`
   - Check the Azure Workload Identity webhook is running: `kubectl get pods -n kube-system | grep azure-wi-webhook`

### Debug Commands

```bash
# Check webhook logs
kubectl logs -n kube-system -l app.kubernetes.io/name=azure-workload-identity-webhook

# Check operator logs
kubectl logs -n $NAMESPACE -l app.kubernetes.io/name=kafka-backup-operator

# Verify identity federation
az identity federated-credential list \
  --resource-group $RESOURCE_GROUP \
  --identity-name $IDENTITY_NAME
```

## Security Best Practices

1. **Principle of Least Privilege**: Only assign "Storage Blob Data Contributor" role scoped to the specific container if possible
2. **Network Security**: Use private endpoints for storage accounts in production
3. **Monitoring**: Enable Azure Monitor and Storage Analytics for audit logging
4. **Rotation**: While Workload Identity uses short-lived tokens, periodically review and audit access

## Migrating from Secret-Based Authentication

If you're migrating from credentials stored in Kubernetes secrets:

1. Set up Workload Identity using the steps above
2. Update your KafkaBackup resources to use `useWorkloadIdentity: true`
3. Remove the `credentialsSecret` field
4. Delete the old Kubernetes secrets containing storage keys
5. Consider revoking the storage account keys if no longer needed

```yaml
# Before (secret-based)
storage:
  storageType: azure
  azure:
    container: kafka-backups
    accountName: mystorageaccount
    credentialsSecret:
      name: azure-storage-creds
      accountKeyKey: AZURE_STORAGE_KEY

# After (Workload Identity)
storage:
  storageType: azure
  azure:
    container: kafka-backups
    accountName: mystorageaccount
    useWorkloadIdentity: true
```
