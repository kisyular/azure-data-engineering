# Azure Data Engineering Project - Setup Guide

> Step-by-step guide to set up Azure resources for a data engineering project.

---

## Prerequisites

- Azure CLI installed (`az --version` to verify)
- Azure account (Free tier or Student subscription works)
- Terminal/Bash shell

---

## Step 0: Load Environment Variables

**IMPORTANT:** Before running any commands, load your environment variables from `.env` file.

### For Bash/Zsh users

```bash
# Load environment variables from .env file
# Run this at the start of EVERY terminal session
set -a  # automatically export all variables
source .env
set +a  # disable auto-export

# Verify variables are loaded
echo "Resource Group: $AZURE_RESOURCE_GROUP"
echo "Storage Account: $AZURE_STORAGE_ACCOUNT_NAME"
echo "SQL Server: $AZURE_SQL_SERVER_NAME"
```

### For Fish shell users

Fish shell doesn't support bash `.env` syntax. Use this helper function:

```bash
# Option 1: One-liner to load .env (run each time)
for line in (cat .env | grep -v '^#' | grep '=')
    set -gx (echo $line | cut -d '=' -f1) (echo $line | cut -d '=' -f2- | tr -d '"')
end

# Verify variables are loaded
echo "Resource Group: $AZURE_RESOURCE_GROUP"
echo "Storage Account: $AZURE_STORAGE_ACCOUNT_NAME"
echo "SQL Server: $AZURE_SQL_SERVER_NAME"
```

```bash
# Option 2: Add this function to ~/.config/fish/config.fish for permanent use
function loadenv
    for line in (cat .env | grep -v '^#' | grep '=')
        set -gx (echo $line | cut -d '=' -f1) (echo $line | cut -d '=' -f2- | tr -d '"')
    end
    echo "Loaded .env variables"
end

# Then just run: loadenv
```

```fish
# Option 3: Use bass plugin (if installed) to run bash commands
# Install: fisher install edc/bass
bass source .env
```

> **Note:** The `-gx` flag in fish means global and exported (available to subprocesses).

---

## Step 1: Login to Azure CLI

```bash
# Login to Azure (opens browser for authentication)
az login
```

**What happens:**

- Opens your default browser
- You authenticate with your Microsoft/Azure account
- CLI stores credentials locally (~/.azure/)

```bash
# Verify you're logged in and see your subscriptions
az account show --output table

# If you have multiple subscriptions, set the one you want to use
# az account set --subscription "Your Subscription Name"
```

---

## Step 2: Create a Resource Group

**What is a Resource Group?**
A logical container that holds related Azure resources. Think of it as a folder for your project.

```bash
# Create Resource Group using environment variables
az group create \
    --name "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION"
```

**Expected output:**

```json
{
  "id": "/subscriptions/.../resourceGroups/azure-4-data-engineering-rg",
  "location": "eastus",
  "name": "azure-4-data-engineering-rg",
  "properties": {
    "provisioningState": "Succeeded"
  }
}
```

```bash
# Verify creation
az group show --name "$AZURE_RESOURCE_GROUP" --output table
```

---

## Step 3: Create a Storage Account

**What is a Storage Account?**
Central storage for all data types - blobs, files, queues, tables. This will be our Data Lake.

```bash
# Create Storage Account with Data Lake Gen2 enabled (--hns true)
az storage account create \
    --name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION" \
    --sku "Standard_LRS" \
    --kind "StorageV2" \
    --hns true \
    --access-tier "Hot" \
    --allow-blob-public-access false \
    --min-tls-version "TLS1_2"
```

**Flag explanations:**

| Flag | Value | Purpose |
| ------ | ------- | --------- |
| `--sku` | Standard_LRS | Locally redundant storage (cheapest) |
| `--kind` | StorageV2 | General-purpose v2 (recommended) |
| `--hns` | true | Hierarchical namespace = Data Lake Gen2 |
| `--access-tier` | Hot | Frequently accessed data |
| `--allow-blob-public-access` | false | Security: no anonymous access |
| `--min-tls-version` | TLS1_2 | Security: enforce TLS 1.2 |

```bash
# Verify creation
az storage account show \
    --name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

### Get Storage Account Key (for later use)

```bash
# Get the storage account key and save to .env
STORAGE_KEY=$(az storage account keys list \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query "[0].value" -o tsv)

echo "Storage Key: $STORAGE_KEY"
echo "Add this to your .env file as AZURE_STORAGE_ACCOUNT_KEY"
```

for fish shell users:

```bash
set STORAGE_KEY (az storage account keys list \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query "[0].value" -o tsv)
echo "Storage Key: $STORAGE_KEY"
echo "Add this to your .env file as AZURE_STORAGE_ACCOUNT_KEY"
```

---

## Step 4: Create a Data Factory

**What is Data Factory?**
Azure's ETL/ELT orchestration service. It moves and transforms data between sources.

```bash
# Create Data Factory
az datafactory create \
    --name "$AZURE_DATA_FACTORY_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION"
```

> **Note:** Data Factory creation takes 1-2 minutes.

```bash
# Verify creation
az datafactory show \
    --name "$AZURE_DATA_FACTORY_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

**Access Data Factory Studio:**

```url
https://adf.azure.com/home?factory=/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.DataFactory/factories/<adf-name>
```

Or simply search "Data Factory" in Azure Portal and click on your factory.

---

## Step 5: Create Storage Containers (Medallion Architecture)

**Medallion Architecture:**

- `landing` - Raw data arrives here first
- `bronze` - Raw data copy (unchanged from source)
- `silver` - Cleaned and validated data
- `gold` - Business-ready aggregations

```bash
# Create all containers for medallion architecture
for container in landing bronze silver gold; do
    echo "Creating container: $container"
    az storage container create \
        --name "$container" \
        --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
        --auth-mode login
done
```

> **Note:** If `--auth-mode login` gives permission errors, use `--auth-mode key` instead.
> The key method is simpler for learning but less secure for production.

```bash
# Alternative: Use key authentication (if login mode fails)
az storage container create \
    --name "landing" \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --account-key "$AZURE_STORAGE_ACCOUNT_KEY"
```

```bash
# Verify containers were created
az storage container list \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --auth-mode login \
    --output table
```

It should list:

```bash
Name     Lease Status    Last Modified
-------  --------------  -------------------------
bronze                   2026-02-01T05:03:01+00:00
gold                     2026-02-01T05:06:47+00:00
landing                  2026-02-01T05:06:56+00:00
silver                   2026-02-01T05:05:37+00:00
```

---

## Step 6: Upload Sample Data

```bash
# Upload sample data to the landing container
az storage blob upload \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --container-name "landing" \
    --name "data/customers.csv" \
    --file "data/customers.csv" \
    --auth-mode key \
    --overwrite
```

> **Note:** `--overwrite` allows re-uploading without errors (like S3 behavior).

```bash
# Verify upload
az storage blob list \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --container-name "landing" \
    --auth-mode key \
    --output table
```

### Upload Multiple Files

```bash
# Upload all CSV files from data directory
az storage blob upload-batch \
    --account-name "$AZURE_STORAGE_ACCOUNT_NAME" \
    --destination "landing" \
    --source "./data" \
    --pattern "*.csv" \
    --auth-mode key \
    --overwrite
```

---

## Step 7: Create Azure SQL Database

**Architecture:**

```architecture
SQL Server (logical server) → contains → SQL Database
          ↓
    Firewall Rules (who can connect)
```

### 7.1 Create SQL Server

```bash
# Create SQL Server (logical server)
# Using environment variables for credentials (secure!)
az sql server create \
    --name "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION_SQL" \
    --admin-user "$AZURE_SQL_DATABASE_USER" \
    --admin-password "$AZURE_SQL_DATABASE_PASSWORD"
```

> **Note:** We use `westus2` for SQL because Student subscriptions have region limitations.
> The server name becomes: `sql-server-4-data-engineering.database.windows.net`

```bash
# Verify SQL Server creation
az sql server show \
    --name "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

### 7.2 Create SQL Database

```bash
# Create SQL Database (Basic tier = $5/month, good for learning)
az sql db create \
    --name "$AZURE_SQL_DATABASE_NAME" \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --edition "Basic" \
    --capacity 5
```

**Pricing tiers:**

| Tier | DTU | Storage | Cost/Month | Use Case |
| ------ | ----- | --------- | ------------ | ---------- |
| Basic | 5 | 2 GB | ~$5 | Learning, dev |
| Standard S0 | 10 | 250 GB | ~$15 | Small apps |
| Standard S1 | 20 | 250 GB | ~$30 | Production |

```bash
# Verify database creation
az sql db show \
    --name "$AZURE_SQL_DATABASE_NAME" \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

### 7.3 Configure Firewall Rules

**IMPORTANT:** By default, SQL Server blocks ALL connections. You must configure firewall rules.

```bash
# Rule 1: Allow Azure services (required for Data Factory, Databricks, etc.)
az sql server firewall-rule create \
    --name "AllowAzureServices" \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0
```

> **Note:** `0.0.0.0` to `0.0.0.0` is a special range that means "Allow Azure services"

```bash
# Rule 2: Allow your current IP address (for local development)
MY_IP=$(curl -s ifconfig.me)
echo "Your IP: $MY_IP"

az sql server firewall-rule create \
    --name "AllowMyIP" \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --start-ip-address "$MY_IP" \
    --end-ip-address "$MY_IP"
```

```bash
# Verify firewall rules
az sql server firewall-rule list \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

### 7.4 Test SQL Connection

```bash
# Test connection using sqlcmd (if installed)
# sqlcmd -S "$AZURE_SQL_SERVER_NAME.database.windows.net" \
#        -d "$AZURE_SQL_DATABASE_NAME" \
#        -U "$AZURE_SQL_DATABASE_USER" \
#        -P "$AZURE_SQL_DATABASE_PASSWORD"

# Or use Azure Portal → SQL Database → Query Editor
echo "Connection string for your apps:"
echo "$AZURE_SQL_CONNECTION_STRING"
```

---

## Step 8: Verify All Resources

```bash
# List all resources in the resource group
az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --output table
```

**Expected resources:**

| Name | Type |
| ------ | ------ |
| sa4dataengineering4rk | Storage Account |
| adf-4-data-engineering-rk | Data Factory |
| sql-server-4-data-engineering | SQL Server |
| sqldb-4-data-engineering | SQL Database |

---

## Cleanup (Optional)

**WARNING:** This deletes EVERYTHING in the resource group!

```bash
# Delete all resources (use with caution!)
# az group delete --name "$AZURE_RESOURCE_GROUP" --yes --no-wait
```

---

## Quick Reference - All Commands

```bash
# Load env vars (run first!)
set -a && source .env && set +a

# Login
az login

# Create resource group
az group create --name "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION"

# Create storage account
az storage account create --name "$AZURE_STORAGE_ACCOUNT_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION" --sku "Standard_LRS" --kind "StorageV2" --hns true

# Create containers
for c in landing bronze silver gold; do az storage container create --name "$c" --account-name "$AZURE_STORAGE_ACCOUNT_NAME" --auth-mode login; done

# Create data factory
az datafactory create --name "$AZURE_DATA_FACTORY_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION"

# Create SQL server & database
az sql server create --name "$AZURE_SQL_SERVER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --location "$AZURE_LOCATION_SQL" --admin-user "$AZURE_SQL_DATABASE_USER" --admin-password "$AZURE_SQL_DATABASE_PASSWORD"
az sql db create --name "$AZURE_SQL_DATABASE_NAME" --server "$AZURE_SQL_SERVER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --edition "Basic"

# Configure firewall
az sql server firewall-rule create --name "AllowAzureServices" --server "$AZURE_SQL_SERVER_NAME" --resource-group "$AZURE_RESOURCE_GROUP" --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
```

---

## Troubleshooting

### "AuthorizationFailed" error

```bash
# Check your current subscription
az account show

# List available subscriptions
az account list --output table

# Switch subscription
az account set --subscription "Your Subscription Name"
```

### "Storage account name already taken"

Storage account names must be globally unique. Add random characters:

```bash
AZURE_STORAGE_ACCOUNT_NAME="sa4dataeng$(date +%s | tail -c 5)"
```

### "SQL Server firewall blocking connection"

```bash
# Add your current IP
MY_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create \
    --name "AllowMyIP_$(date +%Y%m%d)" \
    --server "$AZURE_SQL_SERVER_NAME" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --start-ip-address "$MY_IP" \
    --end-ip-address "$MY_IP"
```

### "Environment variable not set"

```bash
# Verify .env is loaded
echo $AZURE_RESOURCE_GROUP

# If empty, reload
set -a && source .env && set +a
```

## Adding Data to Azure SQL Database

You can run SQL scripts against your Azure SQL Database using several methods:

### Option 1: VS Code with MS SQL Extension (Recommended)

1. **Install the extension**: Search for "SQL Server (mssql)" in VS Code extensions

2. **Connect to your database**:
   - Open Command Palette: `Cmd + Shift + P` (Mac) or `Ctrl + Shift + P` (Windows)
   - Type: `MS SQL: Connect`
   - Enter connection details:

     ```text
     Server:   sql-server-4-data-engineering.database.windows.net
     Database: sqldb-4-data-engineering
     Auth:     SQL Login
     User:     (from your .env file)
     Password: (from your .env file)
     ```

   - Save the connection profile when prompted (e.g., "AzureSpotifyDB")

3. **Run your SQL script**:
   - Open `sql/spotify_initial_load.sql`
   - `Cmd + Shift + P` → `MS SQL: Execute Query`
   - Select your saved connection
   - **Quick tip**: Right-click in the SQL file and select "Execute Query"

### Option 2: Azure Portal Query Editor

```text
Azure Portal → SQL Database → Query Editor → Login → Paste & Run
```

### Option 3: sqlcmd CLI

```bash
# Install sqlcmd (Mac)
brew install sqlcmd

# Run a SQL script
sqlcmd -S "$AZURE_SQL_SERVER_NAME.database.windows.net" \
       -d "$AZURE_SQL_DATABASE_NAME" \
       -U "$AZURE_SQL_DATABASE_USER" \
       -P "$AZURE_SQL_DATABASE_PASSWORD" \
       -i sql/spotify_initial_load.sql
```

### Option 4: Azure Data Studio

```bash
# Install on Mac
brew install --cask azure-data-studio
```

Then connect using the same credentials as Option 1.
