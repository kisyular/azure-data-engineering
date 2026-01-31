# Azure Data Engineering - Complete Guide

> A comprehensive guide to building end-to-end data engineering solutions using Azure Data Factory, Databricks, Unity Catalog, Delta Live Tables, and more.

---

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Project Architecture](#2-project-architecture)
- [3. Azure Fundamentals](#3-azure-fundamentals)
- [4. Azure Data Factory Tutorial](#4-azure-data-factory-tutorial)
- [5. Azure SQL Database as Source](#5-azure-sql-database-as-source)
- [6. Incremental Ingestion Pipelines](#6-incremental-ingestion-pipelines)
- [7. Looping Pipelines](#7-looping-pipelines)
- [8. Logic Apps with Azure Data Factory](#8-logic-apps-with-azure-data-factory)
- [9. Azure Databricks Tutorial](#9-azure-databricks-tutorial)
- [10. Databricks Unity Catalog](#10-databricks-unity-catalog)
- [11. Spark Streaming with Databricks Auto Loader](#11-spark-streaming-with-databricks-auto-loader)
- [12. PySpark Transformations & APIs](#12-pyspark-transformations--apis)
- [13. Metadata-Driven Pipelines with Jinja2](#13-metadata-driven-pipelines-with-jinja2)
- [14. Star Schema and Slowly Changing Dimensions (SCD)](#14-star-schema-and-slowly-changing-dimensions-scd)
- [15. Databricks Delta Live Tables (DLT)](#15-databricks-delta-live-tables-dlt)
- [16. Databricks Asset Bundles for CI/CD](#16-databricks-asset-bundles-for-cicd)
- [Conclusion](#conclusion)

---

## 1. Introduction

### What is Data Engineering?

**Simple Explanation:** Data Engineering is like being a plumber for data. Just as plumbers build pipes to move water from one place to another, data engineers build pipelines to move data from where it's created (sources) to where it's needed (destinations).

**Key Responsibilities:**

- **Extract** data from various sources (databases, APIs, files)
- **Transform** data into useful formats
- **Load** data into storage systems for analysis

### Why Azure for Data Engineering?

Azure provides a complete ecosystem of tools that work together seamlessly:

| Tool               | Purpose                 | Analogy                            |
| ------------------ | ----------------------- | ---------------------------------- |
| Azure Data Factory | Orchestration & ETL     | The conductor of an orchestra      |
| Azure SQL Database | Relational data storage | A well-organized filing cabinet    |
| Azure Databricks   | Big data processing     | A powerful calculator on steroids  |
| Unity Catalog      | Data governance         | The librarian who tracks all books |
| Delta Live Tables  | Declarative pipelines   | An autopilot for data flows        |

---

## 2. Project Architecture

### High-Level Architecture Diagram

```azure-data-engineering-project-structure
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AZURE DATA ENGINEERING PROJECT                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────┐
│              │    │                  │    │                  │    │             │
│   SOURCE     │───▶│  INGESTION       │───▶│  PROCESSING      │───▶│  SERVING    │
│   LAYER      │    │  LAYER           │    │  LAYER           │    │  LAYER      │
│              │    │                  │    │                  │    │             │
└──────────────┘    └──────────────────┘    └──────────────────┘    └─────────────┘
       │                    │                       │                      │
       ▼                    ▼                       ▼                      ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────┐
│ Azure SQL DB │    │ Azure Data       │    │ Azure Databricks │    │ Power BI    │
│ On-Prem DBs  │    │ Factory          │    │ - PySpark        │    │ Analytics   │
│ APIs         │    │ - Pipelines      │    │ - Delta Tables   │    │ ML Models   │
│ Files        │    │ - Triggers       │    │ - Unity Catalog  │    │             │
└──────────────┘    └──────────────────┘    └──────────────────┘    └─────────────┘
```

### Medallion Architecture

```medallion-structure
┌────────────────────────────────────────────────────────────────────────────────┐
│                            MEDALLION ARCHITECTURE                              │
│                                                                                │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌───────────┐  │
│  │             │      │             │      │             │      │           │  │
│  │   SOURCE    │─────▶│   BRONZE    │─────▶│   SILVER    │─────▶│   GOLD    │  │
│  │   (Raw)     │      │   (Landing) │      │   (Clean)   │      │  (Curated)│  │
│  │             │      │             │      │             │      │           │  │
│  └─────────────┘      └─────────────┘      └─────────────┘      └───────────┘  │
│        │                    │                    │                    │        │
│        ▼                    ▼                    ▼                    ▼        │
│   Raw data as-is      Exact copy of       Cleaned, filtered,    Business-      │
│   from sources        source data         validated data         ready data    │
│                       (append-only)       (deduplicated)        (aggregated)   │
└────────────────────────────────────────────────────────────────────────────────┘
```

**The Gold Refining Analogy:**

- **Bronze (Raw Ore):** You dig up everything - dirt, rocks, and gold mixed together
- **Silver (Refined):** You wash and filter out the obvious junk
- **Gold (Pure):** The final, pure product ready to be made into jewelry

---

## 3. Azure Fundamentals

### 3.1 Creating a Free Azure Account

**What You Get:**

- $200 credit for 30 days
- 12 months of free popular services
- 55+ always-free services

**Steps:**

1. Go to [azure.microsoft.com/free](https://azure.microsoft.com/free)
2. Click "Start free"
3. Sign in with Microsoft account
4. Verify with phone number
5. Add credit card (verification only)

### 3.2 Azure Portal Layout

```azure-portal-layout
┌────────────────────────────────────────────────────────────────────────────────┐
│  ┌──────────┐                              AZURE PORTAL                        │
│  │ Search   │  ← Search for any Azure service                                  │
│  └──────────┘                                                                  │
│  ┌─────────────────┐  ┌────────────────────────────────────────────────────┐   │
│  │  NAVIGATION     │  │              MAIN CONTENT AREA                     │   │
│  │  • Home         │  │   - Resource Groups                                │   │
│  │  • Dashboard    │  │   - Data Factory                                   │   │
│  │  • All Services │  │   - Databricks                                     │   │
│  │  • Resources    │  │   - Storage Accounts                               │   │
│  └─────────────────┘  └────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Resource Groups Explained

**Simple Explanation:** A Resource Group is like a folder on your computer - you put related resources together.

```resource-group
┌─────────────────────────────────────────────┐
│   Resource Group: "data-engineering-rg"     │
├─────────────────────────────────────────────┤
│   ┌─────────────┐   ┌─────────────┐         │
│   │ Data Factory│   │ Databricks  │         │
│   └─────────────┘   └─────────────┘         │
│   ┌─────────────┐   ┌─────────────┐         │
│   │ SQL Database│   │ Storage Acct│         │
│   └─────────────┘   └─────────────┘         │
└─────────────────────────────────────────────┘
```

### 3.4 Creating Resources via Azure CLI

```bash
# Login to Azure
az login

# Create Resource Group
az group create \
    --name "data-engineering-rg" \
    --location "eastus"
```

This will output

```bash
{
  "id": "/subscriptions/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/resourceGroups/data-engineering-rg",
  "location": "eastus",
  "managedBy": null,
  "name": "data-engineering-rg",
  "properties": {
    "provisioningState": "Succeeded"
  },
  "tags": null,
  "type": "Microsoft.Resources/resourceGroups"
}
```

```bash
# Create Storage Account
az storage account create \
    --name "sa4dataengdev001" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --sku "Standard_LRS"
```

This will output

```bash
{
  "accessTier": "Hot",
  "accountMigrationInProgress": null,
  "allowBlobPublicAccess": false,
  "allowCrossTenantReplication": false,
  "allowSharedKeyAccess": null,
  "allowedCopyScope": null,
  "azureFilesIdentityBasedAuthentication": null,
  "blobRestoreStatus": null,
  "creationTime": "2026-01-31T17:46:30.821908+00:00",
  "customDomain": null,
  "defaultToOAuthAuthentication": null,
  "dnsEndpointType": null,
  "enableExtendedGroups": null,
  "enableHttpsTrafficOnly": true,
  "enableNfsV3": null,
  "encryption": {
    "encryptionIdentity": null,
    "keySource": "Microsoft.Storage",
    "keyVaultProperties": null,
    "requireInfrastructureEncryption": null,
    "services": {
      "blob": {
        "enabled": true,
        "keyType": "Account",
        "lastEnabledTime": "2026-01-31T17:46:31.181285+00:00"
      },
      "file": {
        "enabled": true,
        "keyType": "Account",
        "lastEnabledTime": "2026-01-31T17:46:31.181285+00:00"
      },
      "queue": null,
      "table": null
    }
  },
  "extendedLocation": null,
  "failoverInProgress": null,
  "geoReplicationStats": null,
  "id": "/subscriptions/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/resourceGroups/data-engineering-rg/providers/Microsoft.Storage/storageAccounts/studentstorageacct001",
  "identity": null,
  "immutableStorageWithVersioning": null,
  "isHnsEnabled": null,
  "isLocalUserEnabled": null,
  "isSftpEnabled": null,
  "isSkuConversionBlocked": null,
  "keyCreationTime": {
    "key1": "2026-01-31T17:46:31.165665+00:00",
    "key2": "2026-01-31T17:46:31.165665+00:00"
  },
  "keyPolicy": null,
  "kind": "StorageV2",
  "largeFileSharesState": null,
  "lastGeoFailoverTime": null,
  "location": "eastus",
  "minimumTlsVersion": "TLS1_0",
  "name": "studentstorageacct001",
  "networkRuleSet": {
    "bypass": "AzureServices",
    "defaultAction": "Allow",
    "ipRules": [],
    "ipv6Rules": [],
    "resourceAccessRules": null,
    "virtualNetworkRules": []
  },
  "primaryEndpoints": {
    "blob": "https://studentstorageacct001.blob.core.windows.net/",
    "dfs": "https://studentstorageacct001.dfs.core.windows.net/",
    "file": "https://studentstorageacct001.file.core.windows.net/",
    "internetEndpoints": null,
    "microsoftEndpoints": null,
    "queue": "https://studentstorageacct001.queue.core.windows.net/",
    "table": "https://studentstorageacct001.table.core.windows.net/",
    "web": "https://studentstorageacct001.z13.web.core.windows.net/"
  },
  "primaryLocation": "eastus",
  "privateEndpointConnections": [],
  "provisioningState": "Succeeded",
  "publicNetworkAccess": null,
  "resourceGroup": "data-engineering-rg",
  "routingPreference": null,
  "sasPolicy": null,
  "secondaryEndpoints": null,
  "secondaryLocation": null,
  "sku": {
    "name": "Standard_LRS",
    "tier": "Standard"
  },
  "statusOfPrimary": "available",
  "statusOfSecondary": null,
  "storageAccountSkuConversionStatus": null,
  "tags": {},
  "type": "Microsoft.Storage/storageAccounts"
}
```

```bash
# Create Data Factory
az datafactory create \
    --name "adf-4-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --location "eastus"
```

This will output

```bash
{
  "additionalProperties": null,
  "createTime": "2026-01-31T17:51:44.433707+00:00",
  "eTag": "\"3f021a2b-0000-0100-0000-697e41300000\"",
  "encryption": {
    "identity": null,
    "keyName": null,
    "keyVersion": null,
    "vaultBaseUrl": null
  },
  "globalParameters": null,
  "id": "/subscriptions/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/resourceGroups/data-engineering-rg/providers/Microsoft.DataFactory/factories/adf-student-tutorial-001",
  "identity": {
    "principalId": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "tenantId": "YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY",
    "type": "SystemAssigned",
    "userAssignedIdentities": null
  },
  "location": "eastus",
  "name": "adf-student-tutorial-001",
  "provisioningState": "Succeeded",
  "publicNetworkAccess": null,
  "purviewConfiguration": null,
  "repoConfiguration": null,
  "resourceGroup": "data-engineering-rg",
  "tags": {},
  "type": "Microsoft.DataFactory/factories",
  "version": "2018-06-01"
}
```

### Naming Convention

```naming-convention
{resource-type}-{project}-{environment}-{region}-{instance}

Examples:
├── adf-dataeng-dev-eastus-001     (Data Factory)
├── sql-dataeng-dev-eastus-001     (SQL Database)
├── dbw-dataeng-dev-eastus-001     (Databricks)
└── st-dataeng-dev-eastus-001      (Storage Account)
```

---

## 4. Azure Data Factory Tutorial

### 4.1 What is Azure Data Factory?

**Simple Explanation:** Azure Data Factory (ADF) is like a postal service for your data. Just as the postal service picks up packages from one location and delivers them to another, ADF picks up data from source systems and delivers it to destination systems.

**Key Concepts for Beginners:**

```azure-data-factory-components
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AZURE DATA FACTORY COMPONENTS                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌──────────────┐                                                              │
│   │   PIPELINE   │  ← Container for activities (like a recipe)                  │
│   │   ┌────────┐ │                                                              │
│   │   │Activity│ │  ← Individual step/task (like an instruction in recipe)      │
│   │   └────────┘ │                                                              │
│   └──────────────┘                                                              │
│                                                                                 │
│   ┌──────────────┐                                                              │
│   │   DATASET    │  ← Definition of your data (what & where)                    │
│   └──────────────┘                                                              │
│           │                                                                     │
│           ▼                                                                     │
│   ┌──────────────┐                                                              │
│   │ LINKED       │  ← Connection info (how to connect)                          │
│   │ SERVICE      │                                                              │
│   └──────────────┘                                                              │
│                                                                                 │
│   ┌──────────────┐                                                              │
│   │   TRIGGER    │  ← When to run (schedule, event, manual)                     │
│   └──────────────┘                                                              │
│                                                                                 │
│   ┌──────────────┐                                                              │
│   │ INTEGRATION  │  ← Compute infrastructure (where processing happens)        │
│   │   RUNTIME    │                                                              │
│   └──────────────┘                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Creating Azure Data Factory

**Via Azure Portal:**

1. Search for "Data Factory" in Azure Portal
2. Click "Create"
3. Fill in the basics:
   - **Subscription:** Your subscription
   - **Resource Group:** data-engineering-rg
   - **Name:** adf-4-dataeng-dev-001
   - **Region:** East US
   - **Version:** V2

**Via Azure CLI:**

```bash
# Create Data Factory
az datafactory create \
    --name "adf-4-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --location "eastus"

# Verify creation
az datafactory show \
    --name "adf-4-dataeng-dev-001" \
    --resource-group "data-engineering-rg"
```

When you run the verification code `az datafactory show` it should output

```bash
{
  "additionalProperties": null,
  "createTime": "2026-01-31T17:51:44.433707+00:00",
  "eTag": "\"3f021a2b-0000-0100-0000-697e41300000\"",
  "encryption": {
    "identity": null,
    "keyName": null,
    "keyVersion": null,
    "vaultBaseUrl": null
  },
  "globalParameters": null,
  "id": "/subscriptions/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/resourceGroups/data-engineering-rg/providers/Microsoft.DataFactory/factories/adf-student-tutorial-001",
  "identity": {
    "principalId": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
    "tenantId": "BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB",
    "type": "SystemAssigned",
    "userAssignedIdentities": null
  },
  "location": "eastus",
  "name": "adf-student-tutorial-001",
  "provisioningState": "Succeeded",
  "publicNetworkAccess": null,
  "purviewConfiguration": null,
  "repoConfiguration": null,
  "resourceGroup": "data-engineering-rg",
  "tags": {},
  "type": "Microsoft.DataFactory/factories",
  "version": "2018-06-01"
}
```

### 4.3 Understanding the ADF Studio Interface

```adf-studio-interface
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ADF STUDIO INTERFACE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐                                                              │
│  │  AUTHOR       │ ← Create pipelines, datasets, dataflows                      │
│  ├───────────────┤                                                              │
│  │  MONITOR      │ ← Watch pipeline runs, debug issues                          │
│  ├───────────────┤                                                              │
│  │  MANAGE       │ ← Linked services, integration runtimes, triggers            │
│  └───────────────┘                                                              │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        PIPELINE CANVAS                                  │    │
│  │   ┌─────────┐      ┌───────────┐      ┌─────────┐                       │    │
│  │   │ Source  │─────▶│ Process   │─────▶│  Sink   │                       │    │
│  │   │  (Copy) │      │(Transform)|      │ (Store) │                       │    │
│  │   └─────────┘      └───────────┘      └─────────┘                       │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Pipeline Activities - Deep Dive

**Common Activity Types:**

| Activity             | Purpose                  | Use Case                                |
| -------------------- | ------------------------ | --------------------------------------- |
| **Copy Data**        | Move data between stores | SQL to Blob, API to Data Lake           |
| **Data Flow**        | Transform data visually  | Complex ETL transformations             |
| **Lookup**           | Read config/metadata     | Get table names, connection strings     |
| **ForEach**          | Loop over items          | Process multiple tables                 |
| **If Condition**     | Branching logic          | Run different paths based on conditions |
| **Execute Pipeline** | Call other pipelines     | Modular pipeline design                 |
| **Set Variable**     | Store values             | Pass data between activities            |
| **Web**              | Call REST APIs           | Trigger external services               |
| **Stored Procedure** | Run SQL procedures       | Execute database logic                  |

### 4.5 Creating Your First Pipeline

**Scenario:** Copy data from Azure SQL Database to Azure Data Lake Storage

```pipeline-architecture
┌────────────────────────────────────────────────────────────────────────────────┐
│                        FIRST PIPELINE ARCHITECTURE                             │
│                                                                                │
│   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐           │
│   │  Azure SQL   │────────▶│  Copy Data   │────────▶│  Data Lake   │           │
│   │  Database    │         │  Activity    │         │  Storage     │           │
│   │  (Source)    │         │              │         │  (Sink)      │           │
│   └──────────────┘         └──────────────┘         └──────────────┘           │
│         │                        │                        │                    │
│         ▼                        ▼                        ▼                    │
│   Linked Service:          Mapping:                Linked Service:             │
│   SQL Connection           Column mappings         ADLS Connection             │
│                            Data types                                          │
└────────────────────────────────────────────────────────────────────────────────┘
```

#### Step 1: Create Linked Services

```json
// Azure SQL Linked Service (JSON representation)
{
    "name": "ls_azure_sql_db",
    "type": "AzureSqlDatabase",
    "typeProperties": {
        "connectionString": "Server=tcp:sql-server.database.windows.net;Database=AdventureWorks;User ID=admin;Password=***;Encrypt=True;"
    }
}

// Azure Data Lake Storage Linked Service
{
    "name": "ls_adls_storage",
    "type": "AzureBlobFS",
    "typeProperties": {
        "url": "https://sa4dataengdev001.dfs.core.windows.net/"
    }
}
```

#### Step 2: Create Datasets

```json
// Source Dataset - SQL Table
{
    "name": "ds_sql_customers",
    "type": "AzureSqlTable",
    "linkedServiceName": "ls_azure_sql_db",
    "typeProperties": {
        "schema": "SalesLT",
        "table": "Customer"
    }
}

// Sink Dataset - Parquet file in Data Lake
{
    "name": "ds_adls_customers_parquet",
    "type": "Parquet",
    "linkedServiceName": "ls_adls_storage",
    "typeProperties": {
        "location": {
            "type": "AzureBlobFSLocation",
            "folderPath": "bronze/customers",
            "fileSystem": "raw"
        }
    }
}
```

#### Step 3: Create Pipeline

```json
{
    "name": "pl_copy_customers",
    "properties": {
        "activities": [
            {
                "name": "Copy Customers Data",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "ds_sql_customers",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "ds_adls_customers_parquet",
                        "type": "DatasetReference"
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": "SELECT * FROM SalesLT.Customer"
                    },
                    "sink": {
                        "type": "ParquetSink"
                    }
                }
            }
        ]
    }
}
```

### 4.6 Parameterizing Pipelines

**Why Parameterize?** Instead of creating separate pipelines for each table, create ONE reusable pipeline.

```parameterizing-pipelines
┌────────────────────────────────────────────────────────────────────────────────┐
│                         PARAMETERIZED PIPELINE                                 │
│                                                                                │
│   Parameters:                                                                  │
│   ┌─────────────────────────────────────────────────────────┐                  │
│   │  schema_name    = "SalesLT"                             │                  │
│   │  table_name     = "Customer"                            │                  │
│   │  output_folder  = "bronze/customers"                    │                  │
│   └─────────────────────────────────────────────────────────┘                  │
│                              │                                                 │
│                              ▼                                                 │
│   ┌──────────────────────────────────────────────────────┐                     │
│   │              Copy Data Activity                       │                    │
│   │   Source: @{pipeline().parameters.schema_name}.       │                    │
│   │           @{pipeline().parameters.table_name}         │                    │
│   │   Sink:   @{pipeline().parameters.output_folder}/     │                    │
│   └──────────────────────────────────────────────────────┘                     │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Pipeline with Parameters (JSON):**

```json
{
    "name": "pl_copy_generic_table",
    "properties": {
        "parameters": {
            "schema_name": {
                "type": "string",
                "defaultValue": "SalesLT"
            },
            "table_name": {
                "type": "string"
            },
            "output_folder": {
                "type": "string"
            }
        },
        "activities": [
            {
                "name": "Copy Table Data",
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "SELECT * FROM @{pipeline().parameters.schema_name}.@{pipeline().parameters.table_name}",
                            "type": "Expression"
                        }
                    },
                    "sink": {
                        "type": "ParquetSink"
                    }
                }
            }
        ]
    }
}
```

### 4.7 Triggers Explained

```trigger-types
┌────────────────────────────────────────────────────────────────────────────────┐
│                              TRIGGER TYPES                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  ┌─────────────────┐    Run at specific times (cron-style)                     │
│  │    SCHEDULE     │    Example: Every day at 2 AM                             │
│  │    TRIGGER      │    "0 0 2 * * *"                                          │
│  └─────────────────┘                                                           │
│                                                                                │
│  ┌─────────────────┐    Run when window of time completes                      │
│  │    TUMBLING     │    Example: Process hourly batches                        │
│  │    WINDOW       │    Good for: Incremental processing                       │
│  └─────────────────┘                                                           │
│                                                                                │
│  ┌─────────────────┐    Run when file arrives                                  │
│  │    EVENT        │    Example: New CSV in blob storage                       │
│  │    TRIGGER      │    Good for: Real-time ingestion                          │
│  └─────────────────┘                                                           │
│                                                                                │
│  ┌─────────────────┐    Run on demand (Debug, Test)                            │
│  │    MANUAL       │    Click "Trigger Now" or API call                        │
│  │    TRIGGER      │                                                           │
│  └─────────────────┘                                                           │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Schedule Trigger Example:**

```json
{
    "name": "tr_daily_ingestion",
    "type": "ScheduleTrigger",
    "typeProperties": {
        "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "startTime": "2024-01-01T02:00:00Z",
            "timeZone": "UTC"
        }
    },
    "pipelines": [
        {
            "pipelineReference": {
                "referenceName": "pl_copy_customers",
                "type": "PipelineReference"
            }
        }
    ]
}
```

### 4.8 Expression Language & Dynamic Content

**ADF uses expressions for dynamic values. Common patterns:**

```expression-cheat-sheet
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         EXPRESSION CHEAT SHEET                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  SYSTEM VARIABLES:                                                              │
│  ─────────────────                                                              │
│  @pipeline().RunId              → Unique pipeline run ID                        │
│  @pipeline().TriggerTime        → When the pipeline was triggered               │
│  @pipeline().parameters.name    → Get parameter value                           │
│  @activity('Copy1').output      → Output from activity named 'Copy1'            │
│                                                                                 │
│  DATE/TIME FUNCTIONS:                                                           │
│  ────────────────────                                                           │
│  @utcnow()                      → Current UTC timestamp                         │
│  @formatDateTime(utcnow(),'yyyy-MM-dd')  → Format: 2024-01-15                   │
│  @adddays(utcnow(),-1)          → Yesterday                                     │
│  @startOfMonth(utcnow())        → First day of current month                    │
│                                                                                 │
│  STRING FUNCTIONS:                                                              │
│  ─────────────────                                                              │
│  @concat('Hello', ' ', 'World') → "Hello World"                                 │
│  @replace('abc', 'b', 'x')      → "axc"                                         │
│  @split('a,b,c', ',')           → ["a", "b", "c"]                               │
│  @toLower('HELLO')              → "hello"                                       │
│                                                                                 │
│  CONDITIONAL:                                                                   │
│  ────────────                                                                   │
│  @if(equals(1,1),'yes','no')    → "yes"                                         │
│  @coalesce(null, 'default')     → "default"                                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Practical Example - Dynamic File Path:**

```dynamic-file-path
// Create a dynamic path: bronze/customers/2024/01/15/data.parquet
@concat(
    'bronze/',
    pipeline().parameters.table_name,
    '/',
    formatDateTime(utcnow(), 'yyyy'),
    '/',
    formatDateTime(utcnow(), 'MM'),
    '/',
    formatDateTime(utcnow(), 'dd'),
    '/data.parquet'
)
```

### 4.9 Monitoring & Debugging

```dashboard
┌────────────────────────────────────────────────────────────────────────────────┐
│                          MONITORING DASHBOARD                                  │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  Pipeline Runs:                                                                │
│  ┌──────────────┬────────────┬──────────┬──────────┬──────────────┐            │
│  │ Pipeline     │ Start Time │ Duration │ Status   │ Triggered By │            │
│  ├──────────────┼────────────┼──────────┼──────────┼──────────────┤            │
│  │ pl_copy_cust │ 02:00:00   │ 5m 23s   │ ✓ Success│ Schedule     │            │
│  │ pl_copy_prod │ 02:05:00   │ 12m 45s  │ ✓ Success│ Schedule     │            │
│  │ pl_copy_ord  │ 02:15:00   │ --       │ ✗ Failed │ Schedule     │            │
│  └──────────────┴────────────┴──────────┴──────────┴──────────────┘            │
│                                                                                │
│  Activity Runs (drill down):                                                   │
│  ┌──────────────┬──────────────┬──────────┬─────────────────────────┐          │
│  │ Activity     │ Input/Output │ Duration │ Error Message           │          │
│  ├──────────────┼──────────────┼──────────┼─────────────────────────┤          │
│  │ Lookup       │ {...}        │ 2s       │ --                      │          │
│  │ Copy Data    │ {...}        │ --       │ Connection timeout      │          │
│  └──────────────┴──────────────┴──────────┴─────────────────────────┘          │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Debug Tips:**

1. **Use Debug Mode:** Test pipelines without triggering
2. **Check Activity Outputs:** Click on activity → Output tab
3. **Review Error Messages:** Failed activities show detailed errors
4. **Use Set Variable:** Print intermediate values for debugging

---

## 5. Azure SQL Database as Source

### 5.1 What is Azure SQL Database?

**Simple Explanation:** Azure SQL Database is like having a powerful Excel spreadsheet in the cloud, but much more organized and capable of handling millions of rows without slowing down.

```azure-sql-database-concepts
┌────────────────────────────────────────────────────────────────────────────────┐
│                     AZURE SQL DATABASE CONCEPTS                                │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Traditional Database (On-Premises)     Azure SQL Database (Cloud)            │
│   ───────────────────────────────────    ──────────────────────────            │
│   • You manage the server               • Microsoft manages the server         │
│   • You handle updates/patches          • Auto-updates & patches               │
│   • You provision hardware              • Elastic scaling                      │
│   • You set up backups                  • Automatic backups (35 days)          │
│   • Limited by physical hardware        • Scale up/down on demand              │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Creating Azure SQL Database

**Via Azure Portal:**

1. Search "SQL databases" → Create
2. Configure:
   - **Database name:** sqldb-adventureworks
   - **Server:** Create new or use existing
   - **Compute + storage:** Basic (for learning)

**Via Azure CLI:**

```bash
# Create SQL Server (logical server)
az sql server create \
    --name "sql-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --admin-user "sqladmin" \
    --admin-password "YourSecurePassword123!"

# Create SQL Database
az sql db create \
    --name "sqldb-adventureworks" \
    --server "sql-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --edition "Basic"

# Configure firewall (allow Azure services)
az sql server firewall-rule create \
    --name "AllowAzureServices" \
    --server "sql-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0
```

### 5.3 Sample Data Setup

**Load AdventureWorks Sample Data:**

```sql
-- Create sample tables for our data engineering project

-- Customers Table
CREATE TABLE SalesLT.Customer (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Email NVARCHAR(100),
    Phone NVARCHAR(25),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);

-- Products Table
CREATE TABLE SalesLT.Product (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100),
    Category NVARCHAR(50),
    ListPrice DECIMAL(10,2),
    StandardCost DECIMAL(10,2),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);

-- Sales Orders Table
CREATE TABLE SalesLT.SalesOrder (
    SalesOrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT FOREIGN KEY REFERENCES SalesLT.Customer(CustomerID),
    OrderDate DATETIME,
    TotalAmount DECIMAL(10,2),
    Status NVARCHAR(20),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);

-- Insert sample data
INSERT INTO SalesLT.Customer (FirstName, LastName, Email, Phone)
VALUES
    ('John', 'Doe', 'john.doe@email.com', '555-0101'),
    ('Jane', 'Smith', 'jane.smith@email.com', '555-0102'),
    ('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103');
```

### 5.4 Connecting ADF to Azure SQL

**Create Linked Service in ADF:**

```json
{
    "name": "ls_azure_sql",
    "type": "AzureSqlDatabase",
    "typeProperties": {
        "connectionString": {
            "type": "SecureString",
            "value": "Server=tcp:sql-dataeng-dev-001.database.windows.net,1433;Database=sqldb-adventureworks;User ID=sqladmin;Password={your_password};Encrypt=True;Connection Timeout=30;"
        }
    }
}
```

**Best Practice - Use Azure Key Vault:**

```json
{
    "name": "ls_azure_sql_keyvault",
    "type": "AzureSqlDatabase",
    "typeProperties": {
        "connectionString": {
            "type": "AzureKeyVaultSecret",
            "store": {
                "referenceName": "ls_keyvault",
                "type": "LinkedServiceReference"
            },
            "secretName": "sql-connection-string"
        }
    }
}
```

---

## 6. Incremental Ingestion Pipelines

### 6.1 Why Incremental Loading?

```full-vs-incremental-load
┌────────────────────────────────────────────────────────────────────────────────┐
│                    FULL LOAD vs INCREMENTAL LOAD                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  FULL LOAD (Simple but Inefficient):                                           │
│  ────────────────────────────────────                                          │
│  Day 1: Copy 1,000,000 rows    [████████████████] 100%                         │
│  Day 2: Copy 1,000,100 rows    [████████████████] 100%  ← Re-copy everything!  │
│  Day 3: Copy 1,000,200 rows    [████████████████] 100%  ← Again!               │
│                                                                                │
│  Problem: Wasting time & money copying unchanged data                          │
│                                                                                │
│  INCREMENTAL LOAD (Efficient):                                                 │
│  ─────────────────────────────                                                 │
│  Day 1: Copy 1,000,000 rows    [████████████████] 100%  ← Initial load         │
│  Day 2: Copy 100 new rows      [█               ] 0.01% ← Only changes!        │
│  Day 3: Copy 100 new rows      [█               ] 0.01% ← Only changes!        │
│                                                                                │
│  Benefit: 99.99% reduction in data transfer                                    │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Watermark Pattern

**Simple Explanation:** A watermark is like a bookmark in a book. It tells you where you left off, so you don't have to start from the beginning.

```watermark-pattern
┌────────────────────────────────────────────────────────────────────────────────┐
│                         WATERMARK PATTERN                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  Watermark Table (tracks progress):                                            │
│  ┌─────────────┬─────────────────────┬─────────────────────┐                   │
│  │ Table Name  │ Last Watermark      │ Updated At          │                   │
│  ├─────────────┼─────────────────────┼─────────────────────┤                   │
│  │ Customer    │ 2024-01-14 23:59:59 │ 2024-01-15 02:00:00 │                   │
│  │ Product     │ 2024-01-14 23:59:59 │ 2024-01-15 02:05:00 │                   │
│  │ SalesOrder  │ 2024-01-14 23:59:59 │ 2024-01-15 02:10:00 │                   │
│  └─────────────┴─────────────────────┴─────────────────────┘                   │
│                                                                                │
│  How it works:                                                                 │
│  ─────────────                                                                 │
│  1. Read last watermark: 2024-01-14 23:59:59                                   │
│  2. Query source: WHERE ModifiedDate > '2024-01-14 23:59:59'                   │
│  3. Copy only new/modified records                                             │
│  4. Update watermark to max(ModifiedDate) from copied data                     │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 Creating Watermark Table

```sql
-- Create watermark tracking table
CREATE TABLE dbo.Watermark (
    TableName NVARCHAR(100) PRIMARY KEY,
    WatermarkValue DATETIME,
    LastUpdated DATETIME DEFAULT GETDATE()
);

-- Initialize watermarks for each table
INSERT INTO dbo.Watermark (TableName, WatermarkValue)
VALUES
    ('SalesLT.Customer', '1900-01-01'),
    ('SalesLT.Product', '1900-01-01'),
    ('SalesLT.SalesOrder', '1900-01-01');
```

### 6.4 Incremental Pipeline Design

```incremental-pipeline-design
┌────────────────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL INGESTION PIPELINE                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐  │
│   │  Lookup     │────▶│  Lookup     │────▶│  Copy Data  │────▶│  Stored     │  │
│   │  Old        │     │  New        │     │  (Delta)    │     │  Procedure  │  │
│   │  Watermark  │     │  Watermark  │     │             │     │  (Update    │  │
│   │             │     │             │     │             │     │  Watermark) │  │
│   └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘  │
│         │                   │                   │                   │          │
│         ▼                   ▼                   ▼                   ▼          │
│   Get last loaded     Get max(Modified    Copy WHERE           Update          │
│   timestamp from      Date) from source   Modified > old       watermark       │
│   watermark table                         AND <= new           table           │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Complete Pipeline JSON:**

```json
{
    "name": "pl_incremental_load",
    "properties": {
        "parameters": {
            "table_name": {"type": "string"},
            "schema_name": {"type": "string", "defaultValue": "SalesLT"}
        },
        "activities": [
            {
                "name": "Lookup_OldWatermark",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "SELECT WatermarkValue FROM dbo.Watermark WHERE TableName = '@{concat(pipeline().parameters.schema_name, '.', pipeline().parameters.table_name)}'",
                            "type": "Expression"
                        }
                    },
                    "dataset": {"referenceName": "ds_sql_generic", "type": "DatasetReference"}
                }
            },
            {
                "name": "Lookup_NewWatermark",
                "type": "Lookup",
                "dependsOn": [{"activity": "Lookup_OldWatermark", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "SELECT MAX(ModifiedDate) as NewWatermark FROM @{pipeline().parameters.schema_name}.@{pipeline().parameters.table_name}",
                            "type": "Expression"
                        }
                    },
                    "dataset": {"referenceName": "ds_sql_generic", "type": "DatasetReference"}
                }
            },
            {
                "name": "Copy_IncrementalData",
                "type": "Copy",
                "dependsOn": [{"activity": "Lookup_NewWatermark", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "SELECT * FROM @{pipeline().parameters.schema_name}.@{pipeline().parameters.table_name} WHERE ModifiedDate > '@{activity('Lookup_OldWatermark').output.firstRow.WatermarkValue}' AND ModifiedDate <= '@{activity('Lookup_NewWatermark').output.firstRow.NewWatermark}'",
                            "type": "Expression"
                        }
                    },
                    "sink": {"type": "ParquetSink"}
                }
            },
            {
                "name": "Update_Watermark",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [{"activity": "Copy_IncrementalData", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "storedProcedureName": "usp_UpdateWatermark",
                    "storedProcedureParameters": {
                        "TableName": {"value": {"value": "@{concat(pipeline().parameters.schema_name, '.', pipeline().parameters.table_name)}", "type": "Expression"}},
                        "NewWatermark": {"value": {"value": "@{activity('Lookup_NewWatermark').output.firstRow.NewWatermark}", "type": "Expression"}}
                    }
                }
            }
        ]
    }
}
```

### 6.5 Stored Procedure for Watermark Update

```sql
CREATE PROCEDURE usp_UpdateWatermark
    @TableName NVARCHAR(100),
    @NewWatermark DATETIME
AS
BEGIN
    UPDATE dbo.Watermark
    SET WatermarkValue = @NewWatermark,
        LastUpdated = GETDATE()
    WHERE TableName = @TableName;
END;
```

### 6.6 Adding Backfilling Feature

**What is Backfilling?** Sometimes you need to reload historical data. Backfilling allows you to specify a custom date range instead of using the watermark.

```add-backfilling-feature
┌────────────────────────────────────────────────────────────────────────────────┐
│                         BACKFILL PIPELINE FLOW                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Parameters:                                                                  │
│   ┌─────────────────────────────────────────┐                                  │
│   │  is_backfill     = true/false           │                                  │
│   │  backfill_start  = "2024-01-01"         │                                  │
│   │  backfill_end    = "2024-01-15"         │                                  │
│   └─────────────────────────────────────────┘                                  │
│                         │                                                      │
│                         ▼                                                      │
│   ┌─────────────────────────────────────────┐                                  │
│   │         IF CONDITION                    │                                  │
│   │    is_backfill == true?                 │                                  │
│   └────────────────┬────────────────────────┘                                  │
│            Yes     │     No                                                    │
│             ▼      │      ▼                                                    │
│   ┌────────────┐   │   ┌────────────┐                                          │
│   │ Use custom │   │   │ Use normal │                                          │
│   │ date range │   │   │ watermark  │                                          │
│   └────────────┘   │   └────────────┘                                          │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Pipeline with Backfill Logic:**

```json
{
    "name": "pl_incremental_with_backfill",
    "properties": {
        "parameters": {
            "table_name": {"type": "string"},
            "is_backfill": {"type": "bool", "defaultValue": false},
            "backfill_start": {"type": "string", "defaultValue": "1900-01-01"},
            "backfill_end": {"type": "string", "defaultValue": "9999-12-31"}
        },
        "activities": [
            {
                "name": "If_Backfill",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {
                        "value": "@equals(pipeline().parameters.is_backfill, true)",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "Copy_BackfillData",
                            "type": "Copy",
                            "typeProperties": {
                                "source": {
                                    "type": "AzureSqlSource",
                                    "sqlReaderQuery": "SELECT * FROM @{pipeline().parameters.table_name} WHERE ModifiedDate BETWEEN '@{pipeline().parameters.backfill_start}' AND '@{pipeline().parameters.backfill_end}'"
                                }
                            }
                        }
                    ],
                    "ifFalseActivities": [
                        {
                            "name": "Execute_IncrementalPipeline",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {"referenceName": "pl_incremental_load", "type": "PipelineReference"}
                            }
                        }
                    ]
                }
            }
        ]
    }
}
```

---

## 7. Looping Pipelines

### 7.1 Why Loop in Pipelines?

**Simple Explanation:** Imagine you need to copy data from 50 different tables. Without looping, you'd create 50 separate copy activities. With looping, you create ONE copy activity and run it 50 times with different parameters.

```looping-benefits
┌────────────────────────────────────────────────────────────────────────────────┐
│                    WITHOUT LOOPING vs WITH LOOPING                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  WITHOUT LOOPING (50 tables = 50 activities):                                  │
│  ──────────────────────────────────────────────                                │
│  ┌────────┐ ┌────────┐ ┌────────┐       ┌────────┐                             │
│  │ Copy   │ │ Copy   │ │ Copy   │  ...  │ Copy   │                             │
│  │ Table1 │ │ Table2 │ │ Table3 │       │Table50 │                             │
│  └────────┘ └────────┘ └────────┘       └────────┘                             │
│  Problem: Hard to maintain, duplicate logic                                    │
│                                                                                │
│  WITH LOOPING (50 tables = 1 ForEach activity):                                │
│  ───────────────────────────────────────────────                               │
│  ┌─────────────────────────────────────────────┐                               │
│  │  ForEach (items: [Table1, Table2, ... ])    │                               │
│  │  ┌────────────────────────────────────────┐ │                               │
│  │  │  Copy @{item().tableName}              │ │  ← Runs 50 times              │
│  │  └────────────────────────────────────────┘ │                               │
│  └─────────────────────────────────────────────┘                               │
│  Benefit: Single source of truth, easy maintenance                             │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 ForEach Activity Deep Dive

**Key Properties:**

| Property       | Description                         | Example                            |
| -------------- | ----------------------------------- | ---------------------------------- |
| `items`        | Array to iterate over               | `["Customer", "Product", "Order"]` |
| `isSequential` | Run items one by one or in parallel | `false` (parallel)                 |
| `batchCount`   | Max parallel executions (1-50)      | `20`                               |
| `activities`   | What to do for each item            | Copy, Execute Pipeline, etc.       |

```foreach-modes
┌────────────────────────────────────────────────────────────────────────────────┐
│                    FOREACH EXECUTION MODES                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  SEQUENTIAL (isSequential = true):                                             │
│  ─────────────────────────────────                                             │
│  Item 1 ────▶ Item 2 ────▶ Item 3 ────▶ Item 4 ────▶ Done                      │
│  (5 min)      (5 min)      (5 min)      (5 min)      Total: 20 min             │
│                                                                                │
│  PARALLEL (isSequential = false, batchCount = 4):                              │
│  ────────────────────────────────────────────────                              │
│  Item 1 ─┐                                                                     │
│  Item 2 ─┼────▶ All complete ────▶ Done                                        │
│  Item 3 ─┤      (5 min)            Total: 5 min                                │
│  Item 4 ─┘                                                                     │
│                                                                                │
│  Speedup: 4x faster with parallel execution!                                   │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Building a Looping Pipeline

#### Step 1: Create Metadata Table

```sql
-- Table to store list of tables to process
CREATE TABLE config.TableMetadata (
    TableId INT PRIMARY KEY IDENTITY(1,1),
    SchemaName NVARCHAR(50),
    TableName NVARCHAR(100),
    WatermarkColumn NVARCHAR(50),
    IsActive BIT DEFAULT 1,
    LoadOrder INT
);

-- Insert tables to process
INSERT INTO config.TableMetadata (SchemaName, TableName, WatermarkColumn, LoadOrder)
VALUES
    ('SalesLT', 'Customer', 'ModifiedDate', 1),
    ('SalesLT', 'Product', 'ModifiedDate', 2),
    ('SalesLT', 'SalesOrder', 'ModifiedDate', 3),
    ('SalesLT', 'OrderDetail', 'ModifiedDate', 4),
    ('SalesLT', 'Category', 'ModifiedDate', 5);
```

#### Step 2: Lookup + ForEach Pipeline

```metadata-driven-loop
┌────────────────────────────────────────────────────────────────────────────────┐
│                    METADATA-DRIVEN LOOPING PIPELINE                            │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌─────────────┐      ┌─────────────────────────────────────────────────────┐ │
│   │   Lookup    │─────▶│              ForEach                                │ │
│   │   (Get all  │      │  ┌─────────────────────────────────────────────┐    │ │
│   │   tables)   │      │  │  Execute Pipeline: pl_incremental_load      │    │ │
│   └─────────────┘      │  │  Parameters:                                │    │ │
│         │              │  │    schema_name: @{item().SchemaName}        │    │ │
│         ▼              │  │    table_name:  @{item().TableName}         │    │ │
│   Returns array:       │  └─────────────────────────────────────────────┘    │ │
│   [                    └─────────────────────────────────────────────────────┘ │
│     {SchemaName:                                                               │
│      "SalesLT",                                                                │
│      TableName:                                                                │
│      "Customer"},                                                              │
│     {...},                                                                     │
│     {...}                                                                      │
│   ]                                                                            │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Complete Pipeline JSON:**

```json
{
    "name": "pl_master_ingestion",
    "properties": {
        "activities": [
            {
                "name": "Lookup_TableList",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": "SELECT SchemaName, TableName, WatermarkColumn FROM config.TableMetadata WHERE IsActive = 1 ORDER BY LoadOrder"
                    },
                    "dataset": {"referenceName": "ds_sql_generic", "type": "DatasetReference"},
                    "firstRowOnly": false
                }
            },
            {
                "name": "ForEach_Table",
                "type": "ForEach",
                "dependsOn": [{"activity": "Lookup_TableList", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "items": {
                        "value": "@activity('Lookup_TableList').output.value",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 10,
                    "activities": [
                        {
                            "name": "Execute_IncrementalPipeline",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "pl_incremental_load",
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "schema_name": "@{item().SchemaName}",
                                    "table_name": "@{item().TableName}"
                                },
                                "waitOnCompletion": true
                            }
                        }
                    ]
                }
            }
        ]
    }
}
```

### 7.4 Nested Loops and Complex Patterns

**Scenario:** Load data from multiple source systems, each with multiple tables

```nested-foreach-pattern
┌────────────────────────────────────────────────────────────────────────────────┐
│                    NESTED FOREACH PATTERN                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌───────────────────────────────────────────────────────────────────────┐    │
│   │  ForEach SOURCE (Systems: [ERP, CRM, HR])                             │    │
│   │  ┌───────────────────────────────────────────────────────────────┐    │    │
│   │  │  ForEach TABLE (Tables from each source)                      │    │    │
│   │  │  ┌─────────────────────────────────────────────────────────┐  │    │    │
│   │  │  │  Copy Data Activity                                     │  │    │    │
│   │  │  │  Source: @{item().SourceSystem}.@{item().TableName}     │  │    │    │
│   │  │  └─────────────────────────────────────────────────────────┘  │    │    │
│   │  └───────────────────────────────────────────────────────────────┘    │    │
│   └───────────────────────────────────────────────────────────────────────┘    │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.5 Error Handling in Loops

```json
{
    "name": "ForEach_WithErrorHandling",
    "type": "ForEach",
    "typeProperties": {
        "items": "@activity('Lookup').output.value",
        "activities": [
            {
                "name": "TryLoad",
                "type": "ExecutePipeline",
                "typeProperties": {
                    "pipeline": {"referenceName": "pl_load_table", "type": "PipelineReference"}
                }
            },
            {
                "name": "LogError",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {"activity": "TryLoad", "dependencyConditions": ["Failed"]}
                ],
                "typeProperties": {
                    "storedProcedureName": "usp_LogError",
                    "storedProcedureParameters": {
                        "TableName": {"value": "@{item().TableName}"},
                        "ErrorMessage": {"value": "@{activity('TryLoad').error.message}"}
                    }
                }
            }
        ]
    }
}
```

---

## 8. Logic Apps with Azure Data Factory

### 8.1 What are Logic Apps?

**Simple Explanation:** Logic Apps is like IFTTT or Zapier for the cloud. It connects different services and automates workflows with a visual designer. Think of it as: "When THIS happens, do THAT."

```logic-apps-overview
┌────────────────────────────────────────────────────────────────────────────────┐
│                    LOGIC APPS OVERVIEW                                         │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  Common Use Cases with ADF:                                                    │
│  ─────────────────────────────                                                 │
│  ┌────────────┐        ┌─────────────┐        ┌──────────────┐                 │
│  │ ADF        │───────▶│ Logic App   │───────▶│ Send Email   │                 │
│  │ Pipeline   │ Trigger│             │ Action │ Notification │                 │
│  │ Completes  │        │             │        │              │                 │
│  └────────────┘        └─────────────┘        └──────────────┘                 │
│                                                                                │
│  ┌────────────┐        ┌─────────────┐        ┌────────────┐                   │
│  │ ADF        │───────▶│ Logic App   │───────▶│ Post to    │                   │
│  │ Pipeline   │        │             │        │ Slack/Teams│                   │
│  │ Fails      │        │             │        │            │                   │
│  └────────────┘        └─────────────┘        └────────────┘                   │
│                                                                                │
│  ┌────────────┐        ┌─────────────┐        ┌──────────────┐                 │
│  │ New File   │───────▶│ Logic App   │───────▶│ Trigger      │                 │
│  │ Arrives    │        │             │        │ ADF Pipeline │                │
│  └────────────┘        └─────────────┘        └──────────────┘                 │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Creating a Logic App for Pipeline Notifications

**Architecture:**

```notification-system-architecture
┌────────────────────────────────────────────────────────────────────────────────┐
│                    NOTIFICATION SYSTEM ARCHITECTURE                            │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   ┌───────────────┐                                                            │
│   │  ADF Pipeline │                                                            │
│   │  Completes    │                                                            │
│   └───────┬───────┘                                                            │
│           │                                                                    │
│           ▼                                                                    │
│   ┌───────────────┐      ┌───────────────┐      ┌───────────────┐              │
│   │  Web Activity │─────▶│  Logic App    │─────▶│  Send Email   │              │
│   │  (HTTP POST)  │      │  (Triggered)  │      │  via Outlook  │              │
│   └───────────────┘      └───────────────┘      └───────────────┘              │
│                                │                                               │
│                                ├─────────────────▶ Post to Teams               │
│                                │                                               │
│                                └─────────────────▶ Log to Database             │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

#### Step 1: Create Logic App via Azure CLI

```bash
# Create Logic App
az logic workflow create \
    --name "la-adf-notifications" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --definition @logic-app-definition.json
```

#### Step 2: Logic App Definition (Trigger: HTTP Request)

```json
{
    "definition": {
        "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
        "triggers": {
            "manual": {
                "type": "Request",
                "kind": "Http",
                "inputs": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "pipelineName": {"type": "string"},
                            "status": {"type": "string"},
                            "runId": {"type": "string"},
                            "message": {"type": "string"},
                            "triggerTime": {"type": "string"}
                        }
                    }
                }
            }
        },
        "actions": {
            "Condition_CheckStatus": {
                "type": "If",
                "expression": {
                    "and": [
                        {"equals": ["@triggerBody()?['status']", "Failed"]}
                    ]
                },
                "actions": {
                    "Send_FailureEmail": {
                        "type": "ApiConnection",
                        "inputs": {
                            "host": {"connection": {"name": "@parameters('$connections')['outlook']['connectionId']"}},
                            "method": "post",
                            "path": "/v2/Mail",
                            "body": {
                                "To": "data-team@company.com",
                                "Subject": "ADF Pipeline FAILED: @{triggerBody()?['pipelineName']}",
                                "Body": "<h2>Pipeline Failure Alert</h2><p><b>Pipeline:</b> @{triggerBody()?['pipelineName']}</p><p><b>Run ID:</b> @{triggerBody()?['runId']}</p><p><b>Error:</b> @{triggerBody()?['message']}</p>"
                            }
                        }
                    }
                },
                "else": {
                    "actions": {
                        "Send_SuccessEmail": {
                            "type": "ApiConnection",
                            "inputs": {
                                "host": {"connection": {"name": "@parameters('$connections')['outlook']['connectionId']"}},
                                "method": "post",
                                "path": "/v2/Mail",
                                "body": {
                                    "To": "data-team@company.com",
                                    "Subject": "ADF Pipeline SUCCESS: @{triggerBody()?['pipelineName']}",
                                    "Body": "<h2>Pipeline Completed Successfully</h2><p><b>Pipeline:</b> @{triggerBody()?['pipelineName']}</p>"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
```

### 8.3 Calling Logic App from ADF Pipeline

**Web Activity Configuration:**

```json
{
    "name": "Notify_PipelineStatus",
    "type": "WebActivity",
    "dependsOn": [
        {"activity": "MainProcessing", "dependencyConditions": ["Succeeded"]}
    ],
    "typeProperties": {
        "url": "https://prod-xx.eastus.logic.azure.com/workflows/xxxx/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=xxxxx",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        },
        "body": {
            "value": "@json(concat('{\"pipelineName\":\"', pipeline().Pipeline, '\",\"status\":\"Succeeded\",\"runId\":\"', pipeline().RunId, '\",\"message\":\"Pipeline completed successfully\",\"triggerTime\":\"', pipeline().TriggerTime, '\"}'))",
            "type": "Expression"
        }
    }
}
```

### 8.4 Error Notification Pattern

```error-notification-pattern
┌───────────────────────────────────────────────────────────────────────────────┐
│                    ERROR NOTIFICATION PIPELINE PATTERN                        │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────────┐ │
│   │                        Main Pipeline                                    │ │
│   │   ┌─────────┐      ┌─────────┐      ┌─────────┐                         │ │
│   │   │ Step 1  │─────▶│ Step 2  │─────▶│ Step 3  │                         │ │
│   │   └─────────┘      └─────────┘      └─────────┘                         │ │
│   │        │                │                │                              │ │
│   │        │ On Failure     │ On Failure     │ On Failure                   │ │
│   │        ▼                ▼                ▼                              │ │
│   │   ┌───────────────────────────────────────────────────────────────────┐ │ │
│   │   │              Error Handler (Web Activity → Logic App)             │ │ │
│   │   └───────────────────────────────────────────────────────────────────┘ │ │
│   │                                                                         │ │
│   └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

**ADF Pipeline with Error Handling:**

```json
{
    "name": "pl_with_notifications",
    "properties": {
        "activities": [
            {
                "name": "MainProcessing",
                "type": "ExecutePipeline",
                "typeProperties": {
                    "pipeline": {"referenceName": "pl_main_etl", "type": "PipelineReference"}
                }
            },
            {
                "name": "NotifySuccess",
                "type": "WebActivity",
                "dependsOn": [{"activity": "MainProcessing", "dependencyConditions": ["Succeeded"]}],
                "typeProperties": {
                    "url": "@pipeline().parameters.logicAppUrl",
                    "method": "POST",
                    "body": {
                        "pipelineName": "@{pipeline().Pipeline}",
                        "status": "Succeeded",
                        "runId": "@{pipeline().RunId}"
                    }
                }
            },
            {
                "name": "NotifyFailure",
                "type": "WebActivity",
                "dependsOn": [{"activity": "MainProcessing", "dependencyConditions": ["Failed"]}],
                "typeProperties": {
                    "url": "@pipeline().parameters.logicAppUrl",
                    "method": "POST",
                    "body": {
                        "pipelineName": "@{pipeline().Pipeline}",
                        "status": "Failed",
                        "runId": "@{pipeline().RunId}",
                        "message": "@{activity('MainProcessing').error.message}"
                    }
                }
            }
        ]
    }
}
```

### 8.5 Microsoft Teams Integration

**Logic App Action for Teams:**

```json
{
    "Post_to_Teams": {
        "type": "ApiConnection",
        "inputs": {
            "host": {
                "connection": {
                    "name": "@parameters('$connections')['teams']['connectionId']"
                }
            },
            "method": "post",
            "path": "/v3/beta/teams/@{encodeURIComponent('team-id')}/channels/@{encodeURIComponent('channel-id')}/messages",
            "body": {
                "body": {
                    "contentType": "html",
                    "content": "<h3>Pipeline Status Update</h3><table><tr><td><b>Pipeline:</b></td><td>@{triggerBody()?['pipelineName']}</td></tr><tr><td><b>Status:</b></td><td style='color: @{if(equals(triggerBody()?['status'], 'Failed'), 'red', 'green')}'>@{triggerBody()?['status']}</td></tr><tr><td><b>Time:</b></td><td>@{triggerBody()?['triggerTime']}</td></tr></table>"
                }
            }
        }
    }
}
```

### 8.6 Scheduled Pipeline Trigger via Logic App

**Use Case:** Trigger ADF pipeline on a complex schedule (e.g., only on business days)

```scheduled-trigger-pattern
┌───────────────────────────────────────────────────────────────────────────────┐
│                    LOGIC APP TRIGGERED PIPELINE                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   ┌─────────────────┐                                                         │
│   │  Recurrence     │  ← Every day at 6 AM                                    │
│   │  Trigger        │                                                         │
│   └────────┬────────┘                                                         │
│            │                                                                  │
│            ▼                                                                  │
│   ┌─────────────────┐                                                         │
│   │  Condition:     │  ← Is it a business day?                                │
│   │  dayOfWeek()    │    (Monday-Friday, not holiday)                         │
│   │  not in [0,6]   │                                                         │
│   └────────┬────────┘                                                         │
│            │ Yes                                                              │
│            ▼                                                                  │
│   ┌─────────────────┐                                                         │
│   │  HTTP Action    │  ← Call ADF REST API                                    │
│   │  POST: Create   │    to trigger pipeline                                  │
│   │  Pipeline Run   │                                                         │
│   └─────────────────┘                                                         │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

**HTTP Action to Trigger ADF Pipeline:**

```json
{
    "TriggerADFPipeline": {
        "type": "Http",
        "inputs": {
            "method": "POST",
            "uri": "https://management.azure.com/subscriptions/{subscription-id}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factory-name}/pipelines/{pipeline-name}/createRun?api-version=2018-06-01",
            "authentication": {
                "type": "ManagedServiceIdentity"
            },
            "body": {
                "param1": "value1"
            }
        }
    }
}
```

---

## 9. Azure Databricks Tutorial

### 9.1 What is Azure Databricks?

**Simple Explanation:** Azure Databricks is like a super-powered workshop where you can process massive amounts of data. Imagine having a regular kitchen (your laptop) vs. a commercial kitchen with 100 chefs (Databricks) - both can cook, but one can handle restaurant-scale operations.

```databricks-explained
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS EXPLAINED                                        │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Traditional Processing              Databricks (Distributed)                 │
│   ─────────────────────               ─────────────────────────                │
│                                                                                │
│   ┌──────────────┐                   ┌──────────────────────────┐              │
│   │   1 Machine  │                   │      DRIVER NODE         │              │
│   │              │                   │   (Coordinator/Brain)    │              │
│   │  ┌────────┐  │                   └────────────┬─────────────┘              │
│   │  │ Python │  │                                │                            │
│   │  │ Script │  │                   ┌────────────┼────────────┐               │
│   │  └────────┘  │                   ▼            ▼            ▼               │
│   │              │                   ┌────┐    ┌────┐    ┌────┐                │
│   │  Process 1GB │                   │Node│    │Node│    │Node│                │
│   │  at a time   │                   │ 1  │    │ 2  │    │ 3  │                │
│   └──────────────┘                   └────┘    └────┘    └────┘                │
│                                      Process 100GB+ in parallel                │
│   Time: Hours                        Time: Minutes                             │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Key Components:**

| Component     | What It Is                     | Analogy               |
| ------------- | ------------------------------ | --------------------- |
| **Workspace** | Your working environment       | Your office building  |
| **Cluster**   | Group of VMs that process data | Team of workers       |
| **Notebook**  | Interactive code environment   | Your notepad          |
| **Job**       | Scheduled/automated execution  | Task on your calendar |
| **DBFS**      | Databricks File System         | Shared company drive  |

### 9.2 Creating an Azure Databricks Workspace

**Via Azure Portal:**

1. Search "Azure Databricks" → Create
2. Configure:
   - **Workspace name:** dbw-dataeng-dev-001
   - **Region:** East US
   - **Pricing Tier:** Premium (for Unity Catalog)

**Via Azure CLI:**

```bash
# Create Databricks Workspace
az databricks workspace create \
    --name "dbw-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --sku premium

# Get workspace URL
az databricks workspace show \
    --name "dbw-dataeng-dev-001" \
    --resource-group "data-engineering-rg" \
    --query "workspaceUrl" -o tsv
```

```databricks-workspace-layout
┌──────────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS WORKSPACE LAYOUT                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────────────────────────────────────┐  │
│  │  SIDEBAR         │  │              MAIN WORKSPACE AREA                 │  │
│  │  ─────────       │  │                                                  │  │
│  │  Workspace       │  │  ┌─────────────────────────────────────────────┐ │  │
│  │  Repos           │  │  │              NOTEBOOK                       │ │  │
│  │  Data            │  │  │  ┌────────────────────────────────────────┐ │ │  │
│  │  Compute         │  │  │  │ # Cell 1 - Python                      │ │ │  │
│  │  Workflows       │  │  │  │ df = spark.read.parquet("/data/...")   │ │ │  │
│  │  SQL             │  │  │  └────────────────────────────────────────┘ │ │  │
│  │  ML              │  │  │  ┌────────────────────────────────────────┐ │ │  │
│  │                  │  │  │  │ # Cell 2 - SQL                         │ │ │  │
│  │                  │  │  │  │ SELECT * FROM customers LIMIT 10       │ │ │  │
│  │                  │  │  │  └────────────────────────────────────────┘ │ │  │
│  │                  │  │  └─────────────────────────────────────────────┘ │  │
│  └──────────────────┘  └──────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 9.4 Clusters Deep Dive

**What is a Cluster?**

A cluster is a set of virtual machines that work together to process your data. Think of it as hiring a team of workers - more workers = faster processing.

```cluster-architecture
┌───────────────────────────────────────────────────────────────────────────────┐
│                    CLUSTER ARCHITECTURE                                       │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────────┐ │
│   │                         CLUSTER                                         │ │
│   │   ┌─────────────────────────────────────────────────────────────────┐   │ │
│   │   │  DRIVER NODE                                                    │   │ │
│   │   │  • Coordinates work                                             │   │ │
│   │   │  • Runs your notebook                                           │   │ │
│   │   │  • Collects results                                             │   │ │
│   │   └─────────────────────────────────────────────────────────────────┘   │ │
│   │                              │                                          │ │
│   │              ┌───────────────┼───────────────┐                          │ │
│   │              ▼               ▼               ▼                          │ │
│   │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│   │   │ WORKER NODE  │  │ WORKER NODE  │  │ WORKER NODE  │                  │ │
│   │   │ • Processes  │  │ • Processes  │  │ • Processes  │                  │ │
│   │   │   data       │  │   data       │  │   data       │                  │ │
│   │   │ • 4-16 cores │  │ • 4-16 cores │  │ • 4-16 cores │                  │ │
│   │   └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│   └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
│   CLUSTER MODES:                                                              │
│   ─────────────                                                               │
│   • All-Purpose: Interactive development (stays running)                      │
│   • Job Cluster: Runs once and terminates (cost-effective)                    │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Cluster Configuration JSON:**

```json
{
    "cluster_name": "data-engineering-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autoscale": {
        "min_workers": 1,
        "max_workers": 4
    },
    "autotermination_minutes": 30,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    },
    "custom_tags": {
        "Environment": "Development",
        "Project": "DataEngineering"
    }
}
```

**Cluster Best Practices:**

```cluster-sizing-guide
┌────────────────────────────────────────────────────────────────────────────────┐
│                    CLUSTER SIZING GUIDE                                        │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Data Size        Recommended Workers    Node Type                            │
│   ─────────        ───────────────────    ─────────                            │
│   < 10 GB          1-2 workers            Standard_DS3_v2                      │
│   10-100 GB        2-4 workers            Standard_DS4_v2                      │
│   100 GB - 1 TB    4-8 workers            Standard_DS5_v2                      │
│   > 1 TB           8+ workers             Standard_E8s_v3                      │
│                                                                                │
│   COST SAVING TIPS:                                                            │
│   ─────────────────                                                            │
│   1. Enable autoscaling (min 1, max as needed)                                 │
│   2. Set auto-termination (30-60 minutes)                                      │
│   3. Use spot instances for non-critical workloads                             │
│   4. Use job clusters for production (not all-purpose)                         │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 9.5 Notebooks in Databricks

**Creating Your First Notebook:**

```python
# Cell 1: Basic Spark operations
# ─────────────────────────────────────────────────────────────────────────────

# Create a simple DataFrame
data = [
    (1, "John", "Engineering", 75000),
    (2, "Jane", "Marketing", 65000),
    (3, "Bob", "Engineering", 80000),
    (4, "Alice", "Sales", 70000)
]

columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Display the DataFrame
display(df)
```

```python
# Cell 2: Read data from Azure Data Lake
# ─────────────────────────────────────────────────────────────────────────────

# Read Parquet files
df_customers = spark.read.parquet(
    "abfss://bronze@sa4dataengdev001.dfs.core.windows.net/customers/"
)

# Show schema
df_customers.printSchema()

# Show sample data
display(df_customers.limit(10))
```

```sql
-- Cell 3: SQL Magic (change cell language)
-- ─────────────────────────────────────────────────────────────────────────────

-- You can write SQL directly in notebooks
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
```

### 9.6 DBFS - Databricks File System

```dbfs-structure
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DBFS STRUCTURE                                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   /                                                                            │
│   ├── /FileStore/           ← Upload files via UI                              │
│   │   ├── /tables/          ← Uploaded tables                                  │
│   │   └── /shared_uploads/  ← Shared files                                     │
│   │                                                                            │
│   ├── /mnt/                 ← Mounted external storage                         │
│   │   ├── /bronze/          ← Mount to ADLS bronze container                   │
│   │   ├── /silver/          ← Mount to ADLS silver container                   │
│   │   └── /gold/            ← Mount to ADLS gold container                     │
│   │                                                                            │
│   ├── /user/                ← User-specific directories                        │
│   │   └── /hive/            ← Hive metastore data                              │
│   │                                                                            │
│   └── /databricks-datasets/ ← Sample datasets                                  │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Working with DBFS:**

```python
# List files in DBFS
dbutils.fs.ls("/FileStore/")

# Read a file
df = spark.read.csv("/FileStore/tables/sample.csv", header=True)

# Write data
df.write.mode("overwrite").parquet("/FileStore/output/processed_data")

# Copy files
dbutils.fs.cp("/source/file.csv", "/destination/file.csv")

# Remove files
dbutils.fs.rm("/path/to/file", recurse=True)
```

### 9.7 Mounting Azure Data Lake Storage

**Why Mount?** Mounting creates a shortcut to your cloud storage, so you can access it like a local folder.

```python
# Mount Azure Data Lake Storage Gen2
# ─────────────────────────────────────────────────────────────────────────────

# Configuration
storage_account = "sa4dataengdev001"
container = "bronze"
mount_point = "/mnt/bronze"

# Using Service Principal
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvault-scope", key="sp-client-id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault-scope", key="sp-client-secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='keyvault-scope', key='tenant-id')}/oauth2/token"
}

# Mount the storage
dbutils.fs.mount(
    source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)

# Verify mount
display(dbutils.fs.ls(mount_point))
```

**Access Patterns Comparison:**

```storage-access-patterns
┌────────────────────────────────────────────────────────────────────────────────┐
│                    STORAGE ACCESS PATTERNS                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  METHOD 1: Direct Access (ABFSS)                                               │
│  ────────────────────────────────                                              │
│  spark.read.parquet("abfss://container@account.dfs.core.windows.net/path")     │
│  ✓ Works with Unity Catalog                                                    │
│  ✓ Modern approach                                                             │
│  ✗ Long URLs                                                                   │
│                                                                                │
│  METHOD 2: Mount Points                                                        │
│  ──────────────────────                                                        │
│  spark.read.parquet("/mnt/bronze/path")                                        │
│  ✓ Simple, short paths                                                         │
│  ✗ Not recommended with Unity Catalog                                          │
│  ✗ Security concerns (shared credentials)                                      │
│                                                                                │
│  METHOD 3: Unity Catalog External Locations (Recommended)                      │
│  ─────────────────────────────────────────────────────────                     │
│  spark.read.table("catalog.schema.table")                                      │
│  ✓ Best security (fine-grained access)                                         │
│  ✓ Governance built-in                                                         │
│  ✓ Modern best practice                                                        │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 9.8 Delta Lake Introduction

**What is Delta Lake?**

Delta Lake is an open-source storage layer that brings reliability to data lakes. Think of it as upgrading from a basic filing cabinet to a smart, versioned document management system.

```delta-lake-benefits
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DELTA LAKE BENEFITS                                         │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Regular Parquet Files              Delta Lake                                │
│   ─────────────────────              ──────────                                │
│                                                                                │
│   ┌─────────────────┐               ┌─────────────────┐                        │
│   │  file1.parquet  │               │  Delta Table    │                        │
│   │  file2.parquet  │               │  ┌───────────┐  │                        │
│   │  file3.parquet  │               │  │Transaction│  │ ← ACID transactions    │
│   └─────────────────┘               │  │   Log     │  │                        │
│                                     │  └───────────┘  │                        │
│   ✗ No transactions                 │  ┌───────────┐  │                        │
│   ✗ No versioning                   │  │ Versions  │  │ ← Time travel          │
│   ✗ Inconsistent reads              │  │ v1,v2,v3  │  │                        │
│   ✗ No schema enforcement           │  └───────────┘  │                        │
│                                     │  ┌───────────┐  │                        │
│                                     │  │  Schema   │  │ ← Schema enforcement   │
│                                     │  │Validation │  │                        │
│                                     │  └───────────┘  │                        │
│                                     └─────────────────┘                        │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Delta Lake Operations:**

```python
# Create a Delta table
# ─────────────────────────────────────────────────────────────────────────────

df.write.format("delta").mode("overwrite").save("/mnt/bronze/customers_delta")

# Read Delta table
df_delta = spark.read.format("delta").load("/mnt/bronze/customers_delta")

# Create managed Delta table
df.write.format("delta").saveAsTable("bronze.customers")
```

```python
# MERGE operation (Upsert)
# ─────────────────────────────────────────────────────────────────────────────

from delta.tables import DeltaTable

# Load existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/silver/customers")

# New/updated data
updates_df = spark.read.parquet("/mnt/bronze/customers_updates")

# Perform MERGE (upsert)
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

```python
# Time Travel
# ─────────────────────────────────────────────────────────────────────────────

# Read previous version
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load("/path/to/delta")

# Read data as of timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-14") \
    .load("/path/to/delta")

# View history
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "/path/to/delta")
display(delta_table.history())
```

### 9.9 Databricks Widgets (Parameters)

**Widgets allow you to add parameters to notebooks:**

```python
# Create widgets
# ─────────────────────────────────────────────────────────────────────────────

# Text input
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")

# Dropdown
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

# Combobox (dropdown + text)
dbutils.widgets.combobox("table_name", "customers", ["customers", "products", "orders"], "Table")

# Multiselect
dbutils.widgets.multiselect("columns", "id", ["id", "name", "email", "phone"], "Columns")

# Get widget values
start_date = dbutils.widgets.get("start_date")
environment = dbutils.widgets.get("environment")

print(f"Processing {environment} data from {start_date}")

# Remove widgets
dbutils.widgets.remove("start_date")
dbutils.widgets.removeAll()
```

### 9.10 Databricks Utilities (dbutils)

```python
# File System Utilities
# ─────────────────────────────────────────────────────────────────────────────
dbutils.fs.ls("/path")                    # List files
dbutils.fs.mkdirs("/path")                # Create directory
dbutils.fs.cp("/src", "/dst")             # Copy
dbutils.fs.mv("/src", "/dst")             # Move
dbutils.fs.rm("/path", recurse=True)      # Remove
dbutils.fs.head("/path/file.txt", 100)    # Read first 100 bytes

# Secrets Utilities
# ─────────────────────────────────────────────────────────────────────────────
dbutils.secrets.listScopes()                           # List secret scopes
dbutils.secrets.list("my-scope")                       # List secrets in scope
password = dbutils.secrets.get("my-scope", "db-pass")  # Get secret value

# Notebook Utilities
# ─────────────────────────────────────────────────────────────────────────────
dbutils.notebook.run("/path/to/notebook", 60, {"param": "value"})  # Run notebook
dbutils.notebook.exit("Success")                                    # Exit with value

# Widget Utilities
# ─────────────────────────────────────────────────────────────────────────────
dbutils.widgets.text("name", "default")    # Create widget
dbutils.widgets.get("name")                # Get value
dbutils.widgets.removeAll()                # Clear all
```

---

## 10. Databricks Unity Catalog

### 10.1 What is Unity Catalog?

**Simple Explanation:** Unity Catalog is like a librarian for all your data. Just as a library has a catalog system to track every book's location and who can access it, Unity Catalog tracks every table, file, and who has permission to use them.

```unity-catalog-hierarchy
┌─────────────────────────────────────────────────────────────────────────────┐
│                         UNITY CATALOG HIERARCHY                             │
│                    (Databricks Lakehouse Data Governance)                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                              ┌──────────────┐                               │
│                              │  METASTORE   │                               │
│                              └──────┬───────┘                               │
│                                     │                                       │
│                    Top-level container (one per region)                     │
│                    Manages access and metadata for all data                 │
│                                     │                                       │
│         ┌───────────────────────────┼───────────────────────────┐           │
│         │                           │                           │           │
│         ▼                           ▼                           ▼           │
│  ┌─────────────┐            ┌─────────────┐            ┌─────────────┐      │
│  │  CATALOG    │            │  CATALOG    │            │  CATALOG    │      │
│  │  (dev)      │            │  (staging)  │            │  (prod)     │      │
│  └──────┬──────┘            └──────┬──────┘            └──────┬──────┘      │
│         │                          │                          │             │
│    Database-like container    Groups related schemas    Isolated env        │
│         │                          │                          │             │
│    ┌────┼────┐              ┌──────┼──────┐            ┌──────┼──────┐      │
│    │    │    │              │      │      │            │      │      │      │
│    ▼    ▼    ▼              ▼      ▼      ▼            ▼      ▼      ▼      │
│  ┌───┐┌───┐┌───┐          ┌───┐ ┌───┐ ┌───┐        ┌───┐ ┌───┐ ┌───┐        │
│  │raw││stg││prd│          │raw│ │stg│ │prd│        │raw│ │stg│ │prd│        │
│  └─┬─┘└─┬─┘└─┬─┘          └─┬─┘ └─┬─┘ └─┬─┘        └─┬─┘ └─┬─┘ └─┬─┘        │
│    │    │    │              │     │     │            │     │     │          │
│    │  SCHEMA  │         Namespace for objects    Medallion architecture     │
│    │(bronze,  │         Examples: bronze, silver, gold                      │
│    │ silver,  │         raw, staging, production                            │
│    │  gold)   │                  │                                          │
│    │          │                  │                                          │
│    └────┬─────┘                  ▼                                          │
│         │                                                                   │
│         └──────────────┬─────────────────────┐                              │
│                        │                     │                              │
│                        ▼                     ▼                              │
│              ┌──────────────────┐  ┌──────────────────┐                     │
│              │  MANAGED TABLES  │  │ EXTERNAL TABLES  │                     │
│              └────────┬─────────┘  └────────┬─────────┘                     │
│                       │                     │                               │
│         ┌─────────────┼─────────────────────┼─────────────────┐             │
│         │             │                     │                 │             │
│         ▼             ▼                     ▼                 ▼             │
│    ┌────────┐   ┌────────┐           ┌────────┐       ┌──────────┐          │
│    │ TABLES │   │ VIEWS  │           │ VOLUMES│       │ FUNCTIONS│          │
│    └────────┘   └────────┘           └────────┘       └──────────┘          │
│         │             │                     │                 │             │
│    Delta tables  Logical views      File storage      SQL/Python UDFs       │
│    Parquet, etc  Security layer     (PDFs, CSVs)      ML models             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

```three-level-namespace
┌───────────────────────────────────────────────────────────────────────────────┐
│                           THREE-LEVEL NAMESPACE                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   Format:  <catalog>.<schema>.<object>                                        │
│                                                                               │
│   Examples:                                                                   │
│   • prod_catalog.gold.dim_customers                                           │
│   • dev_catalog.bronze.raw_sales                                              │
│   • analytics.silver.fact_orders                                              │
│   • ml_catalog.features.customer_lifetime_value                               │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

```medallion-architecture
┌───────────────────────────────────────────────────────────────────────────────┐
│                        TYPICAL MEDALLION ARCHITECTURE                         │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   CATALOG: production                                                         │
│   ├── SCHEMA: bronze (raw data)                                               │
│   │   ├── TABLE: raw_sales                                                    │
│   │   ├── TABLE: raw_customers                                                │
│   │   └── VOLUME: source_files/                                               │
│   │                                                                           │
│   ├── SCHEMA: silver (cleaned & conformed)                                    │
│   │   ├── TABLE: stg_sales                                                    │
│   │   ├── TABLE: stg_customers                                                │
│   │   └── VIEW: vw_active_customers                                           │
│   │                                                                           │
│   └── SCHEMA: gold (business-ready)                                           │
│       ├── TABLE: dim_customers                                                │
│       ├── TABLE: dim_products                                                 │
│       ├── TABLE: fact_sales                                                   │
│       └── FUNCTION: calculate_customer_ltv                                    │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

```key-characteristics
┌───────────────────────────────────────────────────────────────────────────────┐
│                            KEY CHARACTERISTICS                                │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  METASTORE                                                                    │
│  • One per cloud region                                                       │
│  • Centralized metadata repository                                            │
│  • Manages permissions across all catalogs                                    │
│                                                                               │
│  CATALOG                                                                      │
│  • Highest level of data organization                                         │
│  • Isolates environments (dev/staging/prod)                                   │
│  • Can be shared across workspaces                                            │
│  • Supports cross-catalog queries                                             │
│                                                                               │
│  SCHEMA (Database)                                                            │
│  • Groups related tables/views                                                │
│  • Common pattern: bronze/silver/gold layers                                  │
│  • Implements data quality tiers                                              │
│  • Can have different access controls                                         │
│                                                                               │
│  TABLES/VIEWS                                                                 │
│  • Tables: Managed (Unity Catalog controls) or External (you control)         │
│  • Views: Virtual tables with security/transformation logic                   │
│  • Volumes: Non-tabular data (files, images, models)                          │
│  • Functions: Reusable SQL/Python code                                        │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

```governance-security
┌───────────────────────────────────────────────────────────────────────────────┐
│                         GOVERNANCE & SECURITY                                 │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  FINE-GRAINED ACCESS CONTROL                                        │      │
│  │                                                                     │      │
│  │  Catalog Level:  GRANT USE CATALOG ON catalog TO user               │      │
│  │  Schema Level:   GRANT SELECT ON SCHEMA schema TO user              │      │
│  │  Table Level:    GRANT SELECT ON TABLE table TO user                │      │
│  │  Column Level:   GRANT SELECT(col1, col2) ON TABLE table TO user    │      │
│  │  Row Level:      CREATE ROW FILTER ... (using SQL predicates)       │      │
│  │                                                                     │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  DATA LINEAGE                                                       │      │
│  │                                                                     │      │
│  │  • Automatic tracking of data flows                                 │      │
│  │  • Table → Transformation → Table                                   │      │
│  │  • Supports impact analysis                                         │      │
│  │  • Audit logs for compliance                                        │      │
│  │                                                                     │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Setting Up Unity Catalog

#### Step 1: Create Metastore (Admin Only)

```metastore-setup
┌────────────────────────────────────────────────────────────────────────────────┐
│                    METASTORE SETUP                                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   1. Create Storage Account for Unity Catalog                                  │
│      └── Container: "unity-catalog"                                            │
│                                                                                │
│   2. Create Access Connector for Azure Databricks                              │
│      └── Managed Identity to access storage                                    │
│                                                                                │
│   3. Create Metastore in Databricks Account Console                            │
│      └── Account Console → Data → Create Metastore                             │
│                                                                                │
│   4. Assign Metastore to Workspace                                             │
│      └── Metastore → Workspaces → Assign                                       │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Azure CLI Setup:**

```bash
# Create storage account for Unity Catalog
az storage account create \
    --name "stunitycatalogdev001" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --sku "Standard_LRS" \
    --kind "StorageV2" \
    --hierarchical-namespace true

# Create container
az storage container create \
    --name "unity-catalog" \
    --account-name "stunitycatalogdev001"

# Create Access Connector
az databricks access-connector create \
    --name "ac-unity-catalog" \
    --resource-group "data-engineering-rg" \
    --location "eastus" \
    --identity-type "SystemAssigned"
```

### 10.3 Creating Catalogs and Schemas

```sql
-- Create a catalog
-- ─────────────────────────────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS dev_catalog
COMMENT 'Development environment catalog';

-- Use the catalog
USE CATALOG dev_catalog;

-- Create schemas (following medallion architecture)
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data landing zone';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready aggregated data';

-- View all catalogs
SHOW CATALOGS;

-- View schemas in current catalog
SHOW SCHEMAS;
```

### 10.4 Managing Tables in Unity Catalog

```sql
-- Create a managed table (Unity Catalog manages storage)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE dev_catalog.bronze.raw_customers (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_date TIMESTAMP
)
USING DELTA
COMMENT 'Raw customer data from source system';

-- Create table from DataFrame (PySpark)
-- ─────────────────────────────────────────────────────────────────────────────
```

```python
# PySpark: Save DataFrame as Unity Catalog table
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev_catalog.silver.customers")
```

```sql
-- Create external table (you manage storage location)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE dev_catalog.bronze.ext_orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
)
USING DELTA
LOCATION 'abfss://bronze@sa4dataengdev001.dfs.core.windows.net/orders';
```

### 10.5 Access Control with Unity Catalog

```unity-catalog-permissions
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG PERMISSIONS                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   PRIVILEGE HIERARCHY:                                                          │
│   ────────────────────                                                          │
│                                                                                 │
│   Metastore Owner                                                               │
│        │ CREATE CATALOG                                                         │
│        ▼                                                                        │
│   Catalog Owner/Admin                                                           │
│        │ CREATE SCHEMA, USE CATALOG                                             │
│        ▼                                                                        │
│   Schema Owner                                                                  │
│        │ CREATE TABLE, USE SCHEMA                                               │
│        ▼                                                                        │
│   Table Owner                                                                   │
│        │ SELECT, MODIFY                                                         │
│        ▼                                                                        │
│   Users                                                                         │
│          SELECT (read-only)                                                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```sql
-- Grant permissions
-- ─────────────────────────────────────────────────────────────────────────────

-- Grant catalog usage to a group
GRANT USE CATALOG ON CATALOG dev_catalog TO `data-engineers`;

-- Grant schema usage
GRANT USE SCHEMA ON SCHEMA dev_catalog.gold TO `data-analysts`;

-- Grant table read access
GRANT SELECT ON TABLE dev_catalog.gold.dim_customers TO `data-analysts`;

-- Grant full table access
GRANT ALL PRIVILEGES ON TABLE dev_catalog.silver.customers TO `data-engineers`;

-- View grants
SHOW GRANTS ON TABLE dev_catalog.gold.dim_customers;

-- Revoke permissions
REVOKE SELECT ON TABLE dev_catalog.gold.dim_customers FROM `data-analysts`;
```

### 10.6 External Locations and Storage Credentials

```sql
-- Create storage credential (admin)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE STORAGE CREDENTIAL azure_storage_cred
WITH (
    AZURE_MANAGED_IDENTITY = '/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Databricks/accessConnectors/ac-unity-catalog'
);

-- Create external location
CREATE EXTERNAL LOCATION bronze_location
URL 'abfss://bronze@sa4dataengdev001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL azure_storage_cred)
COMMENT 'Bronze layer storage location';

-- Grant access to external location
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION bronze_location TO `data-engineers`;
```

---

## 11. Spark Streaming with Databricks Auto Loader

### 11.1 What is Spark Streaming?

**Simple Explanation:** Instead of processing data in batches (like doing laundry once a week), streaming processes data continuously as it arrives (like a self-cleaning oven that cleans while you cook).

```batch-vs-streaming
┌────────────────────────────────────────────────────────────────────────────────┐
│                    BATCH vs STREAMING PROCESSING                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   BATCH PROCESSING:                                                            │
│   ─────────────────                                                            │
│   Files arrive: 📄 📄 📄 📄 📄 📄 📄 📄 📄 📄                                 │
│                  └─────────────────────────────┘                               │
│                              │                                                 │
│                              ▼ Process all at 2AM                              │
│                  ┌─────────────────────────────┐                               │
│                  │    Batch Job (Daily)        │                               │
│                  └─────────────────────────────┘                               │
│   Latency: Hours                                                               │
│                                                                                │
│   STREAMING PROCESSING:                                                        │
│   ─────────────────────                                                        │
│   Files arrive:  📄──▶process──▶📄──▶process──▶📄──▶process                    │
│                  Continuous processing as data arrives                         │
│   Latency: Seconds to Minutes                                                  │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 11.2 What is Auto Loader?

**Auto Loader** is Databricks' recommended way to ingest files. It automatically discovers new files and processes them incrementally.

```auto-loader-architecture
┌────────────────────────────────────────────────────────────────────────────────┐
│                    AUTO LOADER ARCHITECTURE                                    │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Cloud Storage                                                                │
│   ┌─────────────────────────────────────────────┐                              │
│   │  /landing/                                  │                              │
│   │    ├── file_001.json  ← Already processed   │                              │
│   │    ├── file_002.json  ← Already processed   │                              │
│   │    ├── file_003.json  ← NEW FILE!           │ ◄────┐                       │
│   │    └── file_004.json  ← NEW FILE!           │      │                       │
│   └─────────────────────────────────────────────┘      │                       │
│                         │                              │                       │
│                         ▼                              │                       │
│   ┌─────────────────────────────────────────────┐      │ Checkpoint tracks     │
│   │              AUTO LOADER                    │      │ processed files       │
│   │   • Discovers new files automatically       │──────┘                       │
│   │   • Tracks progress via checkpointing       │                              │
│   │   • Handles schema evolution                │                              │
│   │   • Scales to millions of files             │                              │
│   └─────────────────────────────────────────────┘                              │
│                         │                                                      │
│                         ▼                                                      │
│   ┌─────────────────────────────────────────────┐                              │
│   │              DELTA TABLE                    │                              │
│   │   Only new files appended                   │                              │
│   └─────────────────────────────────────────────┘                              │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 11.3 Auto Loader Basic Usage

```python
# Basic Auto Loader - Read JSON files
# ─────────────────────────────────────────────────────────────────────────────

# Define the source and checkpoint paths
source_path = "abfss://landing@sa4dataengdev001.dfs.core.windows.net/customers/"
checkpoint_path = "abfss://checkpoints@sa4dataengdev001.dfs.core.windows.net/customers/"

# Read streaming data with Auto Loader
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path + "schema/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
)

# Add metadata columns
from pyspark.sql.functions import current_timestamp, input_file_name

df_enriched = df_stream \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# Write to Delta table
(
    df_enriched.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path + "checkpoint/")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # Process all available files and stop
    .toTable("dev_catalog.bronze.customers")
)
```

### 11.4 Auto Loader Options Deep Dive

```python
# Advanced Auto Loader configuration
# ─────────────────────────────────────────────────────────────────────────────

df_stream = (
    spark.readStream
    .format("cloudFiles")

    # File format options
    .option("cloudFiles.format", "json")                    # json, csv, parquet, avro
    .option("cloudFiles.inferColumnTypes", "true")          # Infer data types
    .option("cloudFiles.schemaHints", "id INT, price DOUBLE")  # Type hints

    # Schema evolution
    .option("cloudFiles.schemaLocation", "/path/schema/")   # Store inferred schema
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle new columns

    # File discovery
    .option("cloudFiles.useNotifications", "true")          # Use cloud events (faster)
    .option("cloudFiles.maxFilesPerTrigger", 1000)          # Limit files per batch
    .option("cloudFiles.maxBytesPerTrigger", "10g")         # Limit bytes per batch

    # File patterns
    .option("pathGlobFilter", "*.json")                     # Only JSON files
    .option("recursiveFileLookup", "true")                  # Include subdirectories

    .load(source_path)
)
```

### 11.5 Different Trigger Modes

```python
# Trigger modes explained
# ─────────────────────────────────────────────────────────────────────────────

# 1. AVAILABLE NOW - Process all files and stop (batch-like)
# Use for: Scheduled jobs, catching up on backlog
query = (
    df_stream.writeStream
    .trigger(availableNow=True)
    .toTable("bronze.customers")
)

# 2. PROCESSING TIME - Run at fixed intervals
# Use for: Near real-time with controlled frequency
query = (
    df_stream.writeStream
    .trigger(processingTime="5 minutes")
    .toTable("bronze.customers")
)

# 3. CONTINUOUS - Lowest latency (milliseconds)
# Use for: True real-time requirements
query = (
    df_stream.writeStream
    .trigger(continuous="1 second")
    .toTable("bronze.customers")
)

# 4. DEFAULT (micro-batch) - Process as fast as possible
# Use for: High throughput streaming
query = (
    df_stream.writeStream
    .toTable("bronze.customers")
)
```

### 11.6 Complete Streaming Pipeline Example

```python
# Complete Auto Loader Pipeline
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
source_path = "abfss://landing@storage.dfs.core.windows.net/sales/"
checkpoint_base = "abfss://checkpoints@storage.dfs.core.windows.net/sales/"

# Define expected schema (optional but recommended)
sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

# Read stream with Auto Loader
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/schema/")
    .schema(sales_schema)  # Use defined schema
    .load(source_path)
)

# Transform the data
df_transformed = (
    df_raw
    # Add metadata
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", input_file_name())

    # Parse and transform
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount", col("quantity") * col("unit_price"))

    # Data quality
    .filter(col("order_id").isNotNull())
    .filter(col("quantity") > 0)
)

# Write to Bronze layer
bronze_query = (
    df_transformed.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/bronze_checkpoint/")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("dev_catalog.bronze.sales_orders")
)

# Wait for completion
bronze_query.awaitTermination()
```

### 11.7 Monitoring Streaming Queries

```python
# Monitor streaming query progress
# ─────────────────────────────────────────────────────────────────────────────

# Start a streaming query
query = df_stream.writeStream.format("delta").start()

# Check query status
print(f"Query ID: {query.id}")
print(f"Query Name: {query.name}")
print(f"Is Active: {query.isActive}")

# Get recent progress
for progress in query.recentProgress:
    print(f"Batch ID: {progress['batchId']}")
    print(f"Input Rows: {progress['numInputRows']}")
    print(f"Processing Time: {progress['batchDuration']}ms")

# Stop query gracefully
query.stop()

# List all active streams
for stream in spark.streams.active:
    print(f"Stream: {stream.name}, Status: {stream.status}")
```

### 11.8 Error Handling in Streaming

```python
# Rescue data pattern for handling bad records
# ─────────────────────────────────────────────────────────────────────────────

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)

    # Rescue bad records instead of failing
    .option("rescuedDataColumn", "_rescued_data")

    # Handle corrupt records
    .option("mode", "PERMISSIVE")  # Don't fail on bad records

    .load(source_path)
)

# Separate good and bad records
df_good = df_stream.filter(col("_rescued_data").isNull())
df_bad = df_stream.filter(col("_rescued_data").isNotNull())

# Write good records to main table
df_good.writeStream.toTable("bronze.sales_valid")

# Write bad records to error table for investigation
df_bad.writeStream.toTable("bronze.sales_errors")
```

```streaming-best-practices
┌────────────────────────────────────────────────────────────────────────────────┐
│                    STREAMING BEST PRACTICES                                    │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  1. ALWAYS use checkpoints                                                     │
│     └── Enables exactly-once processing and recovery                           │
│                                                                                │
│  2. DEFINE schemas explicitly when possible                                    │
│     └── Faster startup, predictable behavior                                   │
│                                                                                │
│  3. USE rescue data column for bad records                                     │
│     └── Don't lose data due to format issues                                   │
│                                                                                │
│  4. SIZE triggers appropriately                                                │
│     └── Balance latency vs. throughput                                         │
│                                                                                │
│  5. MONITOR query progress                                                     │
│     └── Set up alerts for failed/lagging queries                               │
│                                                                                │
│  6. TEST with availableNow first                                               │
│     └── Debug before running continuous                                        │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 12. PySpark Transformations & APIs

### 12.1 Understanding PySpark

**Simple Explanation:** PySpark is Python's way of talking to Spark. It lets you write Python code that gets executed across many machines in parallel. Think of it as writing instructions that 100 workers follow simultaneously.

```pyspark-data-structures
┌────────────────────────────────────────────────────────────────────────────────┐
│                    PYSPARK DATA STRUCTURES                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   RDD (Resilient Distributed Dataset)                                          │
│   ───────────────────────────────────                                          │
│   • Low-level API (rarely used directly)                                       │
│   • Unstructured data                                                          │
│   • Full control, but verbose                                                  │
│                                                                                │
│   DataFrame (Most Common)                                                      │
│   ───────────────────────                                                      │
│   • High-level API (like pandas)                                               │
│   • Structured data with schema                                                │
│   • Optimized by Catalyst optimizer                                            │
│   • ✓ USE THIS FOR MOST TASKS                                                  │
│                                                                                │
│   Dataset (Scala/Java only)                                                    │
│   ─────────────────────────                                                    │
│   • Typed DataFrame                                                            │
│   • Compile-time type safety                                                   │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Creating DataFrames

```python
# Method 1: From Python list
# ─────────────────────────────────────────────────────────────────────────────
data = [
    (1, "John", "Engineering", 75000),
    (2, "Jane", "Marketing", 65000),
    (3, "Bob", "Engineering", 80000)
]
columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)
display(df)

# Method 2: From pandas DataFrame
# ─────────────────────────────────────────────────────────────────────────────
import pandas as pd

pandas_df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["John", "Jane", "Bob"]
})

df = spark.createDataFrame(pandas_df)

# Method 3: From files
# ─────────────────────────────────────────────────────────────────────────────
df_parquet = spark.read.parquet("/path/to/data.parquet")
df_csv = spark.read.option("header", "true").csv("/path/to/data.csv")
df_json = spark.read.json("/path/to/data.json")
df_delta = spark.read.format("delta").load("/path/to/delta")

# Method 4: From table
# ─────────────────────────────────────────────────────────────────────────────
df = spark.table("catalog.schema.table_name")
```

### 12.3 Essential DataFrame Operations

```python
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# SELECTION & FILTERING
# ═══════════════════════════════════════════════════════════════════════════════

# Select columns
df.select("name", "salary")
df.select(col("name"), col("salary"))
df.select(df.name, df.salary)

# Select with expressions
df.select(
    col("name"),
    col("salary"),
    (col("salary") * 1.1).alias("salary_with_raise")
)

# Filter rows
df.filter(col("salary") > 70000)
df.filter("salary > 70000")  # SQL string
df.where(col("department") == "Engineering")

# Multiple conditions
df.filter((col("salary") > 70000) & (col("department") == "Engineering"))
df.filter((col("salary") > 70000) | (col("department") == "Marketing"))

# ═══════════════════════════════════════════════════════════════════════════════
# ADDING & MODIFYING COLUMNS
# ═══════════════════════════════════════════════════════════════════════════════

# Add new column
df.withColumn("bonus", col("salary") * 0.1)

# Rename column
df.withColumnRenamed("salary", "annual_salary")

# Multiple columns at once
df.withColumns({
    "bonus": col("salary") * 0.1,
    "tax": col("salary") * 0.2,
    "net_salary": col("salary") - (col("salary") * 0.2)
})

# Drop columns
df.drop("bonus", "tax")

# ═══════════════════════════════════════════════════════════════════════════════
# SORTING & LIMITING
# ═══════════════════════════════════════════════════════════════════════════════

# Sort ascending
df.orderBy("salary")
df.orderBy(col("salary").asc())

# Sort descending
df.orderBy(col("salary").desc())

# Multiple sort columns
df.orderBy(col("department").asc(), col("salary").desc())

# Limit rows
df.limit(10)

# Get distinct values
df.select("department").distinct()
df.dropDuplicates(["department"])
```

### 12.4 Aggregations

```python
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# BASIC AGGREGATIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Simple aggregations
df.agg(
    count("*").alias("total_rows"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
)

# Group by and aggregate
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    round(avg("salary"), 2).alias("avg_salary_rounded")
)

# Multiple grouping columns
df.groupBy("department", "job_title").agg(
    count("*").alias("count"),
    sum("salary").alias("total_salary")
)

# ═══════════════════════════════════════════════════════════════════════════════
# WINDOW FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Ranking functions
df.withColumn("rank", rank().over(window_spec)) \
  .withColumn("dense_rank", dense_rank().over(window_spec)) \
  .withColumn("row_number", row_number().over(window_spec))

# Running totals
window_running = Window.partitionBy("department").orderBy("hire_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_salary_total", sum("salary").over(window_running))

# Lead and Lag
df.withColumn("prev_salary", lag("salary", 1).over(window_spec)) \
  .withColumn("next_salary", lead("salary", 1).over(window_spec))
```

### 12.5 Joins

```python
# ═══════════════════════════════════════════════════════════════════════════════
# JOIN TYPES
# ═══════════════════════════════════════════════════════════════════════════════

# Sample DataFrames
employees = spark.createDataFrame([
    (1, "John", 101),
    (2, "Jane", 102),
    (3, "Bob", None)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Marketing"),
    (103, "Sales")
], ["dept_id", "dept_name"])

# Inner Join (only matching rows)
employees.join(departments, employees.dept_id == departments.dept_id, "inner")

# Left Join (all from left, matching from right)
employees.join(departments, employees.dept_id == departments.dept_id, "left")

# Right Join (all from right, matching from left)
employees.join(departments, employees.dept_id == departments.dept_id, "right")

# Full Outer Join (all from both)
employees.join(departments, employees.dept_id == departments.dept_id, "full")

# Left Anti Join (left rows with no match)
employees.join(departments, employees.dept_id == departments.dept_id, "left_anti")

# Left Semi Join (left rows with match, no right columns)
employees.join(departments, employees.dept_id == departments.dept_id, "left_semi")

# ═══════════════════════════════════════════════════════════════════════════════
# JOIN BEST PRACTICES
# ═══════════════════════════════════════════════════════════════════════════════

# Avoid duplicate column names
result = employees.alias("e").join(
    departments.alias("d"),
    col("e.dept_id") == col("d.dept_id"),
    "left"
).select(
    col("e.emp_id"),
    col("e.name"),
    col("d.dept_name")
)

# Broadcast small tables for performance
from pyspark.sql.functions import broadcast

# If departments is small (< 10MB), broadcast it
result = employees.join(broadcast(departments), "dept_id", "left")
```

### 12.6 String Functions

```python
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# STRING MANIPULATION
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    ("  John Doe  ", "john.doe@email.com"),
    ("Jane Smith", "JANE.SMITH@EMAIL.COM")
], ["name", "email"])

df.select(
    # Case conversion
    upper(col("name")).alias("upper_name"),
    lower(col("email")).alias("lower_email"),
    initcap(col("name")).alias("title_case"),

    # Trimming
    trim(col("name")).alias("trimmed"),
    ltrim(col("name")).alias("left_trimmed"),
    rtrim(col("name")).alias("right_trimmed"),

    # Substring
    substring(col("name"), 1, 4).alias("first_4_chars"),

    # String length
    length(col("name")).alias("name_length"),

    # Replace
    regexp_replace(col("name"), " ", "_").alias("name_underscored"),

    # Split
    split(col("email"), "@").alias("email_parts"),
    split(col("email"), "@")[0].alias("email_username"),

    # Concatenation
    concat(col("name"), lit(" - "), col("email")).alias("combined"),
    concat_ws(" | ", col("name"), col("email")).alias("combined_sep")
)
```

### 12.7 Date and Time Functions

```python
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# DATE/TIME OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    ("2024-01-15", "2024-01-15 10:30:45")
], ["date_str", "timestamp_str"])

df.select(
    # Parse strings to date/timestamp
    to_date(col("date_str"), "yyyy-MM-dd").alias("date"),
    to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),

    # Current date/time
    current_date().alias("today"),
    current_timestamp().alias("now"),

    # Extract components
    year(col("date_str")).alias("year"),
    month(col("date_str")).alias("month"),
    dayofmonth(col("date_str")).alias("day"),
    dayofweek(col("date_str")).alias("day_of_week"),
    weekofyear(col("date_str")).alias("week"),
    quarter(col("date_str")).alias("quarter"),

    # Date arithmetic
    date_add(col("date_str"), 7).alias("plus_7_days"),
    date_sub(col("date_str"), 7).alias("minus_7_days"),
    add_months(col("date_str"), 1).alias("plus_1_month"),

    # Date difference
    datediff(current_date(), col("date_str")).alias("days_since"),
    months_between(current_date(), col("date_str")).alias("months_since"),

    # Formatting
    date_format(col("timestamp_str"), "MMMM dd, yyyy").alias("formatted_date"),
    date_format(col("timestamp_str"), "HH:mm").alias("time_only"),

    # Truncation
    trunc(col("date_str"), "MM").alias("first_of_month"),
    trunc(col("date_str"), "YY").alias("first_of_year")
)
```

### 12.8 Handling Null Values

```python
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# NULL HANDLING
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    (1, "John", 75000),
    (2, None, 65000),
    (3, "Bob", None)
], ["id", "name", "salary"])

# Filter nulls
df.filter(col("name").isNull())
df.filter(col("name").isNotNull())

# Replace nulls
df.fillna(0)                              # Replace all nulls with 0
df.fillna({"name": "Unknown", "salary": 0})  # Column-specific
df.na.fill({"name": "Unknown"})            # Alternative syntax

# Drop rows with nulls
df.dropna()                               # Drop if ANY column is null
df.dropna(how="all")                      # Drop if ALL columns are null
df.dropna(subset=["name", "salary"])      # Drop if specified columns are null

# Coalesce - return first non-null value
df.withColumn("name_clean", coalesce(col("name"), lit("Unknown")))

# When/Otherwise for complex logic
df.withColumn("salary_status",
    when(col("salary").isNull(), "Missing")
    .when(col("salary") < 50000, "Low")
    .when(col("salary") < 80000, "Medium")
    .otherwise("High")
)
```

### 12.9 Complex Data Types

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ═══════════════════════════════════════════════════════════════════════════════
# ARRAYS
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    (1, ["python", "java", "scala"]),
    (2, ["python", "sql"])
], ["id", "skills"])

df.select(
    col("skills"),
    size(col("skills")).alias("num_skills"),           # Array length
    col("skills")[0].alias("first_skill"),              # Index access
    array_contains(col("skills"), "python").alias("knows_python"),
    explode(col("skills")).alias("skill"),              # Flatten array
    array_distinct(col("skills")).alias("unique_skills")
)

# ═══════════════════════════════════════════════════════════════════════════════
# STRUCTS (Nested Objects)
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    (1, {"city": "NYC", "zip": "10001"}),
    (2, {"city": "LA", "zip": "90001"})
], ["id", "address"])

df.select(
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
    col("address")["city"].alias("city_alt")
)

# Create struct
df.select(
    struct(col("id"), lit("USA").alias("country")).alias("info")
)

# ═══════════════════════════════════════════════════════════════════════════════
# MAPS (Key-Value Pairs)
# ═══════════════════════════════════════════════════════════════════════════════

df = spark.createDataFrame([
    (1, {"email": "john@example.com", "phone": "555-0101"})
], ["id", "contacts"])

df.select(
    col("contacts")["email"].alias("email"),
    map_keys(col("contacts")).alias("contact_types"),
    map_values(col("contacts")).alias("contact_values")
)
```

### 12.10 User Defined Functions (UDFs)

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType
import pandas as pd

# ═══════════════════════════════════════════════════════════════════════════════
# STANDARD UDF (Slower - serialization overhead)
# ═══════════════════════════════════════════════════════════════════════════════

# Define Python function
def categorize_salary(salary):
    if salary is None:
        return "Unknown"
    elif salary < 50000:
        return "Low"
    elif salary < 80000:
        return "Medium"
    else:
        return "High"

# Register as UDF
categorize_salary_udf = udf(categorize_salary, StringType())

# Use in DataFrame
df.withColumn("salary_category", categorize_salary_udf(col("salary")))

# ═══════════════════════════════════════════════════════════════════════════════
# PANDAS UDF (Faster - vectorized operations)
# ═══════════════════════════════════════════════════════════════════════════════

@pandas_udf(StringType())
def categorize_salary_pandas(salary: pd.Series) -> pd.Series:
    return salary.apply(lambda x:
        "Unknown" if pd.isna(x)
        else "Low" if x < 50000
        else "Medium" if x < 80000
        else "High"
    )

# Use Pandas UDF
df.withColumn("salary_category", categorize_salary_pandas(col("salary")))

# ═══════════════════════════════════════════════════════════════════════════════
# REGISTER UDF FOR SQL
# ═══════════════════════════════════════════════════════════════════════════════

spark.udf.register("categorize_salary_sql", categorize_salary, StringType())

# Now use in SQL
spark.sql("""
    SELECT name, salary, categorize_salary_sql(salary) as category
    FROM employees
""")
```

### 12.11 Writing Data

```python
# ═══════════════════════════════════════════════════════════════════════════════
# WRITE MODES
# ═══════════════════════════════════════════════════════════════════════════════

# overwrite - Replace existing data
df.write.mode("overwrite").parquet("/path/to/output")

# append - Add to existing data
df.write.mode("append").parquet("/path/to/output")

# ignore - Skip if exists
df.write.mode("ignore").parquet("/path/to/output")

# error/errorifexists - Fail if exists (default)
df.write.mode("error").parquet("/path/to/output")

# ═══════════════════════════════════════════════════════════════════════════════
# WRITE FORMATS
# ═══════════════════════════════════════════════════════════════════════════════

# Parquet (recommended)
df.write.mode("overwrite").parquet("/path/to/data")

# Delta (best for data lakes)
df.write.format("delta").mode("overwrite").save("/path/to/delta")

# CSV
df.write.mode("overwrite").option("header", "true").csv("/path/to/csv")

# JSON
df.write.mode("overwrite").json("/path/to/json")

# Save as table
df.write.mode("overwrite").saveAsTable("catalog.schema.table_name")

# ═══════════════════════════════════════════════════════════════════════════════
# PARTITIONING
# ═══════════════════════════════════════════════════════════════════════════════

# Partition by columns
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("/path/to/partitioned")

# Output structure:
# /path/to/partitioned/
#   ├── year=2024/
#   │   ├── month=01/
#   │   │   └── part-00000.parquet
#   │   └── month=02/
#   │       └── part-00000.parquet

# Control number of output files
df.coalesce(1).write.parquet("/path")     # Single file (small data)
df.repartition(10).write.parquet("/path") # 10 files
```

---

## 13. Metadata-Driven Pipelines with Jinja2

### 13.1 What are Metadata-Driven Pipelines?

**Simple Explanation:** Instead of writing separate code for each table or transformation, you write ONE template and use metadata (configuration) to generate the specific code. It's like having a cake recipe template where you just change the flavor ingredients.

```traditional-vs-metadata
┌────────────────────────────────────────────────────────────────────────────────┐
│                    TRADITIONAL vs METADATA-DRIVEN                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   TRADITIONAL APPROACH (50 tables = 50 notebooks):                             │
│   ────────────────────────────────────────────────                             │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                │
│   │ customers.py    │  │ products.py     │  │ orders.py       │                │
│   │ ─────────────   │  │ ─────────────   │  │ ─────────────   │                │
│   │ df = read()     │  │ df = read()     │  │ df = read()     │                │
│   │ df = transform()│  │ df = transform()│  │ df = transform()│                │
│   │ df.write()      │  │ df.write()      │  │ df.write()      │                │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘                │
│   Problem: Duplicate code, hard to maintain                                    │
│                                                                                │
│   METADATA-DRIVEN APPROACH (1 template + config):                              │
│   ────────────────────────────────────────────────                             │
│   ┌─────────────────┐     ┌─────────────────────────────────┐                  │
│   │ Template.py     │ +   │ metadata.json                   │                  │
│   │ ─────────────   │     │ ──────────────                  │                  │
│   │ {source_table}  │     │ [{table: "customers", ...},     │                  │
│   │ {transforms}    │     │  {table: "products", ...},      │                  │
│   │ {target_table}  │     │  {table: "orders", ...}]        │                  │
│   └─────────────────┘     └─────────────────────────────────┘                  │
│   Benefit: Single source of truth, easy updates                                │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 13.2 Introduction to Jinja2

**What is Jinja2?** A templating engine for Python that lets you create dynamic text/code using variables and logic.

```python
# Install Jinja2 (in Databricks, it's pre-installed)
# pip install Jinja2

from jinja2 import Template

# Basic template example
template = Template("Hello, {{ name }}!")
result = template.render(name="World")
print(result)  # Output: Hello, World!
```

**Jinja2 Syntax Cheat Sheet:**

```jinja2-syntax
┌────────────────────────────────────────────────────────────────────────────────┐
│                    JINJA2 SYNTAX                                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   VARIABLES:                                                                   │
│   ──────────                                                                   │
│   {{ variable }}              → Output variable value                          │
│   {{ user.name }}             → Access attribute                               │
│   {{ items[0] }}              → Access list item                               │
│                                                                                │
│   CONTROL STRUCTURES:                                                          │
│   ───────────────────                                                          │
│   {% for item in items %}     → Loop                                           │
│   {% endfor %}                                                                 │
│                                                                                │
│   {% if condition %}          → Conditional                                    │
│   {% elif other %}                                                             │
│   {% else %}                                                                   │
│   {% endif %}                                                                  │
│                                                                                │
│   FILTERS:                                                                     │
│   ────────                                                                     │
│   {{ name | upper }}          → Uppercase                                      │
│   {{ name | lower }}          → Lowercase                                      │
│   {{ items | join(', ') }}    → Join list                                      │
│   {{ value | default('N/A') }}→ Default value                                  │
│                                                                                │
│   WHITESPACE CONTROL:                                                          │
│   ───────────────────                                                          │
│   {%- ... -%}                 → Trim whitespace                                │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 13.3 Creating a Metadata Configuration

```python
# metadata_config.py
# ─────────────────────────────────────────────────────────────────────────────

table_configs = [
    {
        "source_schema": "bronze",
        "source_table": "raw_customers",
        "target_schema": "silver",
        "target_table": "customers",
        "primary_key": "customer_id",
        "partition_columns": ["ingestion_date"],
        "transformations": [
            {"column": "email", "transform": "lower"},
            {"column": "first_name", "transform": "trim"},
            {"column": "last_name", "transform": "trim"},
            {"column": "full_name", "transform": "concat", "source_columns": ["first_name", "last_name"]}
        ],
        "filters": [
            {"column": "email", "condition": "isNotNull"}
        ]
    },
    {
        "source_schema": "bronze",
        "source_table": "raw_products",
        "target_schema": "silver",
        "target_table": "products",
        "primary_key": "product_id",
        "partition_columns": ["category"],
        "transformations": [
            {"column": "product_name", "transform": "trim"},
            {"column": "price", "transform": "cast", "data_type": "decimal(10,2)"}
        ],
        "filters": [
            {"column": "price", "condition": "> 0"}
        ]
    }
]
```

### 13.4 Building a Jinja2 Template for PySpark

```python
# transformation_template.py
# ─────────────────────────────────────────────────────────────────────────────

from jinja2 import Template

pyspark_template = """
# Auto-generated transformation for {{ config.target_table }}
# Generated at: {{ generation_time }}
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Read source data
df = spark.table("{{ catalog }}.{{ config.source_schema }}.{{ config.source_table }}")

# Apply transformations
df_transformed = df
{%- for transform in config.transformations %}

# Transform: {{ transform.column }}
{%- if transform.transform == 'lower' %}
df_transformed = df_transformed.withColumn("{{ transform.column }}", lower(col("{{ transform.column }}")))
{%- elif transform.transform == 'upper' %}
df_transformed = df_transformed.withColumn("{{ transform.column }}", upper(col("{{ transform.column }}")))
{%- elif transform.transform == 'trim' %}
df_transformed = df_transformed.withColumn("{{ transform.column }}", trim(col("{{ transform.column }}")))
{%- elif transform.transform == 'cast' %}
df_transformed = df_transformed.withColumn("{{ transform.column }}", col("{{ transform.column }}").cast("{{ transform.data_type }}"))
{%- elif transform.transform == 'concat' %}
df_transformed = df_transformed.withColumn("{{ transform.column }}", concat_ws(" ", {% for src_col in transform.source_columns %}col("{{ src_col }}"){{ ", " if not loop.last else "" }}{% endfor %}))
{%- endif %}
{%- endfor %}

# Apply filters
{%- for filter in config.filters %}
{%- if filter.condition == 'isNotNull' %}
df_transformed = df_transformed.filter(col("{{ filter.column }}").isNotNull())
{%- else %}
df_transformed = df_transformed.filter(col("{{ filter.column }}") {{ filter.condition }})
{%- endif %}
{%- endfor %}

# Add audit columns
df_final = df_transformed \\
    .withColumn("processed_timestamp", current_timestamp()) \\
    .withColumn("source_system", lit("{{ config.source_table }}"))

# Write to target (MERGE for idempotency)
target_path = "{{ catalog }}.{{ config.target_schema }}.{{ config.target_table }}"

if spark.catalog.tableExists(target_path):
    delta_table = DeltaTable.forName(spark, target_path)
    delta_table.alias("target").merge(
        df_final.alias("source"),
        "target.{{ config.primary_key }} = source.{{ config.primary_key }}"
    ).whenMatchedUpdateAll() \\
     .whenNotMatchedInsertAll() \\
     .execute()
else:
    df_final.write \\
        .format("delta") \\
        {%- if config.partition_columns %}
        .partitionBy({{ config.partition_columns | map('tojson') | join(', ') }}) \\
        {%- endif %}
        .saveAsTable(target_path)

print(f"Successfully processed {{ config.target_table }}")
"""

# Generate code for each table
template = Template(pyspark_template)
```

### 13.5 Generating and Executing Pipelines

```python
# pipeline_generator.py
# ─────────────────────────────────────────────────────────────────────────────

from jinja2 import Template
from datetime import datetime

class MetadataPipelineGenerator:
    def __init__(self, catalog: str, template_str: str):
        self.catalog = catalog
        self.template = Template(template_str)

    def generate_code(self, config: dict) -> str:
        """Generate PySpark code from metadata config"""
        return self.template.render(
            config=config,
            catalog=self.catalog,
            generation_time=datetime.now().isoformat()
        )

    def execute_pipeline(self, config: dict):
        """Generate and execute the pipeline"""
        code = self.generate_code(config)

        # Option 1: Execute directly
        exec(code)

        # Option 2: Save as notebook and run
        # self.save_as_notebook(config['target_table'], code)

    def run_all(self, configs: list):
        """Run pipelines for all configurations"""
        results = []
        for config in configs:
            try:
                self.execute_pipeline(config)
                results.append({"table": config['target_table'], "status": "success"})
            except Exception as e:
                results.append({"table": config['target_table'], "status": "failed", "error": str(e)})
        return results

# Usage
generator = MetadataPipelineGenerator(
    catalog="dev_catalog",
    template_str=pyspark_template
)

# Run all pipelines
results = generator.run_all(table_configs)
for r in results:
    print(r)
```

### 13.6 Advanced: SQL Template Generation

```python
# sql_template.py
# ─────────────────────────────────────────────────────────────────────────────

sql_view_template = """
-- Auto-generated view for {{ config.view_name }}
-- Source: {{ config.source_table }}
CREATE OR REPLACE VIEW {{ catalog }}.{{ config.target_schema }}.{{ config.view_name }} AS
SELECT
{%- for column in config.columns %}
    {%- if column.transformation %}
    {{ column.transformation }}({{ column.source_column }}) AS {{ column.alias }}{{ "," if not loop.last else "" }}
    {%- else %}
    {{ column.source_column }}{{ " AS " + column.alias if column.alias else "" }}{{ "," if not loop.last else "" }}
    {%- endif %}
{%- endfor %}
FROM {{ catalog }}.{{ config.source_schema }}.{{ config.source_table }}
{%- if config.where_clause %}
WHERE {{ config.where_clause }}
{%- endif %}
{%- if config.group_by %}
GROUP BY {{ config.group_by | join(', ') }}
{%- endif %}
;
"""

# Example config for SQL generation
view_configs = [
    {
        "source_schema": "silver",
        "source_table": "customers",
        "target_schema": "gold",
        "view_name": "v_customer_summary",
        "columns": [
            {"source_column": "customer_id", "alias": None, "transformation": None},
            {"source_column": "full_name", "alias": "customer_name", "transformation": "UPPER"},
            {"source_column": "email", "alias": None, "transformation": "LOWER"},
            {"source_column": "1", "alias": "customer_count", "transformation": "COUNT"}
        ],
        "where_clause": "email IS NOT NULL",
        "group_by": ["customer_id", "full_name", "email"]
    }
]

# Generate SQL
template = Template(sql_view_template)
for config in view_configs:
    sql = template.render(config=config, catalog="prod_catalog")
    print(sql)
    spark.sql(sql)
```

### 13.7 Configuration Management Patterns

```python
# config_manager.py
# ─────────────────────────────────────────────────────────────────────────────

import json
from typing import List, Dict

class ConfigManager:
    """Manage pipeline configurations from various sources"""

    @staticmethod
    def from_json_file(path: str) -> List[Dict]:
        """Load configs from JSON file"""
        with open(path, 'r') as f:
            return json.load(f)

    @staticmethod
    def from_delta_table(table_name: str) -> List[Dict]:
        """Load configs from Delta table"""
        df = spark.table(table_name)
        return [row.asDict() for row in df.collect()]

    @staticmethod
    def from_yaml_file(path: str) -> List[Dict]:
        """Load configs from YAML file"""
        import yaml
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    @staticmethod
    def validate_config(config: Dict) -> bool:
        """Validate required fields exist"""
        required_fields = ['source_table', 'target_table', 'primary_key']
        return all(field in config for field in required_fields)

# Store configs in Delta table for production
config_df = spark.createDataFrame([
    {
        "pipeline_id": "pl_customers",
        "source_table": "bronze.raw_customers",
        "target_table": "silver.customers",
        "primary_key": "customer_id",
        "config_json": json.dumps({"transformations": [...]}),
        "is_active": True,
        "created_at": "2024-01-15"
    }
])

config_df.write.format("delta").saveAsTable("config.pipeline_metadata")
```

### 13.8 Complete Metadata-Driven Pipeline

```python
# complete_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

from jinja2 import Environment, BaseLoader
from datetime import datetime
import json

class MetadataDrivenPipeline:
    """Complete metadata-driven pipeline framework"""

    def __init__(self, catalog: str):
        self.catalog = catalog
        self.env = Environment(loader=BaseLoader())

    def load_template(self, template_path: str) -> str:
        """Load template from DBFS or storage"""
        return spark.read.text(template_path).collect()[0][0]

    def load_configs(self, config_table: str) -> list:
        """Load active configurations from Delta table"""
        df = spark.table(config_table).filter("is_active = true")
        configs = []
        for row in df.collect():
            config = row.asDict()
            config['transformations'] = json.loads(config.get('config_json', '{}'))
            configs.append(config)
        return configs

    def process_table(self, config: dict, template_str: str) -> dict:
        """Process a single table based on config"""
        start_time = datetime.now()

        try:
            template = self.env.from_string(template_str)
            code = template.render(
                config=config,
                catalog=self.catalog,
                timestamp=start_time.isoformat()
            )

            # Execute generated code
            exec(code)

            return {
                "table": config['target_table'],
                "status": "success",
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }

        except Exception as e:
            return {
                "table": config['target_table'],
                "status": "failed",
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }

    def run_pipeline(self, config_table: str, template_path: str):
        """Execute full metadata-driven pipeline"""
        # Load template and configs
        template_str = self.load_template(template_path)
        configs = self.load_configs(config_table)

        print(f"Processing {len(configs)} tables...")

        # Process each table
        results = []
        for config in configs:
            result = self.process_table(config, template_str)
            results.append(result)
            print(f"  {result['table']}: {result['status']}")

        # Log results
        results_df = spark.createDataFrame(results)
        results_df.write.mode("append").saveAsTable("audit.pipeline_runs")

        return results

# Usage
pipeline = MetadataDrivenPipeline(catalog="prod_catalog")
results = pipeline.run_pipeline(
    config_table="config.pipeline_metadata",
    template_path="/dbfs/templates/transform_template.py"
)
```

---

## 14. Star Schema and Slowly Changing Dimensions (SCD)

### 14.1 What is Dimensional Data Modeling?

**Simple Explanation:** Dimensional modeling organizes data for easy analysis. Imagine organizing a library: instead of having one giant book with everything, you have a catalog (facts) that references separate sections (dimensions) for authors, genres, and publishers.

```dimensional-modeling
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DIMENSIONAL MODELING CONCEPTS                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   FACT TABLE (Measures/Events):                                                │
│   ─────────────────────────────                                                │
│   • Contains measurable business metrics                                       │
│   • Records business events/transactions                                       │
│   • Has foreign keys to dimension tables                                       │
│   • Examples: sales, orders, clicks, transactions                              │
│                                                                                │
│   DIMENSION TABLE (Context/Descriptors):                                       │
│   ──────────────────────────────────────                                       │
│   • Contains descriptive attributes                                            │
│   • Provides context to fact tables                                            │
│   • Usually smaller than fact tables                                           │
│   • Examples: customers, products, dates, locations                            │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 14.2 Star Schema Design

```star-schema
┌───────────────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA EXAMPLE                                        │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│                           ┌─────────────────┐                                 │
│                           │  dim_customer   │                                 │
│                           │  ─────────────  │                                 │
│                           │  customer_key   │◄──┐                             │
│                           │  customer_id    │   │                             │
│                           │  customer_name  │   │                             │
│                           │  email          │   │                             │
│                           │  city           │   │                             │
│                           └─────────────────┘   │                             │
│                                                 │                             │
│   ┌─────────────────┐    ┌─────────────────┐    │    ┌─────────────────┐      │
│   │   dim_product   │    │   FACT_SALES    │    │    │    dim_date     │      │
│   │   ───────────   │    │   ───────────   │    │    │   ──────────    │      │
│   │   product_key   │◄───│   product_key   │    │    │   date_key      │◄──┐  │
│   │   product_id    │    │   customer_key  │────┘    │   full_date     │   │  │
│   │   product_name  │    │   date_key      │─────────│   year          │   │  │
│   │   category      │    │   store_key     │────┐    │   quarter       │   │  │
│   │   brand         │    │   ───────────── │    │    │   month         │   │  │
│   └─────────────────┘    │   quantity      │    │    │   day_of_week   │   │  │
│                          │   unit_price    │    │    └─────────────────┘   │  │
│                          │   total_amount  │    │                          │  │
│                          │   discount      │    │    ┌─────────────────┐   │  │
│                          └─────────────────┘    │    │   dim_store     │   │  │
│                                                 │    │   ──────────    │   │  │
│                                                 └───▶│   store_key     │   │  │
│                                                      │   store_name    │   │  │
│                                                      │   region        │   │  │
│                                                      └─────────────────┘   │  │
│                                                                            │  │
│   The STAR shape: Fact table in center, dimensions around it like points   │  │
│                                                                            │  │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 14.3 Creating Dimension Tables

```python
# dim_date.py - Date Dimension
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

def create_date_dimension(start_date: str, end_date: str):
    """Create a comprehensive date dimension table"""

    # Generate date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = [(start + timedelta(days=x),) for x in range((end - start).days + 1)]

    df = spark.createDataFrame(dates, ["full_date"])

    dim_date = df.select(
        # Surrogate key
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),

        # Date values
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        weekofyear(col("full_date")).alias("week_of_year"),
        dayofmonth(col("full_date")).alias("day_of_month"),
        dayofweek(col("full_date")).alias("day_of_week"),
        dayofyear(col("full_date")).alias("day_of_year"),

        # Formatted values
        date_format(col("full_date"), "MMMM").alias("month_name"),
        date_format(col("full_date"), "MMM").alias("month_short"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        date_format(col("full_date"), "EEE").alias("day_short"),

        # Fiscal year (assuming July start)
        when(month(col("full_date")) >= 7, year(col("full_date")) + 1)
            .otherwise(year(col("full_date"))).alias("fiscal_year"),

        # Flags
        when(dayofweek(col("full_date")).isin([1, 7]), True)
            .otherwise(False).alias("is_weekend"),

        # Period indicators
        concat(lit("Q"), quarter(col("full_date"))).alias("quarter_name"),
        date_format(col("full_date"), "yyyy-MM").alias("year_month")
    )

    return dim_date

# Create and save
dim_date = create_date_dimension("2020-01-01", "2030-12-31")
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
```

```python
# dim_customer.py - Customer Dimension with SCD Type 2
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_customer_dimension(source_df):
    """Transform source customer data into dimension"""

    dim_customer = source_df.select(
        # Generate surrogate key
        monotonically_increasing_id().alias("customer_key"),

        # Natural key
        col("customer_id"),

        # Attributes
        col("first_name"),
        col("last_name"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
        lower(col("email")).alias("email"),
        col("phone"),
        col("address"),
        col("city"),
        col("state"),
        col("country"),

        # SCD Type 2 columns
        current_date().alias("effective_start_date"),
        lit("9999-12-31").cast("date").alias("effective_end_date"),
        lit(True).alias("is_current")
    )

    return dim_customer
```

### 14.4 Understanding Slowly Changing Dimensions

```scd-types
┌───────────────────────────────────────────────────────────────────────────────┐
│                    SCD TYPES EXPLAINED                                        │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   SCD TYPE 0 (Retain Original):                                               │
│   ─────────────────────────────                                               │
│   Never update dimension attributes. Keep original values forever.            │
│   Use case: Birth date, original signup date                                  │
│                                                                               │
│   SCD TYPE 1 (Overwrite):                                                     │
│   ───────────────────────                                                     │
│   Simply overwrite old values with new values. No history preserved.          │
│   Use case: Fixing typos, non-critical attributes                             │
│                                                                               │
│   SCD TYPE 2 (Add New Row):                                                   │
│   ─────────────────────────                                                   │
│   Create new row for each change. Preserves full history.                     │
│   Use case: Address changes, status changes, anything needing history         │
│                                                                               │
│   SCD TYPE 3 (Add New Column):                                                │
│   ────────────────────────────                                                │
│   Add column to store previous value. Limited history (usually 1 prior).      │
│   Use case: Need current and previous value only                              │
│                                                                               │
│   SCD TYPE 4 (History Table):                                                 │
│   ────────────────────────────                                                │
│   Separate table for history. Main table always current.                      │
│   Use case: Frequent changes, query performance critical                      │
│                                                                               │
│   SCD TYPE 6 (Hybrid 1+2+3):                                                  │
│   ──────────────────────────                                                  │
│   Combines types 1, 2, and 3 for maximum flexibility.                         │
│   Use case: Need history AND quick access to current values                   │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 14.5 SCD Type 1 Implementation

```python
# scd_type1.py - Overwrite changes
# ─────────────────────────────────────────────────────────────────────────────

from delta.tables import DeltaTable

def scd_type1_merge(source_df, target_table: str, key_columns: list, update_columns: list):
    """
    SCD Type 1: Overwrite old values with new values
    """

    # Build merge condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

    # Get target Delta table
    delta_table = DeltaTable.forName(spark, target_table)

    # Perform merge
    delta_table.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set={col: f"source.{col}" for col in update_columns}
    ).whenNotMatchedInsertAll().execute()

# Example usage
source_df = spark.table("bronze.customers")
scd_type1_merge(
    source_df=source_df,
    target_table="gold.dim_customer",
    key_columns=["customer_id"],
    update_columns=["email", "phone", "address", "city"]
)
```

### 14.6 SCD Type 2 Implementation

```python
# scd_type2.py - Full History Tracking
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *
from delta.tables import DeltaTable

def scd_type2_merge(
    source_df,
    target_table: str,
    key_column: str,
    tracked_columns: list,
    surrogate_key: str = "sk"
):
    """
    SCD Type 2: Create new row for each change, preserve history

    Required target columns:
    - surrogate_key (generated)
    - effective_start_date
    - effective_end_date
    - is_current
    """

    # Add processing columns to source
    source_with_hash = source_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c) for c in tracked_columns]))
    ).withColumn(
        "effective_start_date",
        current_date()
    ).withColumn(
        "effective_end_date",
        lit("9999-12-31").cast("date")
    ).withColumn(
        "is_current",
        lit(True)
    )

    # Get current records from target
    target_df = spark.table(target_table).filter("is_current = true")

    # Add hash to target for comparison
    target_with_hash = target_df.withColumn(
        "row_hash",
        md5(concat_ws("||", *[col(c) for c in tracked_columns]))
    )

    # Find changed records
    changed_records = source_with_hash.alias("s").join(
        target_with_hash.alias("t"),
        col(f"s.{key_column}") == col(f"t.{key_column}"),
        "left"
    ).filter(
        (col("t.row_hash").isNull()) |  # New records
        (col("s.row_hash") != col("t.row_hash"))  # Changed records
    ).select("s.*")

    # Records to expire (set is_current = false)
    records_to_expire = target_with_hash.alias("t").join(
        source_with_hash.alias("s"),
        col(f"t.{key_column}") == col(f"s.{key_column}"),
        "inner"
    ).filter(
        col("t.row_hash") != col("s.row_hash")
    ).select(
        col(f"t.{surrogate_key}"),
        col(f"t.{key_column}"),
        current_date().alias("effective_end_date")
    )

    # Get Delta table
    delta_table = DeltaTable.forName(spark, target_table)

    # Expire old records
    if records_to_expire.count() > 0:
        delta_table.alias("target").merge(
            records_to_expire.alias("expire"),
            f"target.{surrogate_key} = expire.{surrogate_key}"
        ).whenMatchedUpdate(
            set={
                "is_current": lit(False),
                "effective_end_date": col("expire.effective_end_date")
            }
        ).execute()

    # Insert new/changed records
    if changed_records.count() > 0:
        # Generate new surrogate keys
        max_sk = spark.table(target_table).agg(max(surrogate_key)).collect()[0][0] or 0

        new_records = changed_records.withColumn(
            surrogate_key,
            monotonically_increasing_id() + max_sk + 1
        ).drop("row_hash")

        new_records.write.format("delta").mode("append").saveAsTable(target_table)

    return {
        "expired_count": records_to_expire.count(),
        "new_count": changed_records.count()
    }

# Usage
result = scd_type2_merge(
    source_df=spark.table("silver.customers"),
    target_table="gold.dim_customer",
    key_column="customer_id",
    tracked_columns=["email", "phone", "address", "city", "state"],
    surrogate_key="customer_key"
)
print(f"Expired: {result['expired_count']}, New: {result['new_count']}")
```

### 14.7 Creating Fact Tables

```python
# fact_sales.py - Sales Fact Table
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql.functions import *

def create_fact_sales(sales_df, dim_customer, dim_product, dim_date, dim_store):
    """
    Create fact table by joining source data with dimension surrogate keys
    """

    fact_sales = (
        sales_df.alias("s")

        # Join customer dimension (get surrogate key)
        .join(
            dim_customer.filter("is_current = true").alias("c"),
            col("s.customer_id") == col("c.customer_id"),
            "left"
        )

        # Join product dimension
        .join(
            dim_product.filter("is_current = true").alias("p"),
            col("s.product_id") == col("p.product_id"),
            "left"
        )

        # Join date dimension
        .join(
            dim_date.alias("d"),
            to_date(col("s.order_date")) == col("d.full_date"),
            "left"
        )

        # Join store dimension
        .join(
            dim_store.filter("is_current = true").alias("st"),
            col("s.store_id") == col("st.store_id"),
            "left"
        )

        # Select fact columns with surrogate keys
        .select(
            # Surrogate keys (foreign keys to dimensions)
            col("c.customer_key"),
            col("p.product_key"),
            col("d.date_key"),
            col("st.store_key"),

            # Degenerate dimension (transaction ID)
            col("s.order_id"),

            # Measures
            col("s.quantity"),
            col("s.unit_price"),
            (col("s.quantity") * col("s.unit_price")).alias("gross_amount"),
            col("s.discount_amount"),
            (col("s.quantity") * col("s.unit_price") - col("s.discount_amount")).alias("net_amount"),

            # Audit columns
            current_timestamp().alias("etl_timestamp")
        )
    )

    return fact_sales

# Create fact table
fact = create_fact_sales(
    sales_df=spark.table("silver.sales_orders"),
    dim_customer=spark.table("gold.dim_customer"),
    dim_product=spark.table("gold.dim_product"),
    dim_date=spark.table("gold.dim_date"),
    dim_store=spark.table("gold.dim_store")
)

fact.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date_key") \
    .saveAsTable("gold.fact_sales")
```

### 14.8 Querying the Star Schema

```sql
-- Example analytics queries on star schema
-- ─────────────────────────────────────────────────────────────────────────────

-- Total sales by year and quarter
SELECT
    d.year,
    d.quarter_name,
    SUM(f.net_amount) as total_sales,
    COUNT(DISTINCT f.order_id) as order_count,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter_name
ORDER BY d.year, d.quarter_name;

-- Top 10 customers by revenue
SELECT
    c.full_name,
    c.city,
    SUM(f.net_amount) as total_revenue,
    COUNT(f.order_id) as order_count,
    AVG(f.net_amount) as avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.full_name, c.city
ORDER BY total_revenue DESC
LIMIT 10;

-- Product performance by category
SELECT
    p.category,
    p.brand,
    SUM(f.quantity) as units_sold,
    SUM(f.net_amount) as revenue,
    SUM(f.discount_amount) as total_discounts
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE p.is_current = true
GROUP BY p.category, p.brand
ORDER BY revenue DESC;

-- Historical customer analysis (using SCD Type 2)
SELECT
    c.customer_id,
    c.city,
    c.effective_start_date,
    c.effective_end_date,
    SUM(f.net_amount) as revenue_during_period
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE d.full_date BETWEEN c.effective_start_date AND c.effective_end_date
GROUP BY c.customer_id, c.city, c.effective_start_date, c.effective_end_date
ORDER BY c.customer_id, c.effective_start_date;
```

```star-schema-best-practices
┌────────────────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA BEST PRACTICES                                  │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  1. USE SURROGATE KEYS                                                         │
│     └── Never use natural keys as primary keys in dimensions                   │
│     └── Surrogate keys are stable even when business keys change               │
│                                                                                │
│  2. ALWAYS CREATE DIM_DATE                                                     │
│     └── Pre-build date dimension with all needed attributes                    │
│     └── Avoid date calculations at query time                                  │
│                                                                                │
│  3. HANDLE UNKNOWN MEMBERS                                                     │
│     └── Create "-1" or "0" key for missing dimension values                    │
│     └── Never have NULL foreign keys in fact tables                            │
│                                                                                │
│  4. GRAIN CONSISTENCY                                                          │
│     └── Define the grain (level of detail) clearly                             │
│     └── All facts in a fact table should be at the same grain                  │
│                                                                                │
│  5. PREFER ADDITIVE MEASURES                                                   │
│     └── Store raw values, calculate percentages at query time                  │
│     └── Makes aggregation simpler and more flexible                            │
│                                                                                │
│  6. PARTITION FACT TABLES                                                      │
│     └── Partition by date_key for large fact tables                            │
│     └── Improves query performance significantly                               │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 15. Databricks Delta Live Tables (DLT)

### 15.1 What are Delta Live Tables?

**Simple Explanation:** Delta Live Tables (DLT) is like hiring an autopilot for your data pipelines. Instead of writing code to handle every detail (errors, retries, dependencies), you just describe WHAT you want, and DLT figures out HOW to do it reliably.

```traditional-vs-dlt
┌────────────────────────────────────────────────────────────────────────────────┐
│                    TRADITIONAL PIPELINES vs DLT                                │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   TRADITIONAL (Imperative - HOW to do it):                                     │
│   ─────────────────────────────────────────                                    │
│   • Write code to read data                                                    │
│   • Handle errors manually                                                     │
│   • Manage dependencies yourself                                               │
│   • Write retry logic                                                          │
│   • Track data quality manually                                                │
│   • Manage checkpoints                                                         │
│                                                                                │
│   DLT (Declarative - WHAT you want):                                           │
│   ──────────────────────────────────                                           │
│   • Declare transformations                                                    │
│   • DLT handles errors                                                         │
│   • DLT resolves dependencies                                                  │
│   • Automatic retries                                                          │
│   • Built-in data quality (expectations)                                       │
│   • Automatic state management                                                 │
│                                                                                │
│   You write:          DLT manages:                                             │
│   ┌─────────────┐    ┌─────────────────────────────────────┐                   │
│   │ SELECT *    │ →  │ Scheduling, Recovery, Monitoring,   │                   │
│   │ FROM ...    │    │ Dependencies, Retries, Checkpoints  │                   │
│   └─────────────┘    └─────────────────────────────────────┘                   │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 15.2 DLT Core Concepts

```dlt-terminology
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DLT TERMINOLOGY                                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   PIPELINE                                                                     │
│   └── Collection of tables and their transformations                           │
│       └── Runs as a single unit                                                │
│       └── Manages dependencies automatically                                   │
│                                                                                │
│   STREAMING TABLE                                                              │
│   └── Processes data incrementally as it arrives                               │
│   └── Maintains checkpoints automatically                                      │
│   └── Best for: Append-only data, event streams                                │
│                                                                                │
│   MATERIALIZED VIEW                                                            │
│   └── Recomputes results when source data changes                              │
│   └── Best for: Aggregations, joins, transformations                           │
│                                                                                │
│   VIEW                                                                         │
│   └── Virtual table (not materialized)                                         │
│   └── Best for: Intermediate transformations                                   │
│                                                                                │
│   EXPECTATIONS                                                                 │
│   └── Data quality rules                                                       │
│   └── Can warn, drop bad records, or fail pipeline                             │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 15.3 Creating Your First DLT Pipeline

```python
# dlt_bronze_layer.py
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# BRONZE LAYER - Raw Data Ingestion
# ═══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from source system",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_customers():
    """Ingest raw customer data using Auto Loader"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/checkpoints/customers/schema")
        .load("/landing/customers/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )


@dlt.table(
    name="bronze_orders",
    comment="Raw order data from source system"
)
def bronze_orders():
    """Ingest raw order data"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema")
        .load("/landing/orders/")
        .withColumn("ingestion_timestamp", current_timestamp())
    )
```

### 15.4 Adding Data Quality with Expectations

```python
# dlt_silver_layer.py
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# SILVER LAYER - Cleaned and Validated Data
# ═══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer data with quality checks"
)
@dlt.expect("valid_email", "email IS NOT NULL AND email LIKE '%@%.%'")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
@dlt.expect_or_fail("unique_customer", "customer_id IS NOT NULL")
def silver_customers():
    """Clean and validate customer data"""
    return (
        dlt.read_stream("bronze_customers")
        .select(
            col("customer_id").cast("integer"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            col("phone"),
            col("address"),
            col("city"),
            col("state"),
            col("country"),
            col("ingestion_timestamp")
        )
        .withColumn("processed_timestamp", current_timestamp())
    )


@dlt.table(
    name="silver_orders",
    comment="Cleaned order data"
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_amount", "total_amount > 0")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
def silver_orders():
    """Clean and validate order data"""
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("integer"),
            col("customer_id").cast("integer"),
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
            col("product_id").cast("integer"),
            col("quantity").cast("integer"),
            col("unit_price").cast("decimal(10,2)"),
            (col("quantity") * col("unit_price")).alias("total_amount"),
            col("ingestion_timestamp")
        )
        .withColumn("processed_timestamp", current_timestamp())
    )
```

### 15.5 Expectation Types Explained

```dlt-expectations
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DLT EXPECTATION TYPES                                       │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  @dlt.expect("name", "condition")                                              │
│  ─────────────────────────────────                                             │
│  • Behavior: Log warning, keep ALL records                                     │
│  • Use when: Data quality issues are acceptable, want visibility               │
│  • Example: @dlt.expect("valid_email", "email LIKE '%@%'")                     │
│                                                                                │
│  @dlt.expect_or_drop("name", "condition")                                      │
│  ─────────────────────────────────────────                                     │
│  • Behavior: Drop records that fail the check                                  │
│  • Use when: Bad records should be excluded but pipeline continues             │
│  • Example: @dlt.expect_or_drop("has_id", "id IS NOT NULL")                    │
│                                                                                │
│  @dlt.expect_or_fail("name", "condition")                                      │
│  ─────────────────────────────────────────                                     │
│  • Behavior: Fail entire pipeline if ANY record fails                          │
│  • Use when: Critical data quality rules that cannot be violated               │
│  • Example: @dlt.expect_or_fail("unique_pk", "id IS NOT NULL")                 │
│                                                                                │
│  @dlt.expect_all({"name1": "cond1", "name2": "cond2"})                         │
│  ─────────────────────────────────────────────────────                         │
│  • Apply multiple expectations at once                                         │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 15.6 Gold Layer - Aggregations and Business Logic

```python
# dlt_gold_layer.py
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import *

# ═══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER - Business-Ready Aggregations
# ═══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_customer_orders",
    comment="Customer orders summary - materialized view"
)
def gold_customer_orders():
    """Join customers with their orders"""
    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")

    return (
        customers.alias("c")
        .join(orders.alias("o"), "customer_id", "left")
        .select(
            col("c.customer_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.email"),
            col("o.order_id"),
            col("o.order_date"),
            col("o.total_amount")
        )
    )


@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales aggregation"
)
def gold_daily_sales():
    """Daily sales summary"""
    return (
        dlt.read("silver_orders")
        .groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        )
    )


@dlt.table(
    name="gold_customer_lifetime_value",
    comment="Customer lifetime value calculation"
)
def gold_customer_lifetime_value():
    """Calculate customer lifetime metrics"""
    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")

    customer_metrics = (
        orders.groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            avg("total_amount").alias("avg_order_value")
        )
    )

    return (
        customers.join(customer_metrics, "customer_id", "left")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("lifetime_value"), lit(0)).alias("lifetime_value"),
            col("first_order_date"),
            col("last_order_date"),
            col("avg_order_value"),
            datediff(col("last_order_date"), col("first_order_date")).alias("customer_tenure_days")
        )
    )
```

### 15.7 DLT with Change Data Capture (CDC)

```python
# dlt_cdc.py - Handle incremental updates
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="customers_cdc",
    comment="Customer dimension with SCD Type 1 using CDC"
)
def customers_cdc():
    """Apply CDC changes to customer dimension"""
    return (
        dlt.read_stream("bronze_customers_cdc")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("operation"),  # INSERT, UPDATE, DELETE
            col("sequence_number")
        )
    )


# Apply changes using APPLY CHANGES INTO
dlt.create_streaming_table(
    name="silver_customers_scd",
    comment="Customer dimension with change tracking"
)

dlt.apply_changes(
    target="silver_customers_scd",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by="sequence_number",
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_number"],
    stored_as_scd_type=2  # or 1 for overwrite
)
```

### 15.8 Creating DLT Pipeline in Databricks

```json
{
    "name": "medallion_pipeline",
    "target": "sales_lakehouse",
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 4,
                "mode": "ENHANCED"
            }
        }
    ],
    "libraries": [
        {"notebook": {"path": "/Repos/project/dlt/bronze_layer"}},
        {"notebook": {"path": "/Repos/project/dlt/silver_layer"}},
        {"notebook": {"path": "/Repos/project/dlt/gold_layer"}}
    ],
    "configuration": {
        "source_path": "/landing/",
        "checkpoint_path": "/checkpoints/"
    }
}
```

### 15.9 DLT Pipeline Modes

```dlt-pipeline-modes
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DLT PIPELINE MODES                                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   TRIGGERED MODE (Default):                                                    │
│   ─────────────────────────                                                    │
│   • Runs once when manually triggered or scheduled                             │
│   • Processes all available data then stops                                    │
│   • Best for: Batch processing, scheduled runs                                 │
│                                                                                │
│   CONTINUOUS MODE:                                                             │
│   ────────────────                                                             │
│   • Runs continuously, processing data as it arrives                           │
│   • Low latency (near real-time)                                               │
│   • Best for: Real-time dashboards, streaming use cases                        │
│   • Set "continuous": true in pipeline config                                  │
│                                                                                │
│   DEVELOPMENT MODE:                                                            │
│   ──────────────────                                                           │
│   • Relaxed cluster termination                                                │
│   • Easier debugging                                                           │
│   • Set "development": true in pipeline config                                 │
│                                                                                │
│   PRODUCTION MODE:                                                             │
│   ─────────────────                                                            │
│   • Enhanced monitoring                                                        │
│   • Automatic retries                                                          │
│   • Set "development": false                                                   │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 15.10 Monitoring DLT Pipelines

```python
# Query DLT event logs for monitoring
# ─────────────────────────────────────────────────────────────────────────────

# DLT creates event logs automatically
event_log_path = "/pipelines/{pipeline_id}/system/events"

# Read event log
events_df = spark.read.format("delta").load(event_log_path)

# Check data quality metrics
quality_metrics = (
    events_df
    .filter("event_type = 'flow_progress'")
    .select(
        "timestamp",
        "origin.flow_name",
        "details:flow_progress.metrics.num_output_rows",
        "details:flow_progress.data_quality.expectations"
    )
)

display(quality_metrics)

# Check for failures
failures = (
    events_df
    .filter("event_type = 'flow_progress' AND details:flow_progress.status = 'FAILED'")
    .select(
        "timestamp",
        "origin.flow_name",
        "details:flow_progress.status",
        "error"
    )
)

display(failures)
```

```dlt-best-practices
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DLT BEST PRACTICES                                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  1. ORGANIZE BY LAYER                                                          │
│     └── Separate notebooks for bronze, silver, gold                            │
│     └── Makes debugging and maintenance easier                                 │
│                                                                                │
│  2. USE EXPECTATIONS WISELY                                                    │
│     └── Bronze: Log only (expect)                                              │
│     └── Silver: Drop bad records (expect_or_drop)                              │
│     └── Gold: Fail on critical issues (expect_or_fail)                         │
│                                                                                │
│  3. START WITH TRIGGERED, MOVE TO CONTINUOUS                                   │
│     └── Develop and test in triggered mode                                     │
│     └── Switch to continuous only when needed                                  │
│                                                                                │
│  4. MONITOR DATA QUALITY                                                       │
│     └── Review expectation metrics regularly                                   │
│     └── Set up alerts for quality degradation                                  │
│                                                                                │
│  5. USE MATERIALIZED VIEWS FOR AGGREGATIONS                                    │
│     └── Let DLT handle incremental refresh                                     │
│     └── More efficient than recomputing everything                             │
│                                                                                │
│  6. VERSION YOUR DLT NOTEBOOKS                                                 │
│     └── Use Databricks Repos or Git integration                                │
│     └── Enables CI/CD for pipelines                                            │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 16. Databricks Asset Bundles for CI/CD

### 16.1 What are Databricks Asset Bundles?

**Simple Explanation:** Asset Bundles (DABs) are like shipping containers for your Databricks projects. Just as shipping containers standardize how goods are packaged and transported, Asset Bundles standardize how you package and deploy notebooks, jobs, and pipelines across environments.

```traditional-vs-asset-bundles
┌────────────────────────────────────────────────────────────────────────────────┐
│                    TRADITIONAL vs ASSET BUNDLES DEPLOYMENT                     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   TRADITIONAL (Manual):                                                        │
│   ─────────────────────                                                        │
│   Developer → Copy notebooks manually → Configure jobs in UI → Deploy          │
│   • Error-prone                                                                │
│   • No version control for jobs                                                │
│   • Hard to replicate across environments                                      │
│                                                                                │
│   ASSET BUNDLES (Automated):                                                   │
│   ─────────────────────────                                                    │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│   │    Git      │───▶│   CI/CD     │───▶│   Bundle    │───▶│  Databricks │     │
│   │   Commit    │    │  Pipeline   │    │   Deploy    │    │  Workspace  │     │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │
│   • Version controlled                                                         │
│   • Automated deployment                                                       │
│   • Environment-specific configs                                               │
│   • Reproducible                                                               │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 16.2 Installing Databricks CLI

```bash
# Install Databricks CLI (v0.200+)
# ─────────────────────────────────────────────────────────────────────────────

# macOS (Homebrew)
brew tap databricks/tap
brew install databricks

# Windows (winget)
winget install Databricks.DatabricksCLI

# Linux (curl)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify installation
databricks --version

# Configure authentication
databricks configure --token
# Enter: Workspace URL (https://adb-xxx.azuredatabricks.net)
# Enter: Personal Access Token (generate from User Settings in workspace)

# Verify connection
databricks workspace list /
```

### 16.3 Project Structure

```asset-bundle-structure
┌────────────────────────────────────────────────────────────────────────────────┐
│                    ASSET BUNDLE PROJECT STRUCTURE                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   my-data-project/                                                             │
│   │                                                                            │
│   ├── databricks.yml              # Main bundle configuration                  │
│   │                                                                            │
│   ├── resources/                  # Resource definitions                       │
│   │   ├── jobs.yml               # Job configurations                          │
│   │   ├── pipelines.yml          # DLT pipeline configurations                 │
│   │   └── clusters.yml           # Cluster configurations                      │
│   │                                                                            │
│   ├── src/                       # Source code                                 │
│   │   ├── notebooks/             # Databricks notebooks                        │
│   │   │   ├── bronze/                                                          │
│   │   │   ├── silver/                                                          │
│   │   │   └── gold/                                                            │
│   │   ├── python/                # Python wheel packages                       │
│   │   │   └── my_package/                                                      │
│   │   └── sql/                   # SQL files                                   │
│   │                                                                            │
│   ├── tests/                     # Unit and integration tests                  │
│   │                                                                            │
│   └── .github/                   # GitHub Actions workflows                    │
│       └── workflows/                                                           │
│           └── deploy.yml                                                       │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 16.4 Creating databricks.yml

```yaml
# databricks.yml - Main bundle configuration
# ─────────────────────────────────────────────────────────────────────────────

bundle:
  name: sales-data-pipeline

# Include additional resource files
include:
  - resources/*.yml

# Variables for parameterization
variables:
  catalog:
    default: dev_catalog
  warehouse_id:
    description: "SQL Warehouse ID for queries"

# Workspace settings
workspace:
  host: https://adb-1234567890.azuredatabricks.net

# Environment-specific configurations
targets:
  # Development environment
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
    variables:
      catalog: dev_catalog

  # Staging environment
  staging:
    workspace:
      host: https://adb-staging.azuredatabricks.net
    variables:
      catalog: staging_catalog

  # Production environment
  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
    variables:
      catalog: prod_catalog
    run_as:
      service_principal_name: "sp-data-pipeline"
```

### 16.5 Defining Jobs

```yaml
# resources/jobs.yml
# ─────────────────────────────────────────────────────────────────────────────

resources:
  jobs:
    # Daily ETL Job
    daily_etl_job:
      name: "daily-etl-${bundle.target}"
      description: "Daily ETL pipeline for sales data"

      # Schedule
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
        timezone_id: "UTC"

      # Job clusters
      job_clusters:
        - job_cluster_key: "etl-cluster"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"

      # Tasks
      tasks:
        # Task 1: Bronze ingestion
        - task_key: "bronze_ingestion"
          job_cluster_key: "etl-cluster"
          notebook_task:
            notebook_path: "../src/notebooks/bronze/ingest_data.py"
            base_parameters:
              catalog: "${var.catalog}"
              source_path: "/landing/sales/"

        # Task 2: Silver transformation
        - task_key: "silver_transformation"
          depends_on:
            - task_key: "bronze_ingestion"
          job_cluster_key: "etl-cluster"
          notebook_task:
            notebook_path: "../src/notebooks/silver/transform_data.py"
            base_parameters:
              catalog: "${var.catalog}"

        # Task 3: Gold aggregation
        - task_key: "gold_aggregation"
          depends_on:
            - task_key: "silver_transformation"
          job_cluster_key: "etl-cluster"
          notebook_task:
            notebook_path: "../src/notebooks/gold/aggregate_data.py"
            base_parameters:
              catalog: "${var.catalog}"

      # Email notifications
      email_notifications:
        on_failure:
          - data-team@company.com
        on_success:
          - data-team@company.com

      # Tags
      tags:
        project: "sales-pipeline"
        environment: "${bundle.target}"
```

### 16.6 Defining DLT Pipelines

```yaml
# resources/pipelines.yml
# ─────────────────────────────────────────────────────────────────────────────

resources:
  pipelines:
    # Sales DLT Pipeline
    sales_pipeline:
      name: "sales-dlt-pipeline-${bundle.target}"
      target: "${var.catalog}.sales"
      development: true
      continuous: false
      channel: "CURRENT"

      clusters:
        - label: "default"
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: "ENHANCED"

      libraries:
        - notebook:
            path: "../src/notebooks/dlt/bronze_layer.py"
        - notebook:
            path: "../src/notebooks/dlt/silver_layer.py"
        - notebook:
            path: "../src/notebooks/dlt/gold_layer.py"

      configuration:
        source_path: "/landing/sales/"
        checkpoint_path: "/checkpoints/sales/"

      # Photon acceleration
      photon: true

      # Notifications
      notifications:
        - email_recipients:
            - data-team@company.com
          alerts:
            - on_update_failure
            - on_flow_failure
```

### 16.7 Bundle CLI Commands

```bash
# Bundle Development Workflow
# ─────────────────────────────────────────────────────────────────────────────

# Initialize a new bundle project
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy bundle to workspace (dev by default)
databricks bundle deploy

# Deploy to specific target
databricks bundle deploy --target staging
databricks bundle deploy --target prod

# Run a specific job
databricks bundle run daily_etl_job

# Run with parameters override
databricks bundle run daily_etl_job --params '{"date": "2024-01-15"}'

# Destroy bundle resources
databricks bundle destroy

# Sync local files to workspace (development)
databricks bundle sync
```

### 16.8 GitHub Actions CI/CD Pipeline

```yaml
# .github/workflows/deploy.yml
# ─────────────────────────────────────────────────────────────────────────────

name: Deploy Databricks Bundle

on:
  push:
    branches:
      - main        # Production
      - develop     # Staging
  pull_request:
    branches:
      - main
      - develop

env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

jobs:
  # Validation job
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Validate bundle
        run: databricks bundle validate

  # Test job
  test:
    needs: validate
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install pytest pyspark

      - name: Run unit tests
        run: pytest tests/ -v

  # Deploy to staging (on develop branch)
  deploy-staging:
    needs: test
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Deploy to staging
        run: databricks bundle deploy --target staging

  # Deploy to production (on main branch)
  deploy-production:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Deploy to production
        run: databricks bundle deploy --target prod

      - name: Run production job
        run: databricks bundle run daily_etl_job --target prod
```

### 16.9 Azure DevOps Pipeline

```yaml
# azure-pipelines.yml
# ─────────────────────────────────────────────────────────────────────────────

trigger:
  branches:
    include:
      - main
      - develop

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: databricks-credentials  # Variable group with secrets

stages:
  # Validation Stage
  - stage: Validate
    displayName: 'Validate Bundle'
    jobs:
      - job: ValidateBundle
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.10'

          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
            displayName: 'Install Databricks CLI'

          - script: |
              databricks bundle validate
            displayName: 'Validate Bundle'
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  # Deploy to Staging
  - stage: DeployStaging
    displayName: 'Deploy to Staging'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
    jobs:
      - deployment: DeployToStaging
        environment: 'staging'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self

                - script: |
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target staging
                  displayName: 'Deploy Bundle to Staging'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST_STAGING)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_STAGING)

  # Deploy to Production
  - stage: DeployProduction
    displayName: 'Deploy to Production'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployToProduction
        environment: 'production'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self

                - script: |
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target prod
                  displayName: 'Deploy Bundle to Production'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST_PROD)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_PROD)
```

### 16.10 Best Practices

```asset-bundle-best-practices
┌────────────────────────────────────────────────────────────────────────────────┐
│                    ASSET BUNDLES BEST PRACTICES                                │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  1. USE VARIABLES FOR ENVIRONMENT-SPECIFIC VALUES                              │
│     └── Catalog names, paths, cluster sizes                                    │
│     └── Avoid hardcoding environment-specific values                           │
│                                                                                │
│  2. SEPARATE RESOURCES INTO MULTIPLE FILES                                     │
│     └── jobs.yml, pipelines.yml, clusters.yml                                  │
│     └── Easier to manage and review                                            │
│                                                                                │
│  3. USE SERVICE PRINCIPALS FOR PRODUCTION                                      │
│     └── Never use personal tokens in production                                │
│     └── Configure run_as in prod target                                        │
│                                                                                │
│  4. IMPLEMENT PROPER TESTING                                                   │
│     └── Unit tests for Python code                                             │
│     └── Integration tests before production deploy                             │
│                                                                                │
│  5. USE ENVIRONMENT PROTECTION RULES                                           │
│     └── Require approval for production deployments                            │
│     └── Use GitHub/Azure DevOps environments                                   │
│                                                                                │
│  6. VERSION CONTROL EVERYTHING                                                 │
│     └── databricks.yml, resources, notebooks                                   │
│     └── Never modify production resources manually                             │
│                                                                                │
│  7. IMPLEMENT ROLLBACK STRATEGY                                                │
│     └── Tag successful deployments                                             │
│     └── Have a process to revert to previous version                           │
│                                                                                │
│  8. MONITOR DEPLOYMENTS                                                        │
│     └── Set up alerts for failed deployments                                   │
│     └── Track deployment history                                               │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## Conclusion

Congratulations! You've completed this comprehensive Azure Data Engineering guide covering:

1. **Azure Fundamentals** - Setting up your cloud environment
2. **Azure Data Factory** - Building ETL/ELT pipelines
3. **Incremental Ingestion** - Efficient data loading patterns
4. **Looping & Logic Apps** - Advanced orchestration
5. **Azure Databricks** - Big data processing platform
6. **Unity Catalog** - Data governance and security
7. **Spark Streaming** - Real-time data processing
8. **PySpark Transformations** - Data manipulation at scale
9. **Metadata-Driven Pipelines** - Template-based automation
10. **Star Schema & SCD** - Dimensional data modeling
11. **Delta Live Tables** - Declarative data pipelines
12. **Asset Bundles** - CI/CD for Databricks

```next-steps
┌────────────────────────────────────────────────────────────────────────────────┐
│                         NEXT STEPS                                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  1. Build a complete project using these concepts                              │
│  2. Explore Azure Synapse Analytics for enterprise scenarios                   │
│  3. Learn about data mesh and data products                                    │
│  4. Implement data quality frameworks                                          │
│  5. Explore machine learning integration with MLflow                           │
│  6. Study for Azure Data Engineer certification (DP-203)                       │
│                                                                                │
│  Resources:                                                                    │
│  • Microsoft Learn: https://learn.microsoft.com/azure/                         │
│  • Databricks Documentation: https://docs.databricks.com/                      │
│  • Delta Lake: https://delta.io/                                               │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

---
