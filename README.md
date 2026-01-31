# Azure Data Engineering - Complete Guide

> A comprehensive guide to building end-to-end data engineering solutions using Azure Data Factory, Databricks, Unity Catalog, Delta Live Tables, and more.

---

## Table of Contents

- [Azure Data Engineering - Complete Guide](#azure-data-engineering---complete-guide)
  - [Table of Contents](#table-of-contents)
  - [1. Introduction](#1-introduction)
    - [What is Data Engineering?](#what-is-data-engineering)
    - [Why Azure for Data Engineering?](#why-azure-for-data-engineering)
  - [2. Project Architecture](#2-project-architecture)
    - [High-Level Architecture Diagram](#high-level-architecture-diagram)
    - [Medallion Architecture](#medallion-architecture)
  - [3. Azure Fundamentals](#3-azure-fundamentals)
    - [3.1 Creating a Free Azure Account](#31-creating-a-free-azure-account)
    - [3.2 Azure Portal Layout](#32-azure-portal-layout)
    - [3.3 Resource Groups Explained](#33-resource-groups-explained)
    - [3.4 Creating Resources via Azure CLI](#34-creating-resources-via-azure-cli)
    - [Naming Convention](#naming-convention)

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

| Tool | Purpose | Analogy |
|------|---------|---------|
| Azure Data Factory | Orchestration & ETL | The conductor of an orchestra |
| Azure SQL Database | Relational data storage | A well-organized filing cabinet |
| Azure Databricks | Big data processing | A powerful calculator on steroids |
| Unity Catalog | Data governance | The librarian who tracks all books |
| Delta Live Tables | Declarative pipelines | An autopilot for data flows |

---

## 2. Project Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AZURE DATA ENGINEERING PROJECT                         │
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

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            MEDALLION ARCHITECTURE                               │
│                                                                                 │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌───────────┐  │
│  │             │      │             │      │             │      │           │  │
│  │   SOURCE    │─────▶│   BRONZE    │─────▶│   SILVER    │─────▶│   GOLD    │  │
│  │   (Raw)     │      │   (Landing) │      │   (Clean)   │      │  (Curated)│  │
│  │             │      │             │      │             │      │           │  │
│  └─────────────┘      └─────────────┘      └─────────────┘      └───────────┘  │
│        │                    │                    │                    │        │
│        ▼                    ▼                    ▼                    ▼        │
│   Raw data as-is      Exact copy of       Cleaned, filtered,    Business-     │
│   from sources        source data         validated data         ready data    │
│                       (append-only)       (deduplicated)        (aggregated)   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**For Beginners - The Gold Refining Analogy:**

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

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ┌──────────┐                              AZURE PORTAL                         │
│  │ Search   │  ← Search for any Azure service                                   │
│  └──────────┘                                                                   │
│  ┌─────────────────┐  ┌────────────────────────────────────────────────────┐   │
│  │  NAVIGATION     │  │              MAIN CONTENT AREA                     │   │
│  │  • Home         │  │   - Resource Groups                                │   │
│  │  • Dashboard    │  │   - Data Factory                                   │   │
│  │  • All Services │  │   - Databricks                                     │   │
│  │  • Resources    │  │   - Storage Accounts                               │   │
│  └─────────────────┘  └────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Resource Groups Explained

**Simple Explanation:** A Resource Group is like a folder on your computer - you put related resources together.

```
┌─────────────────────────────────────────────┐
│        Resource Group: "data-eng-rg"        │
├─────────────────────────────────────────────┤
│   ┌─────────────┐   ┌─────────────┐        │
│   │ Data Factory│   │ Databricks  │        │
│   └─────────────┘   └─────────────┘        │
│   ┌─────────────┐   ┌─────────────┐        │
│   │ SQL Database│   │ Storage Acct│        │
│   └─────────────┘   └─────────────┘        │
└─────────────────────────────────────────────┘
```

### 3.4 Creating Resources via Azure CLI

```bash
# Login to Azure
az login

# Create Resource Group
az group create \
    --name "data-eng-rg" \
    --location "eastus"

# Create Storage Account
az storage account create \
    --name "stdataengdev001" \
    --resource-group "data-eng-rg" \
    --location "eastus" \
    --sku "Standard_LRS"

# Create Data Factory
az datafactory create \
    --name "adf-dataeng-dev-001" \
    --resource-group "data-eng-rg" \
    --location "eastus"
```

### Naming Convention

```
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

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AZURE DATA FACTORY COMPONENTS                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌──────────────┐                                                              │
│   │   PIPELINE   │  ← Container for activities (like a recipe)                  │
│   │   ┌────────┐ │                                                              │
│   │   │Activity│ │  ← Individual step/task (like an instruction in recipe)     │
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
   - **Resource Group:** data-eng-rg
   - **Name:** adf-dataeng-dev-001
   - **Region:** East US
   - **Version:** V2

**Via Azure CLI:**

```bash
# Create Data Factory
az datafactory create \
    --name "adf-dataeng-dev-001" \
    --resource-group "data-eng-rg" \
    --location "eastus"

# Verify creation
az datafactory show \
    --name "adf-dataeng-dev-001" \
    --resource-group "data-eng-rg"
```

### 4.3 Understanding the ADF Studio Interface

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ADF STUDIO INTERFACE                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐                                                              │
│  │  AUTHOR       │ ← Create pipelines, datasets, dataflows                      │
│  ├───────────────┤                                                              │
│  │  MONITOR      │ ← Watch pipeline runs, debug issues                          │
│  ├───────────────┤                                                              │
│  │  MANAGE       │ ← Linked services, integration runtimes, triggers            │
│  └───────────────┘                                                              │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        PIPELINE CANVAS                                   │   │
│  │   ┌─────────┐      ┌─────────┐      ┌─────────┐                         │   │
│  │   │ Source  │─────▶│ Process │─────▶│  Sink   │                         │   │
│  │   │  (Copy) │      │(Transform)     │ (Store) │                         │   │
│  │   └─────────┘      └─────────┘      └─────────┘                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Pipeline Activities - Deep Dive

**Common Activity Types:**

| Activity | Purpose | Use Case |
|----------|---------|----------|
| **Copy Data** | Move data between stores | SQL to Blob, API to Data Lake |
| **Data Flow** | Transform data visually | Complex ETL transformations |
| **Lookup** | Read config/metadata | Get table names, connection strings |
| **ForEach** | Loop over items | Process multiple tables |
| **If Condition** | Branching logic | Run different paths based on conditions |
| **Execute Pipeline** | Call other pipelines | Modular pipeline design |
| **Set Variable** | Store values | Pass data between activities |
| **Web** | Call REST APIs | Trigger external services |
| **Stored Procedure** | Run SQL procedures | Execute database logic |

### 4.5 Creating Your First Pipeline

**Scenario:** Copy data from Azure SQL Database to Azure Data Lake Storage

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                        FIRST PIPELINE ARCHITECTURE                              │
│                                                                                 │
│   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐          │
│   │  Azure SQL   │────────▶│  Copy Data   │────────▶│  Data Lake   │          │
│   │  Database    │         │  Activity    │         │  Storage     │          │
│   │  (Source)    │         │              │         │  (Sink)      │          │
│   └──────────────┘         └──────────────┘         └──────────────┘          │
│         │                        │                        │                    │
│         ▼                        ▼                        ▼                    │
│   Linked Service:          Mapping:                Linked Service:             │
│   SQL Connection           Column mappings         ADLS Connection             │
│                            Data types                                          │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Step 1: Create Linked Services**

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
        "url": "https://stdataengdev001.dfs.core.windows.net/"
    }
}
```

**Step 2: Create Datasets**

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

**Step 3: Create Pipeline**

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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                         PARAMETERIZED PIPELINE                                  │
│                                                                                 │
│   Parameters:                                                                   │
│   ┌─────────────────────────────────────────────────────────┐                  │
│   │  schema_name    = "SalesLT"                             │                  │
│   │  table_name     = "Customer"                            │                  │
│   │  output_folder  = "bronze/customers"                    │                  │
│   └─────────────────────────────────────────────────────────┘                  │
│                              │                                                  │
│                              ▼                                                  │
│   ┌──────────────────────────────────────────────────────┐                     │
│   │              Copy Data Activity                       │                     │
│   │   Source: @{pipeline().parameters.schema_name}.       │                     │
│   │           @{pipeline().parameters.table_name}         │                     │
│   │   Sink:   @{pipeline().parameters.output_folder}/     │                     │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              TRIGGER TYPES                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    Run at specific times (cron-style)                     │
│  │    SCHEDULE     │    Example: Every day at 2 AM                             │
│  │    TRIGGER      │    "0 0 2 * * *"                                          │
│  └─────────────────┘                                                           │
│                                                                                 │
│  ┌─────────────────┐    Run when window of time completes                      │
│  │    TUMBLING     │    Example: Process hourly batches                        │
│  │    WINDOW       │    Good for: Incremental processing                       │
│  └─────────────────┘                                                           │
│                                                                                 │
│  ┌─────────────────┐    Run when file arrives                                  │
│  │    EVENT        │    Example: New CSV in blob storage                       │
│  │    TRIGGER      │    Good for: Real-time ingestion                          │
│  └─────────────────┘                                                           │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                         EXPRESSION CHEAT SHEET                                  │
├────────────────────────────────────────────────────────────────────────────────┤
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
│  @formatDateTime(utcnow(),'yyyy-MM-dd')  → Format: 2024-01-15                  │
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
└────────────────────────────────────────────────────────────────────────────────┘
```

**Practical Example - Dynamic File Path:**

```
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                          MONITORING DASHBOARD                                   │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Pipeline Runs:                                                                 │
│  ┌──────────────┬────────────┬──────────┬──────────┬──────────────┐           │
│  │ Pipeline     │ Start Time │ Duration │ Status   │ Triggered By │           │
│  ├──────────────┼────────────┼──────────┼──────────┼──────────────┤           │
│  │ pl_copy_cust │ 02:00:00   │ 5m 23s   │ ✓ Success│ Schedule     │           │
│  │ pl_copy_prod │ 02:05:00   │ 12m 45s  │ ✓ Success│ Schedule     │           │
│  │ pl_copy_ord  │ 02:15:00   │ --       │ ✗ Failed │ Schedule     │           │
│  └──────────────┴────────────┴──────────┴──────────┴──────────────┘           │
│                                                                                 │
│  Activity Runs (drill down):                                                    │
│  ┌──────────────┬──────────────┬──────────┬─────────────────────────┐         │
│  │ Activity     │ Input/Output │ Duration │ Error Message           │         │
│  ├──────────────┼──────────────┼──────────┼─────────────────────────┤         │
│  │ Lookup       │ {...}        │ 2s       │ --                      │         │
│  │ Copy Data    │ {...}        │ --       │ Connection timeout      │         │
│  └──────────────┴──────────────┴──────────┴─────────────────────────┘         │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                     AZURE SQL DATABASE CONCEPTS                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   Traditional Database (On-Premises)     Azure SQL Database (Cloud)            │
│   ───────────────────────────────────    ──────────────────────────           │
│   • You manage the server               • Microsoft manages the server         │
│   • You handle updates/patches          • Auto-updates & patches               │
│   • You provision hardware              • Elastic scaling                       │
│   • You set up backups                  • Automatic backups (35 days)          │
│   • Limited by physical hardware        • Scale up/down on demand              │
│                                                                                 │
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
    --resource-group "data-eng-rg" \
    --location "eastus" \
    --admin-user "sqladmin" \
    --admin-password "YourSecurePassword123!"

# Create SQL Database
az sql db create \
    --name "sqldb-adventureworks" \
    --server "sql-dataeng-dev-001" \
    --resource-group "data-eng-rg" \
    --edition "Basic"

# Configure firewall (allow Azure services)
az sql server firewall-rule create \
    --name "AllowAzureServices" \
    --server "sql-dataeng-dev-001" \
    --resource-group "data-eng-rg" \
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    FULL LOAD vs INCREMENTAL LOAD                                │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  FULL LOAD (Simple but Inefficient):                                           │
│  ────────────────────────────────────                                          │
│  Day 1: Copy 1,000,000 rows    [████████████████] 100%                         │
│  Day 2: Copy 1,000,100 rows    [████████████████] 100%  ← Re-copy everything!  │
│  Day 3: Copy 1,000,200 rows    [████████████████] 100%  ← Again!               │
│                                                                                 │
│  Problem: Wasting time & money copying unchanged data                           │
│                                                                                 │
│  INCREMENTAL LOAD (Efficient):                                                  │
│  ─────────────────────────────                                                  │
│  Day 1: Copy 1,000,000 rows    [████████████████] 100%  ← Initial load         │
│  Day 2: Copy 100 new rows      [█               ] 0.01% ← Only changes!        │
│  Day 3: Copy 100 new rows      [█               ] 0.01% ← Only changes!        │
│                                                                                 │
│  Benefit: 99.99% reduction in data transfer                                     │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Watermark Pattern

**Simple Explanation:** A watermark is like a bookmark in a book. It tells you where you left off, so you don't have to start from the beginning.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                         WATERMARK PATTERN                                       │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Watermark Table (tracks progress):                                             │
│  ┌─────────────┬─────────────────────┬─────────────────────┐                   │
│  │ Table Name  │ Last Watermark      │ Updated At          │                   │
│  ├─────────────┼─────────────────────┼─────────────────────┤                   │
│  │ Customer    │ 2024-01-14 23:59:59 │ 2024-01-15 02:00:00 │                   │
│  │ Product     │ 2024-01-14 23:59:59 │ 2024-01-15 02:05:00 │                   │
│  │ SalesOrder  │ 2024-01-14 23:59:59 │ 2024-01-15 02:10:00 │                   │
│  └─────────────┴─────────────────────┴─────────────────────┘                   │
│                                                                                 │
│  How it works:                                                                  │
│  ─────────────                                                                  │
│  1. Read last watermark: 2024-01-14 23:59:59                                   │
│  2. Query source: WHERE ModifiedDate > '2024-01-14 23:59:59'                   │
│  3. Copy only new/modified records                                              │
│  4. Update watermark to max(ModifiedDate) from copied data                      │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL INGESTION PIPELINE                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐ │
│   │  Lookup     │────▶│  Lookup     │────▶│  Copy Data  │────▶│  Stored     │ │
│   │  Old        │     │  New        │     │  (Delta)    │     │  Procedure  │ │
│   │  Watermark  │     │  Watermark  │     │             │     │  (Update    │ │
│   │             │     │             │     │             │     │  Watermark) │ │
│   └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘ │
│         │                   │                   │                   │         │
│         ▼                   ▼                   ▼                   ▼         │
│   Get last loaded     Get max(Modified    Copy WHERE           Update         │
│   timestamp from      Date) from source   Modified > old       watermark      │
│   watermark table                         AND <= new           table          │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                         BACKFILL PIPELINE FLOW                                  │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   Parameters:                                                                   │
│   ┌─────────────────────────────────────────┐                                  │
│   │  is_backfill     = true/false           │                                  │
│   │  backfill_start  = "2024-01-01"         │                                  │
│   │  backfill_end    = "2024-01-15"         │                                  │
│   └─────────────────────────────────────────┘                                  │
│                         │                                                       │
│                         ▼                                                       │
│   ┌─────────────────────────────────────────┐                                  │
│   │         IF CONDITION                    │                                  │
│   │    is_backfill == true?                 │                                  │
│   └────────────────┬────────────────────────┘                                  │
│            Yes     │     No                                                     │
│             ▼      │      ▼                                                     │
│   ┌────────────┐   │   ┌────────────┐                                          │
│   │ Use custom │   │   │ Use normal │                                          │
│   │ date range │   │   │ watermark  │                                          │
│   └────────────┘   │   └────────────┘                                          │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    WITHOUT LOOPING vs WITH LOOPING                              │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  WITHOUT LOOPING (50 tables = 50 activities):                                  │
│  ──────────────────────────────────────────────                                │
│  ┌────────┐ ┌────────┐ ┌────────┐       ┌────────┐                            │
│  │ Copy   │ │ Copy   │ │ Copy   │  ...  │ Copy   │                            │
│  │ Table1 │ │ Table2 │ │ Table3 │       │Table50 │                            │
│  └────────┘ └────────┘ └────────┘       └────────┘                            │
│  Problem: Hard to maintain, duplicate logic                                     │
│                                                                                 │
│  WITH LOOPING (50 tables = 1 ForEach activity):                                │
│  ───────────────────────────────────────────────                               │
│  ┌─────────────────────────────────────────────┐                               │
│  │  ForEach (items: [Table1, Table2, ... ])    │                               │
│  │  ┌────────────────────────────────────────┐ │                               │
│  │  │  Copy @{item().tableName}              │ │  ← Runs 50 times             │
│  │  └────────────────────────────────────────┘ │                               │
│  └─────────────────────────────────────────────┘                               │
│  Benefit: Single source of truth, easy maintenance                              │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 ForEach Activity Deep Dive

**Key Properties:**

| Property | Description | Example |
|----------|-------------|---------|
| `items` | Array to iterate over | `["Customer", "Product", "Order"]` |
| `isSequential` | Run items one by one or in parallel | `false` (parallel) |
| `batchCount` | Max parallel executions (1-50) | `20` |
| `activities` | What to do for each item | Copy, Execute Pipeline, etc. |

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    FOREACH EXECUTION MODES                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  SEQUENTIAL (isSequential = true):                                             │
│  ─────────────────────────────────                                             │
│  Item 1 ────▶ Item 2 ────▶ Item 3 ────▶ Item 4 ────▶ Done                     │
│  (5 min)      (5 min)      (5 min)      (5 min)      Total: 20 min            │
│                                                                                 │
│  PARALLEL (isSequential = false, batchCount = 4):                              │
│  ────────────────────────────────────────────────                              │
│  Item 1 ─┐                                                                      │
│  Item 2 ─┼────▶ All complete ────▶ Done                                        │
│  Item 3 ─┤      (5 min)            Total: 5 min                                │
│  Item 4 ─┘                                                                      │
│                                                                                 │
│  Speedup: 4x faster with parallel execution!                                   │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Building a Looping Pipeline

**Step 1: Create Metadata Table**

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

**Step 2: Lookup + ForEach Pipeline**

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    METADATA-DRIVEN LOOPING PIPELINE                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌─────────────┐      ┌─────────────────────────────────────────────────────┐ │
│   │   Lookup    │─────▶│              ForEach                                │ │
│   │   (Get all  │      │  ┌─────────────────────────────────────────────┐   │ │
│   │   tables)   │      │  │  Execute Pipeline: pl_incremental_load      │   │ │
│   └─────────────┘      │  │  Parameters:                                │   │ │
│         │              │  │    schema_name: @{item().SchemaName}        │   │ │
│         ▼              │  │    table_name:  @{item().TableName}         │   │ │
│   Returns array:       │  └─────────────────────────────────────────────┘   │ │
│   [                    └─────────────────────────────────────────────────────┘ │
│     {SchemaName:                                                                │
│      "SalesLT",                                                                 │
│      TableName:                                                                 │
│      "Customer"},                                                               │
│     {...},                                                                      │
│     {...}                                                                       │
│   ]                                                                             │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    NESTED FOREACH PATTERN                                       │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌───────────────────────────────────────────────────────────────────────┐   │
│   │  ForEach SOURCE (Systems: [ERP, CRM, HR])                             │   │
│   │  ┌───────────────────────────────────────────────────────────────┐   │   │
│   │  │  ForEach TABLE (Tables from each source)                      │   │   │
│   │  │  ┌─────────────────────────────────────────────────────────┐ │   │   │
│   │  │  │  Copy Data Activity                                     │ │   │   │
│   │  │  │  Source: @{item().SourceSystem}.@{item().TableName}     │ │   │   │
│   │  │  └─────────────────────────────────────────────────────────┘ │   │   │
│   │  └───────────────────────────────────────────────────────────────┘   │   │
│   └───────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    LOGIC APPS OVERVIEW                                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Common Use Cases with ADF:                                                     │
│  ─────────────────────────────                                                  │
│  ┌────────────┐        ┌─────────────┐        ┌────────────┐                   │
│  │ ADF        │───────▶│ Logic App   │───────▶│ Send Email │                   │
│  │ Pipeline   │ Trigger│             │ Action │ Notification│                  │
│  │ Completes  │        │             │        │            │                   │
│  └────────────┘        └─────────────┘        └────────────┘                   │
│                                                                                 │
│  ┌────────────┐        ┌─────────────┐        ┌────────────┐                   │
│  │ ADF        │───────▶│ Logic App   │───────▶│ Post to    │                   │
│  │ Pipeline   │        │             │        │ Slack/Teams│                   │
│  │ Fails      │        │             │        │            │                   │
│  └────────────┘        └─────────────┘        └────────────┘                   │
│                                                                                 │
│  ┌────────────┐        ┌─────────────┐        ┌────────────┐                   │
│  │ New File   │───────▶│ Logic App   │───────▶│ Trigger    │                   │
│  │ Arrives    │        │             │        │ ADF Pipeline│                  │
│  └────────────┘        └─────────────┘        └────────────┘                   │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Creating a Logic App for Pipeline Notifications

**Architecture:**

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    NOTIFICATION SYSTEM ARCHITECTURE                             │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌───────────────┐                                                             │
│   │  ADF Pipeline │                                                             │
│   │  Completes    │                                                             │
│   └───────┬───────┘                                                             │
│           │                                                                     │
│           ▼                                                                     │
│   ┌───────────────┐      ┌───────────────┐      ┌───────────────┐             │
│   │  Web Activity │─────▶│  Logic App    │─────▶│  Send Email   │             │
│   │  (HTTP POST)  │      │  (Triggered)  │      │  via Outlook  │             │
│   └───────────────┘      └───────────────┘      └───────────────┘             │
│                                │                                                │
│                                ├─────────────────▶ Post to Teams               │
│                                │                                                │
│                                └─────────────────▶ Log to Database             │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Step 1: Create Logic App via Azure CLI**

```bash
# Create Logic App
az logic workflow create \
    --name "la-adf-notifications" \
    --resource-group "data-eng-rg" \
    --location "eastus" \
    --definition @logic-app-definition.json
```

**Step 2: Logic App Definition (Trigger: HTTP Request)**

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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    ERROR NOTIFICATION PIPELINE PATTERN                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────┐ │
│   │                        Main Pipeline                                     │ │
│   │   ┌─────────┐      ┌─────────┐      ┌─────────┐                         │ │
│   │   │ Step 1  │─────▶│ Step 2  │─────▶│ Step 3  │                         │ │
│   │   └─────────┘      └─────────┘      └─────────┘                         │ │
│   │        │                │                │                               │ │
│   │        │ On Failure     │ On Failure     │ On Failure                   │ │
│   │        ▼                ▼                ▼                               │ │
│   │   ┌───────────────────────────────────────────────────────────────────┐ │ │
│   │   │              Error Handler (Web Activity → Logic App)             │ │ │
│   │   └───────────────────────────────────────────────────────────────────┘ │ │
│   │                                                                         │ │
│   └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
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

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    LOGIC APP TRIGGERED PIPELINE                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌─────────────────┐                                                          │
│   │  Recurrence     │  ← Every day at 6 AM                                     │
│   │  Trigger        │                                                          │
│   └────────┬────────┘                                                          │
│            │                                                                    │
│            ▼                                                                    │
│   ┌─────────────────┐                                                          │
│   │  Condition:     │  ← Is it a business day?                                 │
│   │  dayOfWeek()    │    (Monday-Friday, not holiday)                          │
│   │  not in [0,6]   │                                                          │
│   └────────┬────────┘                                                          │
│            │ Yes                                                                │
│            ▼                                                                    │
│   ┌─────────────────┐                                                          │
│   │  HTTP Action    │  ← Call ADF REST API                                     │
│   │  POST: Create   │    to trigger pipeline                                   │
│   │  Pipeline Run   │                                                          │
│   └─────────────────┘                                                          │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
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
