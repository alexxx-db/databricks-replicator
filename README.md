# Data Replication System for Databricks

A Python plug-in solution to replicate data between Databricks envs. Support and accelerate workloads in multi-cloud migration, single-cloud migration, workspace migration, DR, backup and recovery, multi-cloud data mesh.

Cloud agnostic - cross metastore or same metastore replication

## Overview

This system provides incremental data and UC metadata replication capabilities between Databricks env or within same env with D2D Delta Share and deep clone, with specialized handling for Streaming Tables. It supports multiple operation types that can be run independently or together:

- **Backup**: Export Streaming Table backing tables and add delta tables to Share
- **Replication**: Cross-metastore/same metastore incremental data and uc replication
- **Reconciliation**: Data validation with row counts, schema checks, and missing data detection

## Supported Object Types
- Streaming Tables (data only, no checkpoints)
- Managed Table
- External Table
- UC metadata
  - Table & Column Tags
  
## WIP
- Volume Files
- UC metadata
  - Catalog & Tags
  - Schema & Tags
  - Views
  - Volume & Tags

## Unsupported Object Types
- Materialized Views

## Key Features

### Delta Sharing
Option to let the tool setup Delta share automatically for you, i.e. Recipient, Shares and Shared Catalogs. Or BYO Delta share infra

### Incremental Data Replication
The system leverages Deep Clone for incrementality

### Streaming Table Handling
The system automatically handles Streaming Tables complexities:
- Export ST backing tables
- Constructs internal table path using pipeline ID
- Deep clone ST backing tables rather than ST tables directly

### UC Metadata Replication
Export and import UC metadata including tags

### Run Anywhere
- The tool can be executed in source, target workspace, or via external compute
- The tool can be executed in cli, or deployed via DAB as workflow job


### Robust Logging & Error Handling
- Configurable retry logic with exponential backoff
- Graceful degradation where operations continue if individual tables fail
- Comprehensive error logging with run id and full stack traces
- All operations tracked in audit tables for monitoring and alerting
- Print out all executed SQL in DEBUG mode for easy troubleshooting

### Flexible Configuration
- YAML-based configuration with Pydantic validation
- CLI args to override YAML configuration
- Catalog, schema and table flexible selective replication
- Configurable concurrency and timeout settings

## Installation

### Prerequisites
- User or Service Principal in source and target workspace created with metastore admin right. If metastore admin permission is not available, check <a href=./permissions.md>here</a> to apply more granular UC access control 
- PAT or OAuth Token for user or sp created and stored in Databricks Key Vault. 
**Note**: if this tool is run in source workspace, only target workspace token secrets need to be created in source. Conversely, if run in target workspace, source token needs to be created in target.
- For cross-metastore replication, enable Delta Sharing (DS) including network connectivity https://docs.databricks.com/aws/en/delta-sharing/set-up#gsc.tab=0
- Network connectivity from the tool to source or target workspace. e.g. if tool runs in source workspace, source data plane (outbound) should be able to establish connect to target workspace control plane (inbound). And vica versa.

### Getting Started

1. Install Databricks CLI

2. Setup dev env:
```bash
git clone <repository-url>
cd <repository folder>
make setup
source .venv/bin/activate
```
3. Create your first configuration 
- Clone and modify sample configs in configs folder. Configs with _default suffix allows you to set up replication with minimum configuration.
- For more comprehensive understanding of available configs, check <a href=./configs/README.yaml>README.yaml</a>

4. Run - the system provides a CLI tool `data-replicator` with the following commands:

```bash
# Check all available args
data-replicator --help

# Validate configuration without running
data-replicator <config.yaml> --validate-only

# Run all enabled operations against targeted catalog
data-replicator <config.yaml>  --target-catalog catalog1

# Run all enabled operations against targeted schemas
data-replicator <config.yaml>  --target-catalog catalog1 --target-schemas bronze_1,bronze_2

# Run all enabled operations against targeted tables
data-replicator <config.yaml>  --target-catalog catalog1 --target-schemas bronze_1 --target-tables table1,table2

# Run with different concurrency
data-replicator <config.yaml>  --target-catalog catalog1 --concurrency 10

# Run specific operation only
data-replicator <config.yaml> --operation backup --target-catalog catalog1
data-replicator <config.yaml> --operation replication --target-catalog catalog1
data-replicator <config.yaml> --operation reconciliation --target-catalog catalog1
```

### Operation Types
#### Backup Operations
- For ST, deep clones ST backing tables from source to backup catalogs.
- For object types, add schemas to share.

#### Replication Operations  
- Deep clone tables/volume files across workspaces from share with schema enforcement
- Replicate UC metadata

#### Reconciliation Operations (Table only)
- Row count validation
- Schema structure comparison  
- Missing data detection
  
## Development
### Code Quality Tools
```bash
make quality
```

### Testing
```bash
make test
```

## License

This project is proprietary to Databricks.