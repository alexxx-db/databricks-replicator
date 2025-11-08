## Minimum Permission Required for User/Service Principal at Source Workspace

### Delta Sharing Infra Setup (Source):
```
- CREATE_SHARE, USE_SHARE - backup_config.create_share
- CREATE_RECIPIENT - backup_config.create_recipient
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### Data Replication (Source):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
- USE_SHARE, MODIFY_SHARE - backup_config.add_to_share
- CREATE_CATALOG - backup_config.create_backup_catalog - required for streaming table only 
- USE_CATALOG, CREATE_SCHEMA on backup catalog if not owner - required for streaming table only 
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on backup catalog if not owner - required for streaming table only 
- USE_CATALOG, USE_SCHEMA, SELECT on __databricks__internal catalog - required for streaming table only 
```

### UC Replication (Source):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
- USE_SHARE, MODIFY_SHARE - backup_config.add_to_share
```

## Minimum Permission Required for User/Service Principal at Target Workspace

### Delta Sharing Infra Setup (Target):
```
- CREATE_CATALOG, USE_PROVIDER - replication_config.create_shared_catalog (for replication)
- CREATE_CATALOG, USE_PROVIDER - reconciliation_config.create_shared_catalog (for reconciliation)
```

### Data Replication (Target):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, WRITE_VOLUME on target catalog
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY on __databricks__internal catalog - required for streaming table only
- CREATE_CATALOG - replication_config.create_intermediate_catalog - required only if intermediate catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on intermediate catalog if not owner - required only if intermediate schema needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on intermediate catalog if not owner - required only if intermediate table is used
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### UC Replication (Target):
```
- CREATE_CATALOG
- USE_CATALOG, CREATE_SCHEMA on target catalog
- USE_CATALOG, USE_SCHEMA, CREATE_VOLUME, MODIFY, CREATE_TABLE, APPLY_TAG on target schema
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### Reconciliation (Target):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on target catalog
- CREATE_CATALOG - reconciliation_config.create_recon_catalog - required only if recon result catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on recon result catalog if not owner - required only if recon result schema needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on recon result catalog if not owner
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```
This granular breakdown provides the specific permissions required for each Delta Sharing operation, avoiding the need for broad administrative roles.