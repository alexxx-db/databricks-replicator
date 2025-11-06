"""
Unity Catalog metadata replication provider.

This module handles Unity Catalog metadata replication between source and target
workspaces, including catalogs, schemas, table tags, SQL views, and volumes.
"""

from datetime import datetime, timezone
from typing import List, Optional

from databricks.connect import DatabricksSession

from ..audit.logger import DataReplicationLogger
from ..config.models import (
    RetryConfig,
    RunResult,
    TargetCatalogConfig,
    DatabricksConnectConfig,
    UCObjectType,
)
from ..databricks_operations import DatabricksOperations
from ..exceptions import DataReplicationError, ConfigurationError
from ..utils import (
    create_spark_session,
    validate_spark_session,
    get_workspace_url_from_host,
)
from .base_provider import BaseProvider


class UCReplicationProvider(BaseProvider):
    """Provider for Unity Catalog metadata replication operations."""

    def __init__(
        self,
        source_spark: DatabricksSession,
        logger: DataReplicationLogger,
        db_ops: DatabricksOperations,
        run_id: str,
        catalog_config: TargetCatalogConfig,
        source_databricks_config: DatabricksConnectConfig,
        target_databricks_config: DatabricksConnectConfig,
        retry: Optional[RetryConfig] = None,
        max_workers: int = 2,
        timeout_seconds: int = 300,
        external_location_mapping: Optional[dict] = None,
    ):
        """Initialize the UC replication provider."""
        super().__init__(
            source_spark,
            logger,
            db_ops,
            run_id,
            catalog_config,
            source_databricks_config,
            target_databricks_config,
            retry,
            max_workers,
            timeout_seconds,
            external_location_mapping,
        )

        self.uc_config = catalog_config.uc_replication_config

        # Create and validate target Spark session
        self.target_spark = self._create_target_spark_session(target_databricks_config)

    def _create_target_spark_session(
        self, target_config: DatabricksConnectConfig
    ) -> DatabricksSession:
        """Create and validate a Spark session connected to the target workspace."""
        try:
            token_value = None
            if target_config.token:
                # Handle token from secret scope
                token_value = self.db_ops.get_secret(
                    target_config.token.secret_scope, target_config.token.secret_key
                )

            target_spark = create_spark_session(
                host=target_config.host,
                token=token_value,
                cluster_id=target_config.cluster_id,
            )

            # Validate target spark session
            workspace_url = get_workspace_url_from_host(target_config.host)
            if not validate_spark_session(target_spark, workspace_url):
                self.logger.error(
                    "Target Spark session validation failed: workspace URL mismatch"
                )
                raise ConfigurationError(
                    f"Target Spark session validation failed: workspace URL mismatch"
                )

            return target_spark

        except Exception as e:
            raise ConfigurationError(f"Failed to create target Spark session: {str(e)}")

    def setup_operation_catalogs(self) -> str:
        """
        Setup operation-specific catalogs for UC replication.

        Returns:
            The catalog name to use for operations
        """
        # For UC replication, we use the target catalog name as the primary catalog
        return self.uc_config.source_catalog

    def get_operation_name(self) -> str:
        """
        Get the name of the operation for logging purposes.

        Returns:
            String name of the operation
        """
        return "uc_replication"

    def is_operation_enabled(self) -> bool:
        """
        Check if the UC replication operation is enabled in the configuration.

        Returns:
            True if operation is enabled, False otherwise
        """
        return bool(self.uc_config and self.uc_config.enabled)

    def _get_source_catalog_metadata(self, catalog_name: str) -> dict:
        """Query source catalog metadata using source Spark."""
        try:
            catalog_info = {}

            # Get catalog information
            catalogs_df = self.spark.sql(f"DESCRIBE CATALOG EXTENDED `{catalog_name}`")
            catalog_rows = catalogs_df.collect()

            for row in catalog_rows:
                if row["info_name"] == "Comment":
                    catalog_info["comment"] = row["info_value"]
                elif row["info_name"] == "Properties":
                    catalog_info["properties"] = row["info_value"]

            return catalog_info
        except Exception as e:
            self.logger.error(
                f"Failed to get catalog metadata for {catalog_name}: {str(e)}"
            )
            return {}

    def _get_source_schema_metadata(self, catalog_name: str, schema_name: str) -> dict:
        """Query source schema metadata using source Spark."""
        try:
            schema_info = {}

            # Get schema information
            schemas_df = self.spark.sql(
                f"DESCRIBE SCHEMA EXTENDED `{catalog_name}`.`{schema_name}`"
            )
            schema_rows = schemas_df.collect()

            for row in schema_rows:
                if row["info_name"] == "Comment":
                    schema_info["comment"] = row["info_value"]
                elif row["info_name"] == "Properties":
                    schema_info["properties"] = row["info_value"]

            return schema_info
        except Exception as e:
            self.logger.error(
                f"Failed to get schema metadata for {catalog_name}.{schema_name}: {str(e)}"
            )
            return {}

    def _get_source_table_tags(self, catalog_name: str) -> List[dict]:
        """Query source table tags using source Spark."""
        try:
            # Query table tags from information schema
            tags_df = self.spark.sql(f"""
                SELECT table_catalog, table_schema, table_name, tag_name, tag_value
                FROM `{catalog_name}`.information_schema.table_tags
                WHERE table_catalog = '{catalog_name}'
            """)

            return [row.asDict() for row in tags_df.collect()]
        except Exception as e:
            self.logger.warning(
                f"Failed to get table tags for {catalog_name}: {str(e)}"
            )
            return []

    def _get_source_sql_views(self, catalog_name: str) -> List[dict]:
        """Query source SQL views using source Spark."""
        try:
            # Get all schemas in the catalog
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`")
            schemas = [row["schemaName"] for row in schemas_df.collect()]

            all_views = []
            for schema in schemas:
                try:
                    views_df = self.spark.sql(f"""
                        SELECT table_name, view_definition 
                        FROM `{catalog_name}`.`{schema}`.information_schema.views
                        WHERE table_schema = '{schema}'
                    """)

                    for row in views_df.collect():
                        all_views.append(
                            {
                                "catalog": catalog_name,
                                "schema": schema,
                                "view_name": row["table_name"],
                                "view_definition": row["view_definition"],
                            }
                        )
                except Exception as schema_e:
                    self.logger.warning(
                        f"Failed to get views from {catalog_name}.{schema}: {str(schema_e)}"
                    )
                    continue

            return all_views
        except Exception as e:
            self.logger.warning(f"Failed to get SQL views for {catalog_name}: {str(e)}")
            return []

    def _get_source_volumes(self, catalog_name: str) -> List[dict]:
        """Query source volumes using source Spark."""
        try:
            # Query volumes from information schema
            volumes_df = self.spark.sql(f"""
                SELECT volume_catalog, volume_schema, volume_name, volume_type, storage_location
                FROM `{catalog_name}`.information_schema.volumes
                WHERE volume_catalog = '{catalog_name}'
            """)

            return [row.asDict() for row in volumes_df.collect()]
        except Exception as e:
            self.logger.warning(f"Failed to get volumes for {catalog_name}: {str(e)}")
            return []

    def _replicate_catalog_metadata(
        self, catalog_name: str, catalog_metadata: dict
    ) -> None:
        """Replicate catalog metadata to target using target Spark."""
        try:
            if catalog_metadata.get("comment"):
                self.target_spark.sql(
                    f"COMMENT ON CATALOG `{catalog_name}` IS '{catalog_metadata['comment']}'"
                )

            if catalog_metadata.get("properties"):
                # Parse properties and set them
                # This would need proper property parsing logic
                self.logger.info(
                    f"Catalog properties available for {catalog_name}, manual configuration may be needed"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to replicate catalog metadata for {catalog_name}: {str(e)}"
            )

    def _replicate_schema_metadata(
        self, catalog_name: str, schema_name: str, schema_metadata: dict
    ) -> None:
        """Replicate schema metadata to target using target Spark."""
        try:
            if schema_metadata.get("comment"):
                self.target_spark.sql(
                    f"COMMENT ON SCHEMA `{catalog_name}`.`{schema_name}` IS '{schema_metadata['comment']}'"
                )

            if schema_metadata.get("properties"):
                # Parse properties and set them
                # This would need proper property parsing logic
                self.logger.info(
                    f"Schema properties available for {catalog_name}.{schema_name}, manual configuration may be needed"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to replicate schema metadata for {catalog_name}.{schema_name}: {str(e)}"
            )

    def _replicate_table_tags(self, table_tags: List[dict]) -> None:
        """Replicate table tags to target using target Spark."""
        for tag in table_tags:
            try:
                table_fqn = f"`{tag['table_catalog']}`.`{tag['table_schema']}`.`{tag['table_name']}`"
                self.target_spark.sql(
                    f"ALTER TABLE {table_fqn} SET TAGS ('{tag['tag_name']}' = '{tag['tag_value']}')"
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to set tag {tag['tag_name']} on table {table_fqn}: {str(e)}"
                )

    def _replicate_sql_views(self, sql_views: List[dict]) -> None:
        """Replicate SQL views to target using target Spark."""
        for view in sql_views:
            try:
                view_fqn = (
                    f"`{view['catalog']}`.`{view['schema']}`.`{view['view_name']}`"
                )
                create_view_sql = (
                    f"CREATE OR REPLACE VIEW {view_fqn} AS {view['view_definition']}"
                )
                self.target_spark.sql(create_view_sql)
            except Exception as e:
                self.logger.error(f"Failed to create view {view_fqn}: {str(e)}")

    def _replicate_volumes(self, volumes: List[dict]) -> None:
        """Replicate volumes to target using target Spark."""
        for volume in volumes:
            try:
                volume_fqn = f"`{volume['volume_catalog']}`.`{volume['volume_schema']}`.`{volume['volume_name']}`"

                if volume["volume_type"] == "EXTERNAL":
                    create_volume_sql = f"CREATE VOLUME IF NOT EXISTS {volume_fqn} LOCATION '{volume['storage_location']}'"
                else:
                    create_volume_sql = f"CREATE VOLUME IF NOT EXISTS {volume_fqn}"

                self.target_spark.sql(create_volume_sql)
            except Exception as e:
                self.logger.error(f"Failed to create volume {volume_fqn}: {str(e)}")

    def process_catalog(self) -> List[RunResult]:
        """Process UC metadata replication for the catalog."""
        results = []
        catalog_name = self.catalog_config.catalog_name
        source_catalog = self.uc_config.source_catalog or catalog_name

        self.logger.info(f"Starting UC metadata replication for catalog {catalog_name}")

        uc_object_types = self.uc_config.uc_object_types or []

        for uc_object_type in uc_object_types:
            start_time = datetime.now(timezone.utc)

            try:
                if uc_object_type == UCObjectType.CATALOG:
                    catalog_metadata = self._get_source_catalog_metadata(source_catalog)
                    self._replicate_catalog_metadata(catalog_name, catalog_metadata)

                elif uc_object_type == UCObjectType.SCHEMA:
                    # Get all schemas in the catalog and replicate their metadata
                    schemas_df = self.spark.sql(f"SHOW SCHEMAS IN `{source_catalog}`")
                    schemas = [row["schemaName"] for row in schemas_df.collect()]

                    for schema in schemas:
                        schema_metadata = self._get_source_schema_metadata(
                            source_catalog, schema
                        )
                        self._replicate_schema_metadata(
                            catalog_name, schema, schema_metadata
                        )

                elif uc_object_type == UCObjectType.TABLE_TAGS:
                    table_tags = self._get_source_table_tags(source_catalog)
                    self._replicate_table_tags(table_tags)

                elif uc_object_type == UCObjectType.SQL_VIEWS:
                    sql_views = self._get_source_sql_views(source_catalog)
                    self._replicate_sql_views(sql_views)

                elif uc_object_type == UCObjectType.VOLUMES:
                    volumes = self._get_source_volumes(source_catalog)
                    self._replicate_volumes(volumes)

                elif uc_object_type in [
                    UCObjectType.CATALOG_TAGS,
                    UCObjectType.SCHEMA_TAGS,
                ]:
                    # These would require additional implementation for tag replication
                    self.logger.warning(
                        f"UC object type {uc_object_type} is not yet implemented"
                    )

                end_time = datetime.now(timezone.utc)
                results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=catalog_name,
                        schema_name="ALL",
                        object_name=uc_object_type.value,
                        object_type="uc_metadata",
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                    )
                )

            except Exception as e:
                end_time = datetime.now(timezone.utc)
                error_msg = (
                    f"UC replication failed for {uc_object_type.value}: {str(e)}"
                )
                self.logger.error(error_msg)

                results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=catalog_name,
                        schema_name="ALL",
                        object_name=uc_object_type.value,
                        object_type="uc_metadata",
                        status="failed",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        error_message=error_msg,
                    )
                )

        self.logger.info(
            f"Completed UC metadata replication for catalog {catalog_name}"
        )
        return results
