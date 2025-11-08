#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, reconciliation, and UC replication.
"""

import os
import sys

# Determine the parent directory of the current script for module imports
EXECUTED_IN_WORKSPACE = False
PWD = ""
try:
    PWD = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore  # noqa: E501
    parent_folder = os.path.dirname(os.path.dirname(PWD))
    EXECUTED_IN_WORKSPACE = True
except NameError:
    # Fallback when running outside Databricks (e.g. local development or tests)
    parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if not parent_folder.startswith("/Workspace"):
    parent_folder = "/Workspace" + parent_folder
# Append the framework path to the system path for module resolution
if parent_folder not in sys.path:
    sys.path.append(parent_folder)

import argparse
import uuid
from pathlib import Path

from databricks.sdk import WorkspaceClient

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.loader import ConfigLoader
from data_replication.config.models import TableType, VolumeType, UCObjectType
from data_replication.exceptions import ConfigurationError
from data_replication.providers.provider_factory import ProviderFactory
from data_replication.utils import (
    create_spark_session,
    validate_spark_session,
    get_workspace_url_from_host,
)


def create_logger(config) -> DataReplicationLogger:
    """Create logger instance from configuration."""
    logger = DataReplicationLogger("data_replication")
    if hasattr(config, "logging") and config.logging:
        logger.setup_logging(config.logging)
    return logger


def parse_comma_delimited_enums(value: str, enum_class, arg_name: str):
    """
    Parse comma-delimited string into list of enum values.

    Args:
        value: Comma-delimited string
        enum_class: Enum class to validate against
        arg_name: Argument name for error messages

    Returns:
        List of enum values

    Raises:
        ValueError: If any value is not valid for the enum
    """
    if not value:
        return None

    items = [item.strip() for item in value.split(",")]
    items = [item for item in items if item]  # Filter empty strings

    result = []
    for item in items:
        try:
            # Try to create enum value
            result.append(enum_class(item))
        except ValueError:
            valid_values = [e.value for e in enum_class]
            raise ValueError(
                f"Invalid value '{item}' for {arg_name}. "
                f"Valid values are: {', '.join(valid_values)}"
            )

    return result


def validate_args(args) -> None:
    """
    Validate command line arguments according to business rules.

    Args:
        args: Parsed command line arguments

    Raises:
        ValueError: If validation rules are violated
    """
    # Rule: target-catalog should only contain 1 catalog
    if args.target_catalog:
        catalog_names = [name.strip() for name in args.target_catalog.split(",")]
        catalog_names = [name for name in catalog_names if name]  # Filter empty strings
        if len(catalog_names) > 1:
            raise ValueError(
                f"target-catalog should only contain 1 catalog, but got {len(catalog_names)}: "
                f"{', '.join(catalog_names)}"
            )

    # Rule: when target-schema is provided, target-catalog must be provided
    if args.target_schemas and not args.target_catalog:
        raise ValueError(
            "When target-schemas is provided, target-catalog must also be provided"
        )

    # Rule: when target-tables is provided, target-schemas and target-catalog must be provided
    if args.target_tables:
        if not args.target_schemas:
            raise ValueError(
                "When target-tables is provided, target-schemas must also be provided"
            )
        if not args.target_catalog:
            raise ValueError(
                "When target-tables is provided, target-catalog must also be provided"
            )

    # Rule: if target-tables is provided, target-schemas should only contain 1 schema
    if args.target_tables and args.target_schemas:
        schema_names = [name.strip() for name in args.target_schemas.split(",")]
        schema_names = [name for name in schema_names if name]  # Filter empty strings
        if len(schema_names) > 1:
            raise ValueError(
                f"When target-tables is provided, target-schemas should only contain 1 schema, "
                f"but got {len(schema_names)}: {', '.join(schema_names)}"
            )


def run_backup(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only backup operations."""
    if config.audit_config.logging_workspace == "source":
        spark = logging_spark
    else:
        # Create source Spark session
        source_host = config.source_databricks_connect_config.host
        source_secret_config = config.source_databricks_connect_config.token
        source_cluster_id = config.source_databricks_connect_config.cluster_id
        # create and validate Spark sessions
        spark = create_spark_session(
            source_host, source_secret_config, source_cluster_id, workspace_client
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(source_host)):
            logger.error(
                "Spark session is not connected to the configured source workspace"
            )
            raise ConfigurationError(
                "Spark session is not connected to the configured source workspace"
            )

    backup_factory = ProviderFactory(
        "backup", config, spark, logging_spark, workspace_client, logger, run_id
    )
    summary = backup_factory.run_backup_operations()

    if summary.failed_operations > 0:
        logger.error(f"Backup completed with {summary.failed_operations} failures")
        return 1

    logger.info("All backup operations completed successfully")
    return 0


def run_replication(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only replication operations."""
    if config.audit_config.logging_workspace == "target":
        spark = logging_spark
    else:
        # Create target Spark session
        target_host = config.target_databricks_connect_config.host
        target_secret_config = config.target_databricks_connect_config.token
        target_cluster_id = config.target_databricks_connect_config.cluster_id
        spark = create_spark_session(
            target_host, target_secret_config, target_cluster_id, workspace_client
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
            logger.error(
                "Spark session is not connected to the configured target workspace"
            )
            raise ConfigurationError(
                "Spark session is not connected to the configured target workspace"
            )

    replication_factory = ProviderFactory(
        "replication", config, spark, logging_spark, workspace_client, logger, run_id
    )
    summary = replication_factory.run_replication_operations()

    if summary.failed_operations > 0:
        logger.error(f"Replication completed with {summary.failed_operations} failures")
        return 1

    logger.info("All replication operations completed successfully")
    return 0


def run_reconciliation(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only reconciliation operations."""
    if config.audit_config.logging_workspace == "target":
        spark = logging_spark
    else:
        # Create target Spark session
        target_host = config.target_databricks_connect_config.host
        target_secret_config = config.target_databricks_connect_config.token
        target_cluster_id = config.target_databricks_connect_config.cluster_id
        spark = create_spark_session(
            target_host, target_secret_config, target_cluster_id, workspace_client
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
            logger.error(
                "Spark session is not connected to the configured target workspace"
            )
            raise ConfigurationError(
                "Spark session is not connected to the configured target workspace"
            )

    reconciliation_factory = ProviderFactory(
        "reconciliation", config, spark, logging_spark, workspace_client, logger, run_id
    )
    summary = reconciliation_factory.run_reconciliation_operations()

    if summary.failed_operations > 0:
        logger.error(
            f"Reconciliation completed with {summary.failed_operations} failures"
        )
        return 1

    logger.info("All reconciliation operations completed successfully")
    return 0


def setup_argument_parser():
    """Setup and configure the command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Data Replication Tool for Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("config_file", help="Path to the YAML configuration file")

    parser.add_argument(
        "--operation",
        "-o",
        choices=["all", "backup", "replication", "reconciliation"],
        default="all",
        help="Specific operation to run (default: all enabled operations)",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration without running operations",
    )

    parser.add_argument("--run-id", type=str, help="Unique Run ID")

    parser.add_argument(
        "--target-catalog",
        type=str,
        help="target catalog name, e.g. catalog1",
    )

    parser.add_argument(
        "--target-schemas",
        type=str,
        help="list of schemas separated by comma, e.g. schema1,schema2",
    )

    parser.add_argument(
        "--target-tables",
        type=str,
        help="list of tables separated by comma, e.g. table1,table2",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="maximum number of concurrent tasks, default is 4",
    )

    parser.add_argument(
        "--uc-object-types",
        type=str,
        help="comma-separated list of UC metadata types to replicate. Acceptable values: all,catalog,catalog_tag,schema,schema_tag,view_tag,table_tag,column_tag,volume,volume_tag",
    )

    parser.add_argument(
        "--table-types",
        type=str,
        help="comma-separated list of table types to process. Acceptable values: managed,external,streaming_table",
    )

    parser.add_argument(
        "--volume-types",
        type=str,
        help="comma-separated list of volume types to process. Acceptable values: all,managed,external",
    )

    parser.add_argument(
        "--logging-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Acceptable values: DEBUG,INFO,WARNING,ERROR,CRITICAL",
    )

    return parser


def main():
    """Main entry point for data replication system."""
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Validate command line arguments
    try:
        validate_args(args)
    except ValueError as e:
        print(f"Argument validation error: {e}")
        return 1

    # Validate config file exists
    config_path = Path(args.config_file)
    if not config_path.exists():
        print(
            f"Error: Configuration file not found: {config_path}",
            file=sys.stderr,
        )
        return 1

    try:
        # Parse the enum override arguments
        uc_object_types_override = parse_comma_delimited_enums(
            args.uc_object_types, UCObjectType, "--uc-object-types"
        )
        table_types_override = parse_comma_delimited_enums(
            args.table_types, TableType, "--table-types"
        )
        volume_types_override = parse_comma_delimited_enums(
            args.volume_types, VolumeType, "--volume-types"
        )

        # Load and validate configuration
        config = ConfigLoader.load_from_file(
            config_path=config_path,
            target_catalog_override=args.target_catalog,
            target_schemas_override=args.target_schemas,
            target_tables_override=args.target_tables,
            concurrency_override=args.concurrency,
            uc_object_types_override=uc_object_types_override,
            table_types_override=table_types_override,
            volume_types_override=volume_types_override,
            logging_level_override=args.logging_level,
        )
        logger = create_logger(config)

        logger.info(f"Loaded configuration from {config_path}")

        if args.validate_only:
            logger.info("Configuration validation completed successfully")
            return 0

        run_id = str(uuid.uuid4())
        if args.run_id:
            run_id = args.run_id

        w = WorkspaceClient()
        default_workspace_url = w.config.host
        default_user = w.current_user.me().user_name
        if not EXECUTED_IN_WORKSPACE:
            logger.info("Running from external non-Databricks environment")
        else:
            logger.info("Running from Databricks environment")

        logger.info(f"Connecting to default workspace {default_workspace_url} as user {default_user}")

        if config.audit_config.logging_workspace == "source":
            logging_host = config.source_databricks_connect_config.host
            logging_secret_config = config.source_databricks_connect_config.token
            logging_cluster_id = config.source_databricks_connect_config.cluster_id
        else:
            logging_host = config.target_databricks_connect_config.host
            logging_secret_config = config.target_databricks_connect_config.token
            logging_cluster_id = config.target_databricks_connect_config.cluster_id

        logging_spark = create_spark_session(
            logging_host, logging_secret_config, logging_cluster_id, w
        )
        logging_workspace_url = get_workspace_url_from_host(logging_host)

        if not validate_spark_session(logging_spark, logging_workspace_url):
            logger.error(
                "Logging Spark session is not connected to the configured logging workspace"
            )
            raise ConfigurationError(
                f"""Logging Spark session is not connected to the configured logging workspace. "
                Expected: {logging_workspace_url}"""
            )

        logger.debug(f"Config: {config}")

        logger.info(
            f"Log run_id {run_id} in {config.audit_config.audit_table} at {logging_workspace_url}"
        )
        logger.info(f"All Operations Begins {'-' * 60}")

        if args.operation in ["all", "backup"]:
            # Check if backup is configured
            backup_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.backup_config and cat.backup_config.enabled
            ]

            if backup_catalogs:
                logger.info(f"Backup Begins {'-' * 60}")
                logger.info(
                    f"Running backup operations for {len(backup_catalogs)} catalogs"
                )

                run_backup(config, logger, logging_spark, run_id, w)
                logger.info(f"Backup Ends {'-' * 60}")
            elif args.operation == "backup":
                logger.info("Backup disabled or No catalogs configured for backup")

        if args.operation in ["all", "replication"]:
            # Check if replication is configured
            replication_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.replication_config and cat.replication_config.enabled
            ]

            if replication_catalogs:
                logger.info(f"Replication Begins {'-' * 60}")
                logger.info(
                    f"Running replication operations for {len(replication_catalogs)} catalogs"
                )

                run_replication(config, logger, logging_spark, run_id, w)
                logger.info(f"Replication Ends {'-' * 60}")
            elif args.operation == "replication":
                logger.info(
                    "Replication disabled or No catalogs configured for replication"
                )

        if args.operation in ["all", "reconciliation"]:
            # Check if reconciliation is configured
            reconciliation_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.reconciliation_config and cat.reconciliation_config.enabled
            ]

            if reconciliation_catalogs:
                logger.info(f"Reconciliation Begins {'-' * 60}")
                logger.info(
                    f"Running reconciliation operations for {len(reconciliation_catalogs)} catalogs"
                )

                run_reconciliation(config, logger, logging_spark, run_id, w)
                logger.info(f"Reconciliation Ends {'-' * 60}")
            elif args.operation == "reconciliation":
                logger.info(
                    "Reconciliation disabled or No catalogs configured for reconciliation"
                )

        logger.info(f"All Operations Ends {'-' * 60}")

    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Operation failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
