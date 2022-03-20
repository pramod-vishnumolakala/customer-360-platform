"""
Delta Lake Utilities — helper functions for medallion architecture
operations on ADLS Gen2. Handles compaction, vacuuming, schema evolution.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Customer360-Utils")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def compact_delta_table(path: str, target_file_size_mb: int = 128):
    """
    Compact small files in a Delta table (OPTIMIZE).
    Improves query performance by reducing file count.
    """
    spark = get_spark()
    logger.info(f"Compacting Delta table at {path}")
    spark.sql(f"""
        OPTIMIZE delta.`{path}`
        ZORDER BY (customer_id)
    """)
    logger.info(f"Compaction complete: {path}")


def vacuum_delta_table(path: str, retention_hours: int = 168):
    """
    Remove old Delta files beyond retention window (default 7 days).
    Reduces ADLS Gen2 storage costs.
    """
    spark = get_spark()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    logger.info(f"Vacuuming Delta table at {path} (retention: {retention_hours}h)")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"Vacuum complete: {path}")


def get_table_history(path: str, limit: int = 10) -> DataFrame:
    """Return Delta table change history — useful for auditing."""
    spark = get_spark()
    return (
        DeltaTable.forPath(spark, path)
        .history(limit)
        .select("version", "timestamp", "operation", "operationParameters", "userMetadata")
    )


def restore_table_to_version(path: str, version: int):
    """Restore a Delta table to a previous version — disaster recovery."""
    spark = get_spark()
    logger.warning(f"Restoring {path} to version {version}")
    spark.sql(f"RESTORE TABLE delta.`{path}` TO VERSION AS OF {version}")
    logger.info(f"Restore complete: {path} → version {version}")


def get_table_stats(path: str) -> dict:
    """Return basic stats for a Delta table."""
    spark  = get_spark()
    table  = DeltaTable.forPath(spark, path)
    detail = table.detail().collect()[0]
    return {
        "num_files":        detail["numFiles"],
        "size_bytes":       detail["sizeInBytes"],
        "size_mb":          round(detail["sizeInBytes"] / 1024 / 1024, 2),
        "last_modified":    str(detail["lastModified"]),
        "partition_columns": detail["partitionColumns"],
    }
