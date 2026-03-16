"""
Customer 360 — Bronze → Silver transformation (Azure Databricks / PySpark).
Standardises raw policy, claims and telematics data into clean, conformed
Silver layer tables stored as Delta Lake on ADLS Gen2.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, DateType, BooleanType,
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("Customer360-Bronze-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

ADLS_BASE  = "abfss://customer360@pramodstorage.dfs.core.windows.net"
BRONZE     = f"{ADLS_BASE}/bronze"
SILVER     = f"{ADLS_BASE}/silver"


# ── Policy data cleaning ─────────────────────────────────────────────
def transform_policies(bronze_path: str, silver_path: str):
    """
    Standardise raw policy records — deduplicate, normalise types,
    derive coverage duration, classify policy tier.
    """
    raw = spark.read.format("delta").load(bronze_path)
    logger.info(f"Policy bronze records: {raw.count():,}")

    clean = (
        raw
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["policy_id", "effective_date"])

        # Normalise dates
        .withColumn("effective_date",  F.to_date("effective_date",  "yyyy-MM-dd"))
        .withColumn("expiration_date", F.to_date("expiration_date", "yyyy-MM-dd"))
        .withColumn("inception_date",  F.to_date("inception_date",  "yyyy-MM-dd"))

        # Normalise strings
        .withColumn("policy_status",  F.upper(F.trim("policy_status")))
        .withColumn("policy_type",    F.upper(F.trim("policy_type")))
        .withColumn("product_line",   F.upper(F.trim("product_line")))
        .withColumn("state_code",     F.upper(F.trim("state_code")))

        # Derived fields
        .withColumn("coverage_days",       F.datediff("expiration_date", "effective_date"))
        .withColumn("days_since_inception", F.datediff(F.current_date(), "inception_date"))
        .withColumn("is_active",           F.col("policy_status").isin(["ACTIVE", "IN_FORCE"]).cast(BooleanType()))
        .withColumn("premium_monthly",     F.round(F.col("annual_premium") / 12, 2))

        # Policy tier classification based on annual premium
        .withColumn("premium_tier", F.when(F.col("annual_premium") >= 5000, "PLATINUM")
                                     .when(F.col("annual_premium") >= 2000, "GOLD")
                                     .when(F.col("annual_premium") >= 500,  "SILVER")
                                     .otherwise("BRONZE"))

        # Renewal flag
        .withColumn("is_renewal", F.col("policy_number").contains("-R"))

        # Metadata
        .withColumn("silver_loaded_at",  F.current_timestamp())
        .withColumn("source_system",     F.lit("POLICY_MGMT"))
    )

    dq_failed = clean.filter(
        (F.col("annual_premium") <= 0) |
        (F.col("coverage_days") <= 0)  |
        (F.col("expiration_date") <= F.col("effective_date"))
    )
    logger.warning(f"Policy DQ failures: {dq_failed.count():,}")

    clean_valid = clean.filter(
        (F.col("annual_premium") > 0) &
        (F.col("coverage_days") > 0)  &
        (F.col("expiration_date") > F.col("effective_date"))
    )

    # Upsert into Delta Silver
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(clean_valid.alias("source"), "target.policy_id = source.policy_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        clean_valid.write.format("delta").mode("overwrite").partitionBy("product_line", "policy_status").save(silver_path)

    logger.info(f"Policy silver records written: {clean_valid.count():,}")
    return clean_valid


# ── Claims data cleaning ─────────────────────────────────────────────
def transform_claims(bronze_path: str, silver_path: str):
    """
    Standardise raw claims — normalise statuses, classify severity,
    compute cycle time metrics for actuarial reporting.
    """
    raw = spark.read.format("delta").load(bronze_path)
    logger.info(f"Claims bronze records: {raw.count():,}")

    clean = (
        raw
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("policy_id").isNotNull())
        .dropDuplicates(["claim_id"])

        .withColumn("loss_date",    F.to_date("loss_date",    "yyyy-MM-dd"))
        .withColumn("report_date",  F.to_date("report_date",  "yyyy-MM-dd"))
        .withColumn("close_date",   F.to_date("close_date",   "yyyy-MM-dd"))
        .withColumn("claim_status", F.upper(F.trim("claim_status")))
        .withColumn("peril_code",   F.upper(F.trim("peril_code")))

        # Cycle time
        .withColumn("days_to_report", F.datediff("report_date", "loss_date"))
        .withColumn("days_to_close",  F.datediff("close_date",  "report_date"))
        .withColumn("is_open",        F.col("claim_status").isin(["OPEN", "PENDING", "IN_REVIEW"]).cast(BooleanType()))

        # Severity classification
        .withColumn("severity", F.when(F.col("incurred_loss") >= 100_000, "CATASTROPHIC")
                                  .when(F.col("incurred_loss") >= 25_000,  "LARGE")
                                  .when(F.col("incurred_loss") >= 5_000,   "MEDIUM")
                                  .otherwise("SMALL"))

        # SIU flag
        .withColumn("siu_referral_flag",
                    ((F.col("incurred_loss") > 50_000) |
                     (F.col("days_to_report") > 30)).cast(BooleanType()))

        .withColumn("silver_loaded_at", F.current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        DeltaTable.forPath(spark, silver_path).alias("t") \
            .merge(clean.alias("s"), "t.claim_id = s.claim_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        clean.write.format("delta").mode("overwrite").partitionBy("claim_status", "severity").save(silver_path)

    logger.info(f"Claims silver records written: {clean.count():,}")
    return clean


# ── Customer 360 profile assembly ────────────────────────────────────
def build_customer_360(silver_base: str, gold_path: str):
    """
    Assemble unified Customer 360 profile from silver policy + claims + telematics.
    Output: one row per customer with full 360-degree view.
    """
    policies  = spark.read.format("delta").load(f"{silver_base}/policies")
    claims    = spark.read.format("delta").load(f"{silver_base}/claims")
    telematics = spark.read.format("delta").load(f"{silver_base}/telematics")

    # Policy summary per customer
    policy_summary = policies.groupBy("customer_id").agg(
        F.count("policy_id").alias("total_policies"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_policies"),
        F.sum("annual_premium").alias("total_annual_premium"),
        F.avg("annual_premium").alias("avg_annual_premium"),
        F.max("inception_date").alias("latest_policy_date"),
        F.min("inception_date").alias("first_policy_date"),
        F.sum(F.when(F.col("is_renewal"), 1).otherwise(0)).alias("renewal_count"),
        F.countDistinct("product_line").alias("product_lines_count"),
        F.collect_set("product_line").alias("product_lines"),
        F.max("premium_tier").alias("highest_tier"),
    )

    # Claims summary per customer
    claim_summary = (
        claims
        .join(policies.select("policy_id", "customer_id"), on="policy_id", how="left")
        .groupBy("customer_id").agg(
            F.count("claim_id").alias("total_claims"),
            F.sum("incurred_loss").alias("total_incurred_loss"),
            F.avg("incurred_loss").alias("avg_claim_amount"),
            F.sum(F.when(F.col("is_open"), 1).otherwise(0)).alias("open_claims"),
            F.max("loss_date").alias("latest_claim_date"),
            F.sum(F.when(F.col("severity") == "CATASTROPHIC", 1).otherwise(0)).alias("catastrophic_claims"),
            F.avg("days_to_close").alias("avg_claim_cycle_days"),
        )
    )

    # Telematics summary per customer
    telem_summary = telematics.groupBy("customer_id").agg(
        F.avg("risk_score").alias("avg_telematics_risk"),
        F.avg("miles_driven_monthly").alias("avg_miles_monthly"),
        F.avg("harsh_braking_events").alias("avg_harsh_braking"),
        F.avg("speeding_pct").alias("avg_speeding_pct"),
        F.max("last_trip_date").alias("last_trip_date"),
    )

    # Assemble 360 profile
    customer_360 = (
        policy_summary
        .join(claim_summary,  on="customer_id", how="left")
        .join(telem_summary,  on="customer_id", how="left")

        # Derived 360 KPIs
        .withColumn("claim_frequency",   F.col("total_claims") / F.greatest(F.col("total_policies"), F.lit(1)))
        .withColumn("loss_ratio",        F.col("total_incurred_loss") / F.greatest(F.col("total_annual_premium"), F.lit(0.01)))
        .withColumn("customer_tenure_years",
                    F.round(F.datediff(F.current_date(), F.col("first_policy_date")) / 365.25, 1))
        .withColumn("renewal_rate",
                    F.col("renewal_count") / F.greatest(F.col("total_policies"), F.lit(1)))

        # Customer value tier
        .withColumn("customer_value_tier",
                    F.when(F.col("total_annual_premium") >= 10_000, "TIER_1_PLATINUM")
                     .when(F.col("total_annual_premium") >= 5_000,  "TIER_2_GOLD")
                     .when(F.col("total_annual_premium") >= 1_000,  "TIER_3_SILVER")
                     .otherwise("TIER_4_STANDARD"))

        # Churn risk score (simple rule-based)
        .withColumn("churn_risk_score", (
            F.when(F.col("open_claims") > 2, 30).otherwise(0) +
            F.when(F.col("loss_ratio") > 0.8, 25).otherwise(0) +
            F.when(F.col("renewal_rate") < 0.5, 20).otherwise(0) +
            F.when(F.col("avg_telematics_risk") > 75, 15).otherwise(0)
        ))
        .withColumn("is_churn_risk", F.col("churn_risk_score") >= 50)

        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    customer_360.write.format("delta").mode("overwrite").save(gold_path)
    logger.info(f"Customer 360 gold profiles written: {customer_360.count():,}")
    return customer_360


if __name__ == "__main__":
    transform_policies(f"{BRONZE}/policies", f"{SILVER}/policies")
    transform_claims(f"{BRONZE}/claims",     f"{SILVER}/claims")
    build_customer_360(SILVER,               f"{ADLS_BASE}/gold/customer_360")
