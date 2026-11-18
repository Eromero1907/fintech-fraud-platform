"""
AWS Glue 4.0 PySpark Job — Pipeline Batch Medallion Automatizado
Lee directamente de s3://fintech-dl-182832138582/bronze/ y escribe en Parquet Snappy:
  - silver/transactions/ (particionado por year, month, day, country)
  - silver/quarantine/   (particionado por ingestion_date)
  - gold/fraud_alerts_history/, gold/merchant_risk_ranking/, gold/daily_financial_kpis/
"""

import sys
from datetime import datetime, timezone
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, row_number,
    year, month, dayofmonth, lpad, to_timestamp, to_date,
    count, sum as _sum, avg as _avg, round as _round, explode, coalesce, array
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, ArrayType
)

BUCKET = "fintech-dl-182832138582"
BRONZE_TX_PATH = f"s3://{BUCKET}/bronze/transactions/*.ndjson"
BRONZE_ALERTS_PATH = f"s3://{BUCKET}/bronze/fraud_alerts/*.ndjson"
SILVER_TX_PATH = f"s3://{BUCKET}/silver/transactions/"
QUARANTINE_PATH = f"s3://{BUCKET}/silver/quarantine/"
GOLD_ALERTS_PATH = f"s3://{BUCKET}/gold/fraud_alerts_history/"
GOLD_MERCHANTS_PATH = f"s3://{BUCKET}/gold/merchant_risk_ranking/"
GOLD_KPIS_PATH = f"s3://{BUCKET}/gold/daily_financial_kpis/"

BATCH_ID = f"glue_batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main():
    spark = (
        SparkSession.builder
        .appName(f"FintechMedallionGlueETL_{BATCH_ID}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tx_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ])

    alert_schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("related_transaction_ids", ArrayType(StringType()), True),
        StructField("rule_triggered", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("detection_latency_ms", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True),
    ])

    # 1. Lectura de Bronze (Transacciones y Alertas CEP)
    raw_df = spark.read.schema(tx_schema).json(BRONZE_TX_PATH)
    total_raw = raw_df.count()

    alerts_df = spark.read.schema(alert_schema).json(BRONZE_ALERTS_PATH).dropDuplicates(["alert_id"])

    # Mapa transaction_id -> (alert_id, rule_triggered) desde alertas CEP reales
    exploded_alerts = (
        alerts_df
        .withColumn("tx_ref", explode(coalesce(col("related_transaction_ids"), array(col("transaction_id")))))
        .select(
            col("tx_ref").alias("matched_tx_id"),
            col("alert_id").alias("cep_alert_id"),
            col("rule_triggered").alias("cep_rule_triggered"),
        )
        .dropDuplicates(["matched_tx_id"])
    )

    # 2. Enriquecimiento con Linaje SI3008
    enriched_df = (
        raw_df
        .withColumn("batch_id", lit(BATCH_ID))
        .withColumn("ingestion_date", lit(INGESTION_DATE))
        .withColumn("file_source", lit("s3_bronze_stream"))
        .withColumn("processing_timestamp", current_timestamp().cast(StringType()))
    )

    # 3. Deduplicación por transaction_id
    win = Window.partitionBy("transaction_id").orderBy(col("timestamp").asc())
    ranked_df = enriched_df.withColumn("rn", row_number().over(win))

    dups_df = (
        ranked_df
        .filter((col("transaction_id").isNotNull()) & (col("rn") > 1))
        .drop("rn")
        .withColumn("rejection_reason", lit("DUPLICATE_RECORD: Clave primaria transaction_id duplicada"))
    )

    unique_df = ranked_df.filter((col("transaction_id").isNull()) | (col("rn") == 1)).drop("rn")

    # 4. Reglas de Calidad de Datos
    cond_null = (
        col("transaction_id").isNull() | col("card_id").isNull() | col("customer_id").isNull() |
        col("amount").isNull() | col("amount_usd").isNull() | col("country").isNull() | col("timestamp").isNull()
    )
    cond_amount = (col("amount") <= 0) | (col("amount_usd") <= 0)
    cond_coords = (col("latitude") < -90.0) | (col("latitude") > 90.0) | (col("longitude") < -180.0) | (col("longitude") > 180.0)

    evaluated_df = unique_df.withColumn(
        "rejection_reason",
        when(cond_null, lit("NULL_VALUE: Campo obligatorio nulo o vacío"))
        .when(cond_amount, lit("INVALID_AMOUNT: Monto debe ser mayor a 0"))
        .when(cond_coords, lit("INVALID_COORDINATES: Coordenadas GPS fuera de rango"))
        .otherwise(lit(None))
    )

    quarantine_df = evaluated_df.filter(col("rejection_reason").isNotNull()).unionByName(dups_df)
    clean_df = evaluated_df.filter(col("rejection_reason").isNull()).drop("rejection_reason")

    # 5. Cruce Silver con Alertas CEP reales + Particiones Hive
    silver_df = (
        clean_df
        .join(exploded_alerts, clean_df.transaction_id == exploded_alerts.matched_tx_id, "left")
        .withColumn("is_fraud_flagged", col("cep_alert_id").isNotNull())
        .withColumn("fraud_rule_triggered", coalesce(col("cep_rule_triggered"), lit("NONE")))
        .withColumn("alert_id", col("cep_alert_id"))
        .drop("matched_tx_id", "cep_alert_id", "cep_rule_triggered")
        .withColumn("_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("year", year(col("_ts")).cast(StringType()))
        .withColumn("month", lpad(month(col("_ts")).cast(StringType()), 2, "0"))
        .withColumn("day", lpad(dayofmonth(col("_ts")).cast(StringType()), 2, "0"))
        .drop("_ts")
    )

    total_silver = silver_df.count()
    total_quarantine = quarantine_df.count()

    # Escritura Parquet Snappy en Silver y Quarantine
    (
        silver_df.write.mode("overwrite")
        .partitionBy("year", "month", "day", "country")
        .parquet(SILVER_TX_PATH)
    )
    (
        quarantine_df.write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(QUARANTINE_PATH)
    )

    # 6. Construcción de Zona Gold
    gold_alerts_df = alerts_df.select(
        "alert_id", "transaction_id", "card_id", "customer_id",
        "amount_usd", "currency", "merchant_name", "rule_triggered",
        "city", "country", "detection_latency_ms", "timestamp", "status"
    )
    gold_alerts_df.write.mode("overwrite").parquet(GOLD_ALERTS_PATH)

    gold_merchants_df = (
        silver_df.groupBy("merchant_name", "merchant_category")
        .agg(
            count("*").alias("total_transactions"),
            _sum(when(col("is_fraud_flagged"), 1).otherwise(0)).alias("fraud_transactions"),
            _round(_sum("amount_usd"), 2).alias("total_volume_usd"),
            _round(_sum(when(col("is_fraud_flagged"), col("amount_usd")).otherwise(0.0)), 2).alias("fraud_volume_usd"),
        )
        .withColumn("fraud_rate_pct", _round((col("fraud_transactions") / col("total_transactions")) * 100.0, 2))
        .select("merchant_name", "merchant_category", "total_transactions", "fraud_transactions", "fraud_rate_pct", "total_volume_usd", "fraud_volume_usd")
    )
    gold_merchants_df.write.mode("overwrite").parquet(GOLD_MERCHANTS_PATH)

    # KPIs Ejecutivos Diarios (con DATE y TIMESTAMP compatibles con Athena)
    alerts_summary = gold_alerts_df.agg(
        count("*").alias("alerts_cnt"),
        _round(coalesce(_sum("amount_usd"), lit(0.0)), 2).alias("saved_usd"),
        _round(coalesce(_avg("detection_latency_ms"), lit(0.0)), 3).alias("avg_lat"),
    ).collect()[0]

    silver_summary = silver_df.agg(
        _round(coalesce(_sum("amount_usd"), lit(0.0)), 2).alias("vol_usd"),
        _sum(when(col("is_fraud_flagged"), 1).otherwise(0)).alias("flagged_cnt"),
    ).collect()[0]

    balance_pct = 100.0 if total_raw == (total_silver + total_quarantine) else 0.0
    kpi_row = [(
        BATCH_ID,
        INGESTION_DATE,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        int(total_raw),
        int(total_silver),
        int(total_quarantine),
        float(balance_pct),
        float(silver_summary["vol_usd"]),
        int(total_silver - silver_summary["flagged_cnt"]),
        int(silver_summary["flagged_cnt"]),
        int(alerts_summary["alerts_cnt"]),
        float(alerts_summary["saved_usd"]),
        float(alerts_summary["avg_lat"]),
    )]
    kpi_df = spark.createDataFrame(kpi_row, [
        "batch_id", "ingestion_date_str", "execution_ts_str",
        "total_raw_records", "total_curated_silver_records", "total_quarantine_records",
        "reconciliation_balance_pct", "total_financial_volume_usd",
        "legitimate_transactions", "fraud_transactions_involved",
        "fraud_alerts_emitted", "total_money_saved_usd", "avg_detection_latency_ms"
    ]).withColumn("ingestion_date", to_date(col("ingestion_date_str"), "yyyy-MM-dd")) \
      .withColumn("execution_timestamp", to_timestamp(col("execution_ts_str"), "yyyy-MM-dd HH:mm:ss")) \
      .drop("ingestion_date_str", "execution_ts_str") \
      .select(
          "batch_id", "ingestion_date", "execution_timestamp",
          "total_raw_records", "total_curated_silver_records", "total_quarantine_records",
          "reconciliation_balance_pct", "total_financial_volume_usd",
          "legitimate_transactions", "fraud_transactions_involved",
          "fraud_alerts_emitted", "total_money_saved_usd", "avg_detection_latency_ms"
      )

    kpi_df.write.mode("overwrite").parquet(GOLD_KPIS_PATH)

    print(f"[GLUE RECONCILIATION] Raw={total_raw} | Silver={total_silver} | Quarantine={total_quarantine} | Balance={balance_pct}%")
    spark.stop()


if __name__ == "__main__":
    main()
