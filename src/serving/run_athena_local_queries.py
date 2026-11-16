"""
Ejecutor Local de Consultas SQL Serverless (Emulador de Amazon Athena sobre Parquet Snappy).
Consulta directamente los archivos `.snappy.parquet` generados en las zonas Silver, Quarantine y Gold
usando el motor SQL columnar DuckDB.
"""

import os
import duckdb


def run_athena_queries(lakehouse_dir: str = "./data/s3_lakehouse"):
    con = duckdb.connect()

    silver_glob = os.path.join(lakehouse_dir, "silver", "transactions_curated", "**", "*.parquet")
    quarantine_glob = os.path.join(lakehouse_dir, "quarantine", "transactions", "**", "*.parquet")
    gold_alerts_glob = os.path.join(lakehouse_dir, "gold", "fraud_alerts_history", "*.parquet")
    gold_merch_glob = os.path.join(lakehouse_dir, "gold", "merchant_risk_ranking", "*.parquet")
    gold_kpis_glob = os.path.join(lakehouse_dir, "gold", "daily_financial_kpis", "*.parquet")

    print("\n" + "=" * 85)
    print("  CONSULTA 1 (ATHENA SQL): DINERO TOTAL SALVADO EN VIVO POR REGLA STATEFUL CEP")
    print("=" * 85)
    q1 = f"""
    SELECT 
        rule_triggered AS Regla_Disparada,
        status AS Accion_Tomada,
        COUNT(*) AS Incidentes_CEP,
        ROUND(SUM(amount_usd), 2) AS Dinero_Salvado_USD,
        ROUND(AVG(detection_latency_ms), 3) AS Latencia_ms
    FROM read_parquet('{gold_alerts_glob}')
    GROUP BY rule_triggered, status
    ORDER BY Dinero_Salvado_USD DESC;
    """
    con.sql(q1).show()

    print("=" * 85)
    print("  CONSULTA 2 (ATHENA SQL): RANKING DE COMERCIOS DE ALTO RIESGO (ZONA GOLD)")
    print("=" * 85)
    q2 = f"""
    SELECT 
        merchant_name AS Comercio,
        merchant_category AS Categoria,
        total_transactions AS Total_Tx,
        fraud_transactions AS Tx_Fraudulentas,
        fraud_rate_pct AS Tasa_Fraude_Pct,
        fraud_volume_usd AS Dinero_Involucrado_USD
    FROM read_parquet('{gold_merch_glob}')
    WHERE fraud_transactions > 0
    ORDER BY fraud_volume_usd DESC;
    """
    con.sql(q2).show()

    print("=" * 85)
    print("  CONSULTA 3 (ATHENA SQL): AUDITORÍA DE ZONA DE CUARENTENA Y REGLAS DE CALIDAD")
    print("=" * 85)
    q3 = f"""
    SELECT 
        split_part(rejection_reason, ':', 1) AS Codigo_Rechazo,
        rejection_reason AS Detalle_Regla_Calidad,
        COUNT(*) AS Registros_Segregados
    FROM read_parquet('{quarantine_glob}', hive_partitioning=true)
    GROUP BY rejection_reason
    ORDER BY Registros_Segregados DESC;
    """
    con.sql(q3).show()

    print("=" * 85)
    print("  CONSULTA 4 (ATHENA SQL): PARTICIONES HIVE EN ZONA SILVER (PARQUET SNAPPY)")
    print("=" * 85)
    q4 = f"""
    SELECT 
        year, month, day, country AS Pais,
        COUNT(*) AS Transacciones_Curadas,
        SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS Tx_Marcadas_CEP,
        ROUND(SUM(amount_usd), 2) AS Volumen_Total_USD
    FROM read_parquet('{silver_glob}', hive_partitioning=true)
    GROUP BY year, month, day, country
    ORDER BY Transacciones_Curadas DESC;
    """
    con.sql(q4).show()

    print("=" * 85)
    print("  CONSULTA 5 (ATHENA SQL): RESUMEN EJECUTIVO Y BALANCE DE RECONCILIACIÓN (100%)")
    print("=" * 85)
    q5 = f"""
    SELECT 
        batch_id,
        total_raw_records AS Raw_In,
        total_curated_silver_records AS Silver_Ok,
        total_quarantine_records AS Quarantine_Out,
        reconciliation_balance_pct AS Balance_Pct,
        fraud_alerts_emitted AS Alertas_CEP,
        total_money_saved_usd AS Protegido_USD
    FROM read_parquet('{gold_kpis_glob}');
    """
    con.sql(q5).show()
    con.close()


if __name__ == "__main__":
    run_athena_queries()
