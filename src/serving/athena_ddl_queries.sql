-- ==============================================================================
-- Amazon Athena / AWS Glue Data Catalog DDL & Consultas Analíticas de Sustentación
-- Proyecto: Plataforma Fintech de Detección de Fraude y AML en Tiempo Real
-- Estándar: Formato Columnar Apache Parquet con Compresión Snappy y Particiones Hive
-- Bucket S3: s3://fintech-dl-182832138582/
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. CREACIÓN DE BASE DE DATOS EN GLUE DATA CATALOG / ATHENA
-- ------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS fintech_fraud_lakehouse
COMMENT 'Data Lakehouse Medallion de Transacciones Financieras y Detección de Fraude CEP';

USE fintech_fraud_lakehouse;

-- ------------------------------------------------------------------------------
-- 2. TABLA ZONA SILVER: Transacciones Curadas, Deduplicadas y Enriquecidas
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS silver_transactions (
    transaction_id STRING,
    card_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    amount_usd DOUBLE,
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    channel STRING,
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timestamp STRING,
    batch_id STRING,
    ingestion_date STRING,
    file_source STRING,
    processing_timestamp STRING,
    is_fraud_flagged BOOLEAN,
    fraud_rule_triggered STRING,
    alert_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, country STRING)
STORED AS PARQUET
LOCATION 's3://fintech-dl-182832138582/silver/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'classification'='parquet');

-- Descubrimiento automático de particiones Hive en S3
MSCK REPAIR TABLE silver_transactions;

-- ------------------------------------------------------------------------------
-- 3. TABLA ZONA CUARENTENA: Registros Rechazados por Reglas de Calidad / Duplicados
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_transactions (
    transaction_id STRING,
    card_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    amount_usd DOUBLE,
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    channel STRING,
    country STRING,
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timestamp STRING,
    batch_id STRING,
    file_source STRING,
    processing_timestamp STRING,
    rejection_reason STRING
)
PARTITIONED BY (ingestion_date STRING)
STORED AS PARQUET
LOCATION 's3://fintech-dl-182832138582/silver/quarantine/'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'classification'='parquet');

MSCK REPAIR TABLE quarantine_transactions;

-- ------------------------------------------------------------------------------
-- 4. TABLA ZONA GOLD: Historial de Incidentes de Fraude Bloqueados (CEP)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold_fraud_alerts (
    alert_id STRING,
    transaction_id STRING,
    card_id STRING,
    customer_id STRING,
    amount_usd DOUBLE,
    currency STRING,
    merchant_name STRING,
    rule_triggered STRING,
    city STRING,
    country STRING,
    detection_latency_ms DOUBLE,
    timestamp STRING,
    status STRING
)
STORED AS PARQUET
LOCATION 's3://fintech-dl-182832138582/gold/fraud_alerts_history/'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'classification'='parquet');

-- ------------------------------------------------------------------------------
-- 5. TABLA ZONA GOLD: Ranking de Riesgo por Comercio
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold_merchant_risk (
    merchant_name STRING,
    merchant_category STRING,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate_pct DOUBLE,
    total_volume_usd DOUBLE,
    fraud_volume_usd DOUBLE
)
STORED AS PARQUET
LOCATION 's3://fintech-dl-182832138582/gold/merchant_risk_ranking/'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'classification'='parquet');

-- ------------------------------------------------------------------------------
-- 6. TABLA ZONA GOLD: KPIs Ejecutivos y Reconciliación Diaria
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold_daily_kpis (
    batch_id STRING,
    ingestion_date DATE,
    execution_timestamp TIMESTAMP,
    total_raw_records BIGINT,
    total_curated_silver_records BIGINT,
    total_quarantine_records BIGINT,
    reconciliation_balance_pct DOUBLE,
    total_financial_volume_usd DOUBLE,
    legitimate_transactions BIGINT,
    fraud_transactions_involved BIGINT,
    fraud_alerts_emitted BIGINT,
    total_money_saved_usd DOUBLE,
    avg_detection_latency_ms DOUBLE
)
STORED AS PARQUET
LOCATION 's3://fintech-dl-182832138582/gold/daily_financial_kpis/'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'classification'='parquet');


-- ==============================================================================
-- CONSULTAS DE SUSTENTACIÓN ANTE EL PROFESOR (DEMO EN VIVO)
-- ==============================================================================

-- CONSULTA 1: Dinero total en USD salvado en tiempo real agrupado por regla CEP
SELECT 
    rule_triggered AS Regla_Disparada,
    status AS Accion_Tomada,
    COUNT(*) AS Incidentes_CEP,
    ROUND(SUM(amount_usd), 2) AS Dinero_Salvado_USD,
    ROUND(AVG(detection_latency_ms), 3) AS Latencia_Promedio_ms
FROM gold_fraud_alerts
GROUP BY rule_triggered, status
ORDER BY Dinero_Salvado_USD DESC;

-- CONSULTA 2: Auditoría de Comercios de Alto Riesgo (Top Fraude)
SELECT 
    merchant_name AS Comercio,
    merchant_category AS Categoria,
    total_transactions AS Transacciones_Totales,
    fraud_transactions AS Transacciones_Fraudulentas,
    fraud_rate_pct AS Tasa_Fraude_Pct,
    fraud_volume_usd AS Dinero_Bloqueado_USD
FROM gold_merchant_risk
WHERE fraud_transactions > 0
ORDER BY fraud_volume_usd DESC;

-- CONSULTA 3: Auditoría de Calidad de Datos y Zona de Cuarentena
SELECT 
    SPLIT_PART(rejection_reason, ':', 1) AS Codigo_Rechazo,
    rejection_reason AS Detalle_Regla_Calidad,
    COUNT(*) AS Registros_Segregados
FROM quarantine_transactions
GROUP BY rejection_reason
ORDER BY Registros_Segregados DESC;

-- CONSULTA 4: Tablero de Reconciliación Medallion y KPIs Globales
SELECT 
    batch_id,
    total_raw_records AS Total_Raw,
    total_curated_silver_records AS Total_Silver,
    total_quarantine_records AS Total_Cuarentena,
    reconciliation_balance_pct AS Balance_Reconciliacion_Pct,
    fraud_alerts_emitted AS Alertas_CEP,
    total_money_saved_usd AS Total_Protegido_USD
FROM gold_daily_kpis;
