# Plataforma Fintech de Detección de Fraude y AML en Tiempo Real

> **Asignatura: SI3008 - Sistemas Intensivos en Datos (Universidad EAFIT)**  
> **Arquitectura Híbrida de Streaming Stateful CEP + Data Lakehouse Medallion sobre AWS.**  
> Implementa **Procesamiento de Eventos Complejos con Estado (Stateful CEP)** para bloqueo de fraude financiero y lavado de activos, junto con un pipeline **Batch Medallion (Raw $\rightarrow$ Silver / Quarantine $\rightarrow$ Gold)** en formato **Apache Parquet Snappy** y consultas serverless en **Amazon Athena**.

---

## 1. Arquitectura de Extremo a Extremo

```text
[Simulador POS / Terminales Web]
               │ (1.000 - 3.000 transacciones/segundo)
               ▼
┌─────────────────────────────┐         (Volcado Continuo)        ┌──────────────────────────────────────────────┐
│        APACHE KAFKA         │ ─────────────────────────────────►│              AMAZON S3 LAKEHOUSE             │
│ (financial-transactions)    │                                   │   s3://book-store-datalake-.../fintech/      │
└──────────────┬──────────────┘                                   │   ├── raw/ (Transacciones y Alertas NDJSON)  │
               │ (Micro-batches 3s / Watermark 10s)               │   ├── quarantine/ (Rechazos con linaje)      │
               ▼                                                  │   ├── silver/ (Parquet Snappy particionado)  │
┌─────────────────────────────┐                                   │   └── gold/ (KPIs, Riesgo y Alertas Parquet) │
│   SPARK STREAMING (CEP)     │                                   └──────────────────────┬───────────────────────┘
│ • Regla 1: Viaje Imposible  │                                                          │
│ • Regla 2: Ráfaga de Bots   │                                                          ▼
│ • Regla 3: Pitufeo AML      │                                           ┌──────────────────────────────┐
└──────────────┬──────────────┘                                           │    AWS GLUE DATA CATALOG     │
               │                                                          └──────────────┬───────────────┘
               ▼ (Acción inmediata de bloqueo en < 1 ms)                                 │
┌─────────────────────────────┐                                                          ▼
│     Tópico: fraud-alerts    │                                           ┌──────────────────────────────┐
│ (Integración a pasarelas)   │                                           │        AMAZON ATHENA         │
└─────────────────────────────┘                                           │  (Consultas SQL Serverless)  │
                                                                          └──────────────────────────────┘
```

---

## 2. Estructura del Proyecto

```text
fintech-fraud-platform/
├── README.md
├── docker-compose.yml              # Clúster Kafka KRaft (3 particiones) + Kafka-UI
├── requirements.txt                # pyspark, duckdb, pyarrow, kafka-python, boto3, pytest
├── .env.example                    # Variables de entorno AWS Academy y Kafka
├── config/
│   ├── transaction_schema.json     # Esquema JSON formal bancario
│   └── fraud_rules_config.yaml     # Parámetros dinámicos de reglas CEP, Kafka y S3
├── src/
│   ├── config_loader.py            # Cargador centralizado de YAML y .env
│   ├── ingestion/
│   │   └── financial_producer.py   # Productor con escenarios de fraude y anomalías de calidad
│   ├── streaming/
│   │   ├── haversine_utils.py      # Cálculo de distancias geodésicas y velocidad
│   │   └── streaming_fraud_detector.py # Motor Spark Structured Streaming con Stateful CEP
│   ├── batch/
│   │   └── batch_medallion_etl.py  # Pipeline Medallion (Raw -> Silver/Quarantine -> Gold Parquet)
│   └── serving/
│       ├── athena_ddl_queries.sql  # DDL Parquet Snappy con particiones Hive para Athena
│       └── run_athena_local_queries.py # Emulador local de consultas SQL de Athena
├── data/
│   ├── sample_transactions/        # Datos de prueba generados
│   └── s3_lakehouse/               # Lakehouse local (raw, quarantine, silver, gold en Parquet)
└── tests/
    └── test_fraud_rules.py         # Suite de 8 pruebas unitarias y de integración CEP
```

---

## 3. Guía de Ejecución Rápida (End-to-End)

### Paso 1: Ejecutar Suite de Pruebas Unitarias y Stateful CEP
```bash
PYTHONPATH=. python3 tests/test_fraud_rules.py
```

### Paso 2: Generar Flujo de Transacciones Financieras
```bash
PYTHONPATH=. python3 src/ingestion/financial_producer.py --dry-run --eps 1000 --duration 7 --fraud-interval 2 --save-file data/sample_transactions/test_sample_all_rules.ndjson
```

### Paso 3: Ejecutar el Motor de Detección de Fraude en Streaming
```bash
PYTHONPATH=. python3 src/streaming/streaming_fraud_detector.py --mode test_file --input-file data/sample_transactions/test_sample_all_rules.ndjson
```

### Paso 4: Ejecutar el Pipeline Batch Medallion (Parquet Snappy + Reconciliación 100%)
```bash
PYTHONPATH=. python3 src/batch/batch_medallion_etl.py
```

### Paso 5: Ejecutar las Consultas Analíticas SQL (Amazon Athena / DuckDB)
```bash
PYTHONPATH=. python3 src/serving/run_athena_local_queries.py
```
