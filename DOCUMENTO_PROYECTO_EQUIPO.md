# Documento de Alineación del Proyecto: Plataforma Fintech de Detección de Fraude y AML en Tiempo Real

**Asignatura:** SI3008 – Sistemas Intensivos en Datos (Universidad EAFIT)  
**Estado Actual:** Base de código y pruebas locales **100% completadas** | Pendiente despliegue en **AWS** y sustentación.

---

## 1. ¿De qué trata el proyecto?

Vamos a construir y sustentar una **Plataforma Distribuida de Detección de Fraude Transaccional y Prevención de Lavado de Activos (AML) en Tiempo Real** sobre un **Data Lakehouse Medallion en AWS**.

### ¿Por qué este tema?
* **Sin Machine Learning:** No perdemos tiempo entrenando ni calibrando modelos de IA. Toda la detección se basa en **Procesamiento de Eventos Complejos con Estado (*Stateful CEP*)**, es decir, reglas determinísticas de auditoría bancaria en tiempo real que son 100% explicables ante el profesor.
* **100% Sistemas Intensivos en Datos:** Demuestra flujo masivo en **Apache Kafka** (miles de transacciones/segundo), procesamiento con memoria en ventanas de tiempo en **Spark Structured Streaming**, almacenamiento columnar optimizado en **AWS S3 (Apache Parquet Snappy)**, control de calidad (**Zona de Cuarentena + Reconciliación 100%**) y analítica serverless en **Amazon Athena**.

### Las 3 Reglas de Fraude en Tiempo Real (*Stateful CEP*)
1. **Viaje Imposible (*Impossible Travel*):** El motor recuerda la última ubicación y hora de cada tarjeta (`card_id`). Si una tarjeta compra en Bogotá a las 10:00 AM y en Madrid a las 10:05 AM, calcula la distancia por la fórmula geodésica de **Haversine**; como la velocidad supera los **900 km/h** (avión comercial), bloquea la transacción al instante.
2. **Ataque de Bots (*Card Testing*):** Si una tarjeta registra **4 o más micro-transacciones (≤ $5 USD)** en menos de **30 segundos**, detecta un ataque automatizado probando tarjetas robadas y lo bloquea.
3. **Pitufeo / Evasión Fiscal (*AML Structuring*):** Si un cliente realiza **3 o más transferencias** en menos de 1 hora por montos entre **$9.000.000 y $9.999.999 COP** (justo debajo del tope legal de reporte de $10M COP), levanta una alerta de auditoría AML.

---

## 2. Arquitectura Objetivo en AWS

El despliegue final en nuestra cuenta de AWS Academy debe cumplir con aislamiento estricto de red (**VPC propia**):

* **Red (VPC Propia `10.0.0.0/16`):**
  * **Subred Pública (`10.0.1.0/24`):** Internet Gateway, NAT Gateway y un Bastion Host (`t3.micro`) como único punto de entrada SSH.
  * **Subred Privada (`10.0.2.0/24`):** Instancias aisladas sin IP pública (`ec2-fintech-kafka` y `ec2-fintech-spark` con `LabInstanceProfile`) + **VPC Gateway Endpoint para S3** (el tráfico hacia S3 viaja por la red interna de AWS sin salir a internet).
* **Ingesta y Streaming:** Clúster **Apache Kafka (3 brokers en modo KRaft)** en EC2 + **Apache Spark Structured Streaming** evaluando micro-batches y publicando alertas al tópico `fraud-alerts`.
* **Data Lakehouse Medallion en S3:**
  * `raw/`: Transacciones crudas y alertas CEP emitidas en streaming.
  * `quarantine/`: Registros corruptos o duplicados segregados con columna `rejection_reason`.
  * `silver/`: Transacciones curadas, deduplicadas, con metadatos de linaje y particionadas estilo Hive (`year=YYYY/month=MM/day=DD/country=XX/`) en formato **Parquet Snappy**.
  * `gold/`: Tablas analíticas para el negocio (`gold_fraud_alerts`, `gold_merchant_risk`, `gold_daily_kpis`).
* **Serving Analítico:** **AWS Glue Data Catalog** + **Amazon Athena** para consultar con SQL cuántos dólares salvó el sistema en tiempo real.

---

## 3. ¿Qué YA está hecho y verificado?

Todo el núcleo del proyecto ya está programado y probado en el repositorio:

| Componente | Archivo | Estado Verificado |
| :--- | :--- | :--- |
| **Esquema y Reglas** | `config/transaction_schema.json`<br>`config/fraud_rules_config.yaml`<br>`src/config_loader.py` | Definidos los campos bancarios, umbrales de las 3 reglas y cargador centralizado de configuración. |
| **Productor / Simulador** | `src/ingestion/financial_producer.py` | Genera tráfico realista a **~1.000 transacciones/segundo** manteniendo ciudades fijas por tarjeta, e inyecta los 3 escenarios de fraude + 4 anomalías de calidad. |
| **Motor Streaming CEP** | `src/streaming/haversine_utils.py`<br>`src/streaming/streaming_fraud_detector.py` | Implementa las 3 reglas con estado en memoria, cálculo Haversine, tolerancia a registros corruptos y publicación de alertas en **< 1 ms** por evento. |
| **Pipeline Batch Medallion** | `src/batch/batch_medallion_etl.py` | Procesa `Raw -> Silver/Quarantine -> Gold`, escribe **Parquet comprimido con Snappy** con particiones Hive, cruza las alertas reales de streaming y genera el **Reporte de Reconciliación Exacta (100% Balance)**. |
| **Consultas Athena (SQL)** | `src/serving/athena_ddl_queries.sql`<br>`src/serving/run_athena_local_queries.py` | DDLs listos para Glue/Athena (`STORED AS PARQUET`) y 5 consultas analíticas probadas localmente con DuckDB. |
| **Pruebas Automatizadas** | `tests/test_fraud_rules.py` | **8/8 pruebas unitarias y de integración aprobadas** cubriendo las 3 reglas CEP, geometría Haversine e inmunidad a datos sucios. |

---

## 4. ¿Qué FALTA por hacer? (Plan de Trabajo en Equipo)

El código local funciona de punta a punta; ahora nos falta llevarlo a la nube de AWS y preparar la sustentación:

1. **Montar la Infraestructura en AWS (VPC + S3 + EC2):**
   * Crear la VPC propia (`10.0.0.0/16`) con subred pública, subred privada, NAT Gateway y **S3 Gateway Endpoint**.
   * Crear el bucket S3 con la estructura `fintech_project/{raw,quarantine,silver,gold,athena-results}/`.
   * Configurar los 3 Security Groups (`sg-fintech-bastion`, `sg-fintech-spark`, `sg-fintech-kafka`) y lanzar las 3 instancias EC2 (`bastion` `t3.micro`, `kafka` `t3.large`, `spark` `t3.large` con `LabInstanceProfile`).
2. **Conectar el Código directamente a S3 real (`boto3`) y Kafka en EC2:**
   * Adaptar `streaming_fraud_detector.py` y `batch_medallion_etl.py` para que lean/escriban directamente en `s3://<nuevo-bucket>/fintech_project/` mediante `boto3` en lugar de carpetas locales.
   * Actualizar el nombre del nuevo bucket en `config/fraud_rules_config.yaml` y `src/serving/athena_ddl_queries.sql`.
3. **Levantar Kafka en EC2 y Ejecutar Prueba en Vivo (`--mode spark`):**
   * Instalar Docker en `ec2-fintech-kafka`, levantar los 3 brokers en modo KRaft y crear los tópicos `financial-transactions` y `fraud-alerts` (3 particiones, factor de replicación 3).
   * Correr el productor, Spark Streaming en vivo y validar las tablas reales desde la consola de **Amazon Athena**.
4. **Material de Sustentación:**
   * Generar el diagrama de arquitectura formal en **draw.io (`.drawio`)** reflejando la VPC, subredes, EC2, Kafka, Spark, S3 Medallion y Athena.
   * Documentar evidencias (capturas de Athena, logs de reconciliación y prueba de resiliencia apagando 1 broker) para la presentación ante el profesor.

---

## 5. Instrucciones: ¿Cómo debe quedar subido en la Nube (AWS)?

Para que el proyecto cumpla al 100% con la exigencia del profesor (sin usar la *Default VPC* de AWS), el despliegue en nuestra cuenta de **AWS Academy** debe quedar montado exactamente con esta topología:

### A. Red y Seguridad (VPC Propia)
1. **Crear VPC (`vpc-fintech-fraud`, CIDR `10.0.0.0/16`)** con el asistente *VPC and more* (1 Zona de Disponibilidad `us-east-1a`):
   * **Subred Pública:** Con **Internet Gateway** y **NAT Gateway** (permite que las máquinas privadas descarguen Docker/paquetes sin tener IP pública).
   * **Subred Privada:** Aislada de internet (sin IP pública).
   * **VPC Gateway Endpoint para S3:** Asociado a la tabla de rutas privada para que el tráfico entre Spark/EC2 y S3 viaje 100% por la red interna de AWS.
2. **Crear 3 Security Groups dentro de esa VPC:**
   * `sg-fintech-bastion`: Permite `SSH (22)` únicamente desde nuestra IP (`My IP`).
   * `sg-fintech-spark`: Permite `SSH (22)` únicamente desde `sg-fintech-bastion`.
   * `sg-fintech-kafka`: Permite `SSH (22)` desde `sg-fintech-bastion` y puertos **`19092 - 39092`** desde `sg-fintech-spark`.

### B. Almacenamiento Data Lakehouse (Amazon S3)
* Crear un bucket en `us-east-1` (ej. `fintech-fraud-lakehouse-<account-id>`) con la carpeta `fintech_project/` y las 5 subcarpetas:
  * `raw/`, `quarantine/`, `silver/`, `gold/` y `athena-results/`.

### C. Cómputo (3 Instancias EC2 dentro de nuestra VPC)
| Instancia | Tipo | Subred | IP Pública | IAM Role | Qué corre adentro |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`ec2-fintech-bastion`** | `t3.micro` | Pública | **Sí** | — | Puente SSH seguro (*ProxyJump*) para entrar a las máquinas privadas. |
| **`ec2-fintech-kafka`** | `t3.large` | Privada | **No** | `LabInstanceProfile` | Clúster Docker **Kafka KRaft de 3 brokers** (puertos `19092, 29092, 39092`) con tópicos `financial-transactions` y `fraud-alerts` (3 particiones, RF=3). |
| **`ec2-fintech-spark`** | `t3.large` | Privada | **No** | `LabInstanceProfile` | Java 17 + PySpark + código del repo (`financial_producer.py`, `streaming_fraud_detector.py --mode spark` y `batch_medallion_etl.py`). |

### D. Consultas Finales en la Consola de AWS
1. Abrir **Amazon Athena**, configurar la ubicación de resultados en `s3://<tu-bucket>/fintech_project/athena-results/`.
2. Ejecutar el script `src/serving/athena_ddl_queries.sql` para registrar las tablas externas en **AWS Glue Data Catalog** (`silver_transactions`, `quarantine_transactions`, `gold_fraud_alerts`, `gold_merchant_risk`, `gold_daily_kpis`).
3. Ejecutar `MSCK REPAIR TABLE` sobre `silver_transactions` y `quarantine_transactions`, y correr las 4 consultas analíticas de sustentación en vivo.

