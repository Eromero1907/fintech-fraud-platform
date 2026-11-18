"""
Motor de Detección de Fraude en Tiempo Real (Spark Structured Streaming - Stateful CEP)

Consume transacciones financieras de Apache Kafka, aplica marcas de agua (Watermarking)
y evalúa las 3 reglas determinísticas de eventos complejos leídas de `config/fraud_rules_config.yaml`:
1. Regla 1 (Viaje Imposible): Desplazamiento geográfico > 900 km/h en < 30 min.
2. Regla 2 (Ataque de Bots): 4+ transacciones de <= $5 USD en <= 30 seg.
3. Regla 3 (Pitufeo AML): 3+ transferencias entre $9.0M y $9.99M COP en <= 1 hora.

Sinks Duales:
- Tópico Kafka `fraud-alerts` + S3 `raw/fraud_alerts/` (Alertas de bloqueo inmediato)
- Data Lake S3 `raw/transactions/` (Volcado crudo inmutable para la capa Batch Medallion)
"""

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.config_loader import load_fraud_rules_config
from src.streaming.haversine_utils import calculate_travel_velocity_kmh

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("FraudDetector")

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType, TimestampType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


def get_transaction_spark_schema():
    """Retorna el esquema explícito StructType para Spark Structured Streaming."""
    return StructType([
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
        StructField("is_fraud_scenario", StringType(), True),
    ])


class PureStreamFraudEvaluator:
    """
    Evaluador de reglas CEP en streaming con estado en memoria O(1) por tarjeta/cliente.
    Parametrizado dinámicamente desde `config/fraud_rules_config.yaml`.
    """

    def __init__(
        self,
        max_speed_kmh: float = 900.0,
        min_distance_km: float = 50.0,
        bot_window_sec: float = 30.0,
        bot_min_count: int = 4,
        bot_max_usd: float = 5.0,
        aml_window_hours: float = 1.0,
        aml_min_cop: float = 9000000.0,
        aml_max_cop: float = 9999999.0,
        aml_min_count: int = 3,
    ):
        self.max_speed_kmh = float(max_speed_kmh)
        self.min_distance_km = float(min_distance_km)
        self.bot_window_sec = float(bot_window_sec)
        self.bot_min_count = int(bot_min_count)
        self.bot_max_usd = float(bot_max_usd)
        self.aml_window_hours = float(aml_window_hours)
        self.aml_min_cop = float(aml_min_cop)
        self.aml_max_cop = float(aml_max_cop)
        self.aml_min_count = int(aml_min_count)

        # Estado en memoria (State Store por clave de entidad)
        self.card_geo_state: Dict[str, Dict[str, Any]] = {}
        self.card_recent_txs: Dict[str, List[Tuple[datetime, float, str]]] = {}
        self.customer_aml_txs: Dict[str, List[Tuple[datetime, float, float, str]]] = {}
        self.seen_tx_ids: set = set()

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "PureStreamFraudEvaluator":
        rules_cfg = cfg.get("rules", {})
        it_cfg = rules_cfg.get("impossible_travel", {})
        bot_cfg = rules_cfg.get("bot_burst", {})
        aml_cfg = rules_cfg.get("aml_structuring", {})

        return cls(
            max_speed_kmh=it_cfg.get("max_physical_speed_kmh", 900.0),
            min_distance_km=it_cfg.get("min_distance_threshold_km", 50.0),
            bot_window_sec=bot_cfg.get("window_duration_seconds", 30.0),
            bot_min_count=bot_cfg.get("min_transaction_count", 4),
            bot_max_usd=bot_cfg.get("max_amount_usd", 5.0),
            aml_window_hours=aml_cfg.get("window_duration_hours", 1.0),
            aml_min_cop=aml_cfg.get("min_suspicious_amount_cop", 9000000.0),
            aml_max_cop=aml_cfg.get("max_suspicious_amount_cop", 9999999.0),
            aml_min_count=aml_cfg.get("min_transaction_count", 3),
        )

    def _is_valid_for_cep(self, tx: Dict[str, Any]) -> bool:
        """
        Verifica integridad mínima antes de actualizar el State Store en vivo.
        Los registros corruptos o duplicados se omiten en el estado en memoria
        (pero aterrizan en Raw para ser segregados a Cuarentena por la capa Batch).
        """
        tx_id = tx.get("transaction_id")
        if not tx_id or tx_id in self.seen_tx_ids:
            return False
        if not tx.get("card_id") or not tx.get("customer_id") or not tx.get("timestamp"):
            return False
        try:
            amount = float(tx.get("amount", 0))
            amount_usd = float(tx.get("amount_usd", 0))
            lat = float(tx.get("latitude", 0))
            lon = float(tx.get("longitude", 0))
        except (TypeError, ValueError):
            return False

        if amount <= 0 or amount_usd <= 0:
            return False
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            return False

        self.seen_tx_ids.add(tx_id)
        return True

    def evaluate_transaction(self, tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Evalúa una transacción entrante contra las 3 reglas de fraude con estado.
        Retorna el evento complejo de alerta si se detecta fraude.
        """
        t0 = time.perf_counter()
        if not self._is_valid_for_cep(tx):
            return None

        tx_id = tx["transaction_id"]
        card_id = tx["card_id"]
        cust_id = tx["customer_id"]
        amount = float(tx["amount"])
        amount_usd = float(tx["amount_usd"])
        lat = float(tx["latitude"])
        lon = float(tx["longitude"])
        ts_str = tx["timestamp"]
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

        # ----------------------------------------------------------------------
        # Regla 1: Viaje Imposible (Impossible Travel Velocity)
        # ----------------------------------------------------------------------
        if card_id in self.card_geo_state:
            prev = self.card_geo_state[card_id]
            dist_km, dt_hours, speed_kmh = calculate_travel_velocity_kmh(
                prev["last_lat"], prev["last_lon"], prev["last_ts"],
                lat, lon, ts
            )

            if dist_km > self.min_distance_km and speed_kmh > self.max_speed_kmh:
                latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                logger.warning(
                    f"[ALERTA BLOQUEO] Viaje Imposible en {card_id}! "
                    f"Distancia: {dist_km:.1f} km en {dt_hours*60:.1f} min ({speed_kmh:,.0f} km/h). "
                    f"De {prev['city']} a {tx['city']}."
                )
                self.card_geo_state[card_id] = {
                    "last_lat": lat, "last_lon": lon, "last_ts": ts, "city": tx["city"]
                }
                return {
                    "alert_id": str(uuid.uuid4()),
                    "transaction_id": tx_id,
                    "related_transaction_ids": [prev["tx_id"], tx_id],
                    "rule_triggered": "IMPOSSIBLE_TRAVEL",
                    "card_id": card_id,
                    "customer_id": cust_id,
                    "merchant_name": tx.get("merchant_name", "UNKNOWN"),
                    "merchant_category": tx.get("merchant_category", "UNKNOWN"),
                    "amount_usd": round(amount_usd, 2),
                    "currency": tx["currency"],
                    "from_location": prev["city"],
                    "to_location": tx["city"],
                    "city": tx["city"],
                    "country": tx["country"],
                    "details": f"Velocidad imposible: {speed_kmh:,.0f} km/h ({dist_km:.0f} km en {dt_hours*60:.1f} min)",
                    "detection_latency_ms": latency_ms,
                    "timestamp": ts_str,
                    "status": "BLOCKED",
                }

        self.card_geo_state[card_id] = {
            "last_lat": lat, "last_lon": lon, "last_ts": ts, "city": tx["city"], "tx_id": tx_id
        }

        # ----------------------------------------------------------------------
        # Regla 2: Ataque de Bots (Card Testing / Micro-transactions Burst)
        # ----------------------------------------------------------------------
        if card_id not in self.card_recent_txs:
            self.card_recent_txs[card_id] = []

        cutoff_bot = ts - timedelta(seconds=self.bot_window_sec)
        self.card_recent_txs[card_id] = [
            (t, amt, tid) for (t, amt, tid) in self.card_recent_txs[card_id] if t >= cutoff_bot
        ]

        if amount_usd <= self.bot_max_usd:
            self.card_recent_txs[card_id].append((ts, amount_usd, tx_id))

        if len(self.card_recent_txs[card_id]) >= self.bot_min_count:
            burst_txs = self.card_recent_txs[card_id]
            burst_total_usd = round(sum(amt for _, amt, _ in burst_txs), 2)
            related_ids = [tid for _, _, tid in burst_txs]
            latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)

            logger.warning(
                f"[ALERTA BLOQUEO] Ataque de Bots en {card_id}! "
                f"{len(burst_txs)} micro-transacciones en <= {self.bot_window_sec}s "
                f"(Total bloqueado: ${burst_total_usd:.2f} USD)."
            )
            # Reiniciar ventana tras bloquear la ráfaga para emitir 1 alerta consolidada por incidente
            self.card_recent_txs[card_id] = []
            return {
                "alert_id": str(uuid.uuid4()),
                "transaction_id": tx_id,
                "related_transaction_ids": related_ids,
                "rule_triggered": "BOT_BURST_CARD_TESTING",
                "card_id": card_id,
                "customer_id": cust_id,
                "merchant_name": tx.get("merchant_name", "UNKNOWN"),
                "merchant_category": tx.get("merchant_category", "UNKNOWN"),
                "amount_usd": burst_total_usd,
                "currency": tx["currency"],
                "from_location": tx["city"],
                "to_location": tx["city"],
                "city": tx["city"],
                "country": tx["country"],
                "details": f"Ráfaga de {len(burst_txs)} micro-transacciones consecutivas (<= ${self.bot_max_usd} USD)",
                "detection_latency_ms": latency_ms,
                "timestamp": ts_str,
                "status": "BLOCKED",
            }

        # ----------------------------------------------------------------------
        # Regla 3: Pitufeo / AML Structuring (Evasión de límite de $10M COP)
        # ----------------------------------------------------------------------
        if tx["currency"] == "COP" and (self.aml_min_cop <= amount <= self.aml_max_cop):
            if cust_id not in self.customer_aml_txs:
                self.customer_aml_txs[cust_id] = []

            cutoff_aml = ts - timedelta(hours=self.aml_window_hours)
            self.customer_aml_txs[cust_id] = [
                (t, cop, usd, tid) for (t, cop, usd, tid) in self.customer_aml_txs[cust_id] if t >= cutoff_aml
            ]
            self.customer_aml_txs[cust_id].append((ts, amount, amount_usd, tx_id))

            if len(self.customer_aml_txs[cust_id]) >= self.aml_min_count:
                aml_txs = self.customer_aml_txs[cust_id]
                total_cop = sum(cop for _, cop, _, _ in aml_txs)
                total_usd = round(sum(usd for _, _, usd, _ in aml_txs), 2)
                related_ids = [tid for _, _, _, tid in aml_txs]
                latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)

                logger.warning(
                    f"[ALERTA AML] Structuring / Pitufeo detectado en cliente {cust_id}! "
                    f"{len(aml_txs)} transferencias cercanas a $10M COP (Total: ${total_cop:,.0f} COP / ${total_usd:,.2f} USD)."
                )
                self.customer_aml_txs[cust_id] = []
                return {
                    "alert_id": str(uuid.uuid4()),
                    "transaction_id": tx_id,
                    "related_transaction_ids": related_ids,
                    "rule_triggered": "AML_STRUCTURING",
                    "card_id": card_id,
                    "customer_id": cust_id,
                    "merchant_name": tx.get("merchant_name", "UNKNOWN"),
                    "merchant_category": tx.get("merchant_category", "UNKNOWN"),
                    "amount_usd": total_usd,
                    "currency": tx["currency"],
                    "from_location": tx["city"],
                    "to_location": tx["city"],
                    "city": tx["city"],
                    "country": tx["country"],
                    "details": f"{len(aml_txs)} transferencias fraccionadas acumuladas: ${total_cop:,.0f} COP",
                    "detection_latency_ms": latency_ms,
                    "timestamp": ts_str,
                    "status": "FLAGGED_FOR_AUDIT",
                }

        return None


def run_file_streaming_pipeline(
    input_file: str,
    lakehouse_dir: str = "./data/s3_lakehouse",
    config_path: str = "config/fraud_rules_config.yaml",
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Ejecuta el motor de detección sobre un flujo de entrada, escribiendo:
    1. Todos los eventos crudos recibidos en `raw/transactions/raw_stream.ndjson`
    2. Todas las alertas CEP disparadas en `raw/fraud_alerts/alerts_stream.ndjson`
    """
    cfg = load_fraud_rules_config(config_path)
    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    raw_tx_dir = os.path.join(lakehouse_dir, "raw", "transactions")
    raw_alerts_dir = os.path.join(lakehouse_dir, "raw", "fraud_alerts")
    os.makedirs(raw_tx_dir, exist_ok=True)
    os.makedirs(raw_alerts_dir, exist_ok=True)

    raw_tx_out = os.path.join(raw_tx_dir, "raw_stream.ndjson")
    raw_alerts_out = os.path.join(raw_alerts_dir, "alerts_stream.ndjson")

    logger.info(f"Procesando flujo desde: {input_file}")
    alerts_emitted: List[Dict[str, Any]] = []
    total_events = 0

    with open(input_file, "r", encoding="utf-8") as fin, \
         open(raw_tx_out, "w", encoding="utf-8") as f_raw, \
         open(raw_alerts_out, "w", encoding="utf-8") as f_alert:

        for line in fin:
            if not line.strip():
                continue
            total_events += 1
            f_raw.write(line if line.endswith("\n") else line + "\n")

            tx = json.loads(line)
            alert = evaluator.evaluate_transaction(tx)
            if alert:
                alerts_emitted.append(alert)
                f_alert.write(json.dumps(alert) + "\n")

    total_saved_usd = sum(a["amount_usd"] for a in alerts_emitted)
    logger.info(
        f"[Streaming Completado] Eventos recibidos en Raw: {total_events:,} | "
        f"Alertas CEP emitidas: {len(alerts_emitted)} | "
        f"Monto protegido en vivo: ${total_saved_usd:,.2f} USD"
    )
    logger.info(f" -> Sink Raw Transacciones: {raw_tx_out}")
    logger.info(f" -> Sink Raw Alertas CEP:   {raw_alerts_out}")
    return total_events, alerts_emitted


def run_spark_streaming_job(
    config_path: str = "config/fraud_rules_config.yaml",
    s3_raw_output: str = "./data/s3_lakehouse/raw/transactions",
    s3_alerts_output: str = "./data/s3_lakehouse/raw/fraud_alerts",
    checkpoint_dir: str = "./data/checkpoints/streaming",
):
    """
    Lanza el job distribuido en Apache Spark Structured Streaming.
    Evalúa cada micro-batch con `PureStreamFraudEvaluator`, publica alertas
    al tópico Kafka `fraud-alerts` y vuelca eventos crudos a S3 Raw.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark no está disponible en el entorno actual.")

    cfg = load_fraud_rules_config(config_path)
    kafka_cfg = cfg.get("kafka", {})
    stream_cfg = cfg.get("streaming", {})

    kafka_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    input_topic = kafka_cfg.get("transactions_topic", "financial-transactions")
    alerts_topic = kafka_cfg.get("alerts_topic", "fraud-alerts")
    watermark_sec = stream_cfg.get("watermark_delay_seconds", 10)
    microbatch_sec = stream_cfg.get("microbatch_duration_seconds", 3)

    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    spark = (
        SparkSession.builder
        .appName("FintechRealTimeFraudDetector_StatefulCEP")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = get_transaction_spark_schema()
    df_parsed = (
        df_raw
        .selectExpr("CAST(value AS STRING) as json_val")
        .select(from_json(col("json_val"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", f"{watermark_sec} seconds")
    )

    os.makedirs(s3_raw_output, exist_ok=True)
    os.makedirs(s3_alerts_output, exist_ok=True)

    def process_microbatch(batch_df: "DataFrame", batch_id: int):
        records = [row.asDict() for row in batch_df.collect()]
        if not records:
            return

        # 1. Volcado continuo a S3 Raw
        raw_file = os.path.join(s3_raw_output, f"part_batch_{batch_id}.ndjson")
        with open(raw_file, "a", encoding="utf-8") as f_raw:
            for rec in records:
                rec.pop("event_time", None)
                f_raw.write(json.dumps(rec) + "\n")

        # 2. Evaluación Stateful CEP y publicación a `fraud-alerts`
        batch_alerts = []
        for rec in records:
            alert = evaluator.evaluate_transaction(rec)
            if alert:
                batch_alerts.append(alert)

        if batch_alerts:
            alert_file = os.path.join(s3_alerts_output, f"alerts_batch_{batch_id}.ndjson")
            with open(alert_file, "a", encoding="utf-8") as f_al:
                for al in batch_alerts:
                    f_al.write(json.dumps(al) + "\n")

            # Publicar alertas al tópico Kafka `fraud-alerts` en milisegundos
            from kafka import KafkaProducer
            al_prod = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
            )
            for al in batch_alerts:
                al_prod.send(alerts_topic, key=al["card_id"], value=al)
            al_prod.flush()
            al_prod.close()

    query = (
        df_parsed.writeStream
        .foreachBatch(process_microbatch)
        .option("checkpointLocation", os.path.join(checkpoint_dir, "cep_processor"))
        .trigger(processingTime=f"{microbatch_sec} seconds")
        .start()
    )

    logger.info(f"Motor Spark Streaming CEP activo (Micro-batch: {microbatch_sec}s, Watermark: {watermark_sec}s)")
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Motor de Detección de Fraude en Streaming")
    parser.add_argument("--mode", choices=["spark", "test_file"], default="test_file")
    parser.add_argument("--config", default="config/fraud_rules_config.yaml")
    parser.add_argument("--input-file", default="./data/sample_transactions/test_sample_all_rules.ndjson")
    parser.add_argument("--lakehouse-dir", default="./data/s3_lakehouse")
    args = parser.parse_args()

    if args.mode == "test_file":
        run_file_streaming_pipeline(
            input_file=args.input_file,
            lakehouse_dir=args.lakehouse_dir,
            config_path=args.config,
        )
    else:
        run_spark_streaming_job(config_path=args.config)
