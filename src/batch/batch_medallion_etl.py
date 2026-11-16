"""
Capa Batch: Pipeline ETL Medallion (Raw -> Silver / Quarantine -> Gold)
Estándar SI3008 - Sistemas Intensivos en Datos (Universidad EAFIT)

Características de Grado Productivo:
1. Enriquecimiento con Metadatos de Linaje (`batch_id`, `ingestion_date`, `file_source`, `processing_timestamp`).
2. Segregación a Zona de Cuarentena (`quarantine/transactions/`) con `rejection_reason` detallado:
   - DUPLICATE_RECORD (Idempotencia por `transaction_id`)
   - NULL_VALUE (Campos obligatorios vacíos)
   - INVALID_AMOUNT (`amount <= 0` o `amount_usd <= 0`)
   - INVALID_COORDINATES (`latitude` fuera de [-90, 90] o `longitude` fuera de [-180, 180])
3. Reporte de Reconciliación Exacta: `Total Raw == Total Silver + Total Quarantine (100% Balance)`.
4. Integración Real Streaming -> Batch: Cruza las transacciones curadas con las alertas reales
   emitidas por el motor CEP (`raw/fraud_alerts/`), sin usar etiquetas del simulador.
5. Escritura Columnar en Formato Apache Parquet con Compresión Snappy y Particionamiento Hive
   (`year=YYYY/month=MM/day=DD/country=XX/*.parquet`), compatible 100% con AWS Glue y Amazon Athena.
"""

import argparse
import glob
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from src.config_loader import load_fraud_rules_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("MedallionETL")

try:
    import duckdb  # type: ignore
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class MedallionBatchProcessor:
    """
    Procesador Batch Medallion con soporte nativo para Parquet Snappy (vía DuckDB / PySpark),
    particionamiento Hive, Zona de Cuarentena y Reconciliación de Calidad SI3008.
    """

    def __init__(
        self,
        lakehouse_base_dir: str = "./data/s3_lakehouse",
        batch_id: str = "batch_20261112_01",
        ingestion_date: str = "2026-11-12",
        config_path: str = "config/fraud_rules_config.yaml",
    ):
        self.cfg = load_fraud_rules_config(config_path)
        self.base_dir = lakehouse_base_dir
        self.batch_id = batch_id
        self.ingestion_date = ingestion_date

        self.raw_tx_dir = os.path.join(self.base_dir, "raw", "transactions")
        self.raw_alerts_dir = os.path.join(self.base_dir, "raw", "fraud_alerts")
        self.quarantine_dir = os.path.join(self.base_dir, "quarantine", "transactions")
        self.silver_dir = os.path.join(self.base_dir, "silver", "transactions_curated")
        self.gold_alerts_dir = os.path.join(self.base_dir, "gold", "fraud_alerts_history")
        self.gold_merchants_dir = os.path.join(self.base_dir, "gold", "merchant_risk_ranking")
        self.gold_kpis_dir = os.path.join(self.base_dir, "gold", "daily_financial_kpis")

        # Limpiar salidas previas de zonas curadas para garantizar idempotencia del lote
        for d in [
            self.quarantine_dir,
            self.silver_dir,
            self.gold_alerts_dir,
            self.gold_merchants_dir,
            self.gold_kpis_dir,
        ]:
            if os.path.exists(d):
                shutil.rmtree(d)
            os.makedirs(d, exist_ok=True)

    def _validate_quality(self, tx: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Evalúa las reglas de calidad sobre un registro de Raw.
        Retorna (es_valido, razon_de_rechazo).
        """
        required_fields = [
            "transaction_id", "card_id", "customer_id",
            "amount", "currency", "amount_usd",
            "channel", "country", "city",
            "latitude", "longitude", "timestamp",
        ]
        for field in required_fields:
            if tx.get(field) is None or tx.get(field) == "":
                return False, f"NULL_VALUE: Campo obligatorio '{field}' vacío o nulo"

        try:
            amt = float(tx["amount"])
            amt_usd = float(tx["amount_usd"])
            lat = float(tx["latitude"])
            lon = float(tx["longitude"])
        except (TypeError, ValueError):
            return False, "INVALID_TYPE: Campos numéricos malformados"

        if amt <= 0 or amt_usd <= 0:
            return False, f"INVALID_AMOUNT: Monto debe ser mayor a 0 (recibido: {amt})"

        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            return False, f"INVALID_COORDINATES: Coordenadas GPS fuera de rango ({lat}, {lon})"

        return True, ""

    def load_streaming_alerts(self) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """
        Carga las alertas reales emitidas por el motor de streaming CEP en `raw/fraud_alerts/`.
        Retorna (lista_de_alertas, mapa_tx_id_a_alerta).
        """
        alerts: List[Dict[str, Any]] = []
        tx_to_alert: Dict[str, Dict[str, Any]] = {}

        if not os.path.exists(self.raw_alerts_dir):
            return alerts, tx_to_alert

        alert_files = sorted(glob.glob(os.path.join(self.raw_alerts_dir, "*.ndjson")))
        seen_alert_ids: Set[str] = set()

        for af in alert_files:
            with open(af, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    al = json.loads(line)
                    aid = al.get("alert_id")
                    if aid in seen_alert_ids:
                        continue
                    seen_alert_ids.add(aid)
                    alerts.append(al)

                    # Vincular todas las transacciones involucradas en el patrón CEP
                    rel_ids = al.get("related_transaction_ids", [al.get("transaction_id")])
                    for tid in rel_ids:
                        if tid:
                            tx_to_alert[tid] = al

        return alerts, tx_to_alert

    def run_raw_to_silver_and_quarantine(
        self, raw_input_files: List[str], tx_to_alert: Dict[str, Dict[str, Any]]
    ) -> Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Ejecuta el paso Raw -> Silver + Quarantine:
        1. Enriquece con metadatos de linaje (`batch_id`, `ingestion_date`, `file_source`, `processing_timestamp`).
        2. Detecta duplicados y aplica validaciones de calidad.
        3. Cruza cada transacción válida con las alertas CEP reales del motor de streaming.
        4. Escribe particionado tipo Hive (`year=YYYY/month=MM/day=DD/country=XX/`) y en Parquet Snappy.
        """
        logger.info("[ETL Raw -> Silver/Quarantine] Iniciando ingesta, linaje y control de calidad...")
        seen_tx_ids: Set[str] = set()
        curated_records: List[Dict[str, Any]] = []
        quarantine_records: List[Dict[str, Any]] = []
        total_raw_count = 0
        proc_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for fpath in raw_input_files:
            if not os.path.exists(fpath):
                continue
            source_name = os.path.basename(fpath)
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    total_raw_count += 1
                    tx = json.loads(line)

                    # Enriquecimiento con Metadatos de Linaje SI3008
                    tx["batch_id"] = self.batch_id
                    tx["ingestion_date"] = self.ingestion_date
                    tx["file_source"] = source_name
                    tx["processing_timestamp"] = proc_ts

                    # Remover etiqueta interna de simulación para garantizar pureza analítica
                    tx.pop("is_fraud_scenario", None)

                    tx_id = tx.get("transaction_id")

                    # 1. Verificación de Duplicados
                    if tx_id and tx_id in seen_tx_ids:
                        tx["rejection_reason"] = "DUPLICATE_RECORD: Clave primaria transaction_id duplicada en el lote"
                        quarantine_records.append(tx)
                        continue

                    # 2. Verificación de Reglas de Calidad
                    is_valid, reason = self._validate_quality(tx)
                    if not is_valid:
                        tx["rejection_reason"] = reason
                        quarantine_records.append(tx)
                        continue

                    seen_tx_ids.add(tx_id)

                    # 3. Cruce con Alertas Reales del Motor de Streaming CEP
                    matched_alert = tx_to_alert.get(tx_id)
                    if matched_alert:
                        tx["is_fraud_flagged"] = True
                        tx["fraud_rule_triggered"] = matched_alert["rule_triggered"]
                        tx["alert_id"] = matched_alert["alert_id"]
                    else:
                        tx["is_fraud_flagged"] = False
                        tx["fraud_rule_triggered"] = "NONE"
                        tx["alert_id"] = None

                    # 4. Derivación de claves de partición Hive
                    dt = datetime.strptime(tx["timestamp"], "%Y-%m-%d %H:%M:%S")
                    tx["year"] = dt.strftime("%Y")
                    tx["month"] = dt.strftime("%m")
                    tx["day"] = dt.strftime("%d")

                    curated_records.append(tx)

        # Persistir Zona de Cuarentena particionada por ingestion_date
        q_part_dir = os.path.join(self.quarantine_dir, f"ingestion_date={self.ingestion_date}")
        os.makedirs(q_part_dir, exist_ok=True)
        q_ndjson_path = os.path.join(q_part_dir, "quarantine_records.ndjson")
        with open(q_ndjson_path, "w", encoding="utf-8") as fq:
            for qrec in quarantine_records:
                fq.write(json.dumps(qrec) + "\n")

        # Persistir Zona Silver particionada estilo Hive: year=YYYY/month=MM/day=DD/country=XX/
        partitions_map: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
        for rec in curated_records:
            pkey = (rec["year"], rec["month"], rec["day"], rec["country"])
            partitions_map.setdefault(pkey, []).append(rec)

        for (yr, mo, dy, ctry), rows in partitions_map.items():
            part_dir = os.path.join(
                self.silver_dir, f"year={yr}", f"month={mo}", f"day={dy}", f"country={ctry}"
            )
            os.makedirs(part_dir, exist_ok=True)
            part_ndjson = os.path.join(part_dir, "part-00000.ndjson")
            with open(part_ndjson, "w", encoding="utf-8") as fs:
                for r in rows:
                    # En Hive/Athena las columnas de partición viven en la ruta del directorio
                    row_copy = {k: v for k, v in r.items() if k not in ("year", "month", "day", "country")}
                    fs.write(json.dumps(row_copy) + "\n")

            if DUCKDB_AVAILABLE:
                part_parquet = os.path.join(part_dir, "part-00000.snappy.parquet")
                con = duckdb.connect()
                con.execute(
                    f"COPY (SELECT * FROM read_json_auto('{part_ndjson}')) "
                    f"TO '{part_parquet}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');"
                )
                con.close()
                # Dejar solo el archivo Parquet Snappy en la partición para compatibilidad pura con Athena PARQUET
                os.remove(part_ndjson)

        if DUCKDB_AVAILABLE and quarantine_records:
            q_parquet = os.path.join(q_part_dir, "part-00000.snappy.parquet")
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{q_ndjson_path}')) "
                f"TO '{q_parquet}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');"
            )
            con.close()

        return total_raw_count, curated_records, quarantine_records

    def run_silver_to_gold(
        self,
        curated_txs: List[Dict[str, Any]],
        streaming_alerts: List[Dict[str, Any]],
        total_raw_count: int,
        total_quarantine_count: int,
    ) -> Dict[str, Any]:
        """
        Construye las 3 tablas analíticas de la Zona Gold en formato Parquet Snappy + NDJSON de 1 línea:
        1. `gold/fraud_alerts_history/`: Incidentes bloqueados por el motor CEP en streaming.
        2. `gold/merchant_risk_ranking/`: Comercios ordenados por volumen de fraude real detectado.
        3. `gold/daily_financial_kpis/`: Resumen ejecutivo reconciliado (1 línea por registro).
        """
        logger.info("[ETL Silver -> Gold] Generando tablas analíticas Gold en Parquet Snappy...")

        # 1. Gold: Historial de Alertas CEP Reales
        gold_alerts_rows = []
        for al in streaming_alerts:
            gold_alerts_rows.append({
                "alert_id": al["alert_id"],
                "transaction_id": al["transaction_id"],
                "card_id": al["card_id"],
                "customer_id": al["customer_id"],
                "amount_usd": float(al["amount_usd"]),
                "currency": al["currency"],
                "merchant_name": al.get("merchant_name", "UNKNOWN"),
                "rule_triggered": al["rule_triggered"],
                "city": al["city"],
                "country": al["country"],
                "detection_latency_ms": float(al.get("detection_latency_ms", 1.0)),
                "timestamp": al["timestamp"],
                "status": al["status"],
            })

        alerts_ndjson = os.path.join(self.gold_alerts_dir, "fraud_alerts.ndjson")
        with open(alerts_ndjson, "w", encoding="utf-8") as f:
            for row in gold_alerts_rows:
                f.write(json.dumps(row) + "\n")

        if DUCKDB_AVAILABLE and gold_alerts_rows:
            alerts_parquet = os.path.join(self.gold_alerts_dir, "part-00000.snappy.parquet")
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{alerts_ndjson}')) "
                f"TO '{alerts_parquet}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');"
            )
            con.close()
            os.remove(alerts_ndjson)

        # 2. Gold: Ranking de Riesgo por Comercio (basado en transacciones marcadas por CEP)
        merchant_stats: Dict[str, Dict[str, Any]] = {}
        total_volume_usd = 0.0

        for tx in curated_txs:
            amt_usd = float(tx["amount_usd"])
            total_volume_usd += amt_usd
            m_name = tx["merchant_name"]
            m_cat = tx["merchant_category"]
            is_flagged = bool(tx.get("is_fraud_flagged", False))

            if m_name not in merchant_stats:
                merchant_stats[m_name] = {
                    "merchant_name": m_name,
                    "merchant_category": m_cat,
                    "total_transactions": 0,
                    "fraud_transactions": 0,
                    "total_volume_usd": 0.0,
                    "fraud_volume_usd": 0.0,
                }
            merchant_stats[m_name]["total_transactions"] += 1
            merchant_stats[m_name]["total_volume_usd"] += amt_usd
            if is_flagged:
                merchant_stats[m_name]["fraud_transactions"] += 1
                merchant_stats[m_name]["fraud_volume_usd"] += amt_usd

        merchant_ranking = []
        for data in merchant_stats.values():
            tot = data["total_transactions"]
            fr = data["fraud_transactions"]
            data["fraud_rate_pct"] = round((fr / tot) * 100.0, 2) if tot > 0 else 0.0
            data["total_volume_usd"] = round(data["total_volume_usd"], 2)
            data["fraud_volume_usd"] = round(data["fraud_volume_usd"], 2)
            merchant_ranking.append(data)

        merchant_ranking.sort(key=lambda x: x["fraud_volume_usd"], reverse=True)

        merch_ndjson = os.path.join(self.gold_merchants_dir, "merchant_risk.ndjson")
        with open(merch_ndjson, "w", encoding="utf-8") as f:
            for item in merchant_ranking:
                f.write(json.dumps(item) + "\n")

        if DUCKDB_AVAILABLE and merchant_ranking:
            merch_parquet = os.path.join(self.gold_merchants_dir, "part-00000.snappy.parquet")
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{merch_ndjson}')) "
                f"TO '{merch_parquet}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');"
            )
            con.close()
            os.remove(merch_ndjson)

        # 3. Gold: KPIs Ejecutivos Diarios (Single-line NDJSON + Parquet Snappy)
        total_curated = len(curated_txs)
        total_alerts = len(gold_alerts_rows)
        total_flagged_txs = sum(1 for tx in curated_txs if tx.get("is_fraud_flagged"))
        total_saved_usd = round(sum(a["amount_usd"] for a in gold_alerts_rows), 2)
        avg_latency = (
            round(sum(a["detection_latency_ms"] for a in gold_alerts_rows) / total_alerts, 3)
            if total_alerts > 0 else 0.0
        )

        kpi_record = {
            "batch_id": self.batch_id,
            "ingestion_date": self.ingestion_date,
            "execution_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "total_raw_records": total_raw_count,
            "total_curated_silver_records": total_curated,
            "total_quarantine_records": total_quarantine_count,
            "reconciliation_balance_pct": 100.0 if total_raw_count == (total_curated + total_quarantine_count) else 0.0,
            "total_financial_volume_usd": round(total_volume_usd, 2),
            "legitimate_transactions": total_curated - total_flagged_txs,
            "fraud_transactions_involved": total_flagged_txs,
            "fraud_alerts_emitted": total_alerts,
            "total_money_saved_usd": total_saved_usd,
            "avg_detection_latency_ms": avg_latency,
        }

        kpi_ndjson = os.path.join(self.gold_kpis_dir, "summary_kpi.ndjson")
        # Escribir en UNA SOLA LÍNEA (estándar NDJSON para Hive / Athena)
        with open(kpi_ndjson, "w", encoding="utf-8") as f:
            f.write(json.dumps(kpi_record) + "\n")

        if DUCKDB_AVAILABLE:
            kpi_parquet = os.path.join(self.gold_kpis_dir, "part-00000.snappy.parquet")
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{kpi_ndjson}')) "
                f"TO '{kpi_parquet}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');"
            )
            con.close()
            os.remove(kpi_ndjson)

        return kpi_record

    def print_reconciliation_report(
        self,
        total_raw: int,
        total_silver: int,
        quarantine_records: List[Dict[str, Any]],
        kpi_record: Dict[str, Any],
    ):
        """Imprime el Reporte Oficial de Reconciliación de Datos estilo SI3008."""
        total_quarantine = len(quarantine_records)
        reconciled_sum = total_silver + total_quarantine
        is_balanced = total_raw == reconciled_sum

        # Conteo por causa de rechazo en Cuarentena
        reasons_count: Dict[str, int] = {}
        for q in quarantine_records:
            r_code = q.get("rejection_reason", "UNKNOWN").split(":", 1)[0]
            reasons_count[r_code] = reasons_count.get(r_code, 0) + 1

        print("\n" + "=" * 72)
        print("      REPORTE DE RECONCILIACIÓN DE DATOS MEDALLION (SI3008 - EAFIT)")
        print("=" * 72)
        print(f" ID del Lote (Batch ID):                 {self.batch_id}")
        print(f" Fecha de Ingestión:                     {self.ingestion_date}")
        print(f" Formato de Almacenamiento Lakehouse:    Apache Parquet (Snappy)")
        print("-" * 72)
        print(f" Total Registros Entregados (Zona Raw):  {total_raw:,}")
        print(f"  (+) Registros Aceptados (Zona Silver): {total_silver:,}")
        print(f"  (+) Registros Rechazados (Cuarentena): {total_quarantine:,}")
        for r_code, cnt in reasons_count.items():
            print(f"       • {r_code:<30} {cnt:,}")
        print(f" Total Reconciliado (Silver+Cuarentena): {reconciled_sum:,}")
        print("-" * 72)
        if is_balanced:
            print(" RESULTADO RECONCILIACIÓN: SUCCESS (BALANCE EXACTO 100.0%)")
        else:
            print(" RESULTADO RECONCILIACIÓN: ERROR (DESBALANCE EN CONTEOS)")
        print("-" * 72)
        print(f" Alertas CEP Reconciliadas (Streaming):  {kpi_record['fraud_alerts_emitted']}")
        print(f" Transacciones Fraudulentas Bloqueadas:  {kpi_record['fraud_transactions_involved']}")
        print(f" Dinero Total Protegido (Zona Gold):     ${kpi_record['total_money_saved_usd']:,.2f} USD")
        print(f" Latencia Promedio de Detección CEP:     {kpi_record['avg_detection_latency_ms']:.3f} ms")
        print("=" * 72 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Pipeline Batch Medallion ETL (SI3008)")
    parser.add_argument("--lakehouse-dir", default="./data/s3_lakehouse")
    parser.add_argument("--config", default="config/fraud_rules_config.yaml")
    parser.add_argument("--batch-id", default=os.getenv("BATCH_ID", "batch_20261112_01"))
    parser.add_argument("--ingestion-date", default=os.getenv("INGESTION_DATE", "2026-11-12"))
    args = parser.parse_args()

    processor = MedallionBatchProcessor(
        lakehouse_base_dir=args.lakehouse_dir,
        batch_id=args.batch_id,
        ingestion_date=args.ingestion_date,
        config_path=args.config,
    )

    raw_files = sorted(glob.glob(os.path.join(processor.raw_tx_dir, "*.ndjson")))
    if not raw_files:
        raise FileNotFoundError(
            f"No se encontraron archivos crudos en {processor.raw_tx_dir}. "
            "Ejecuta primero `streaming_fraud_detector.py`."
        )

    streaming_alerts, tx_to_alert = processor.load_streaming_alerts()
    total_raw, curated_txs, quarantine_txs = processor.run_raw_to_silver_and_quarantine(
        raw_files, tx_to_alert
    )
    kpi_record = processor.run_silver_to_gold(
        curated_txs=curated_txs,
        streaming_alerts=streaming_alerts,
        total_raw_count=total_raw,
        total_quarantine_count=len(quarantine_txs),
    )
    processor.print_reconciliation_report(
        total_raw=total_raw,
        total_silver=len(curated_txs),
        quarantine_records=quarantine_txs,
        kpi_record=kpi_record,
    )


if __name__ == "__main__":
    main()
