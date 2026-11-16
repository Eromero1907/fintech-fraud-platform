"""
Productor y Simulador de Transacciones Financieras de Alta Velocidad.

Genera transacciones financieras sintéticas realistas para Apache Kafka
con particionamiento por `card_id`, lectura dinámica de `config/fraud_rules_config.yaml`
e inyección controlada de los 3 escenarios de fraude (Stateful CEP) y anomalías técnicas
de calidad de datos para validar la Zona de Cuarentena en el Data Lakehouse.
"""

import argparse
import copy
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.config_loader import load_fraud_rules_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("FinancialProducer")

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("Librería 'kafka-python' no instalada. Habilitado modo Dry-Run / Archivo.")


CITIES = [
    {"city": "Bogota", "country": "COL", "lat": 4.7110, "lon": -74.0721, "currency": "COP"},
    {"city": "Medellin", "country": "COL", "lat": 6.2442, "lon": -75.5812, "currency": "COP"},
    {"city": "Cali", "country": "COL", "lat": 3.4516, "lon": -76.5320, "currency": "COP"},
    {"city": "Miami", "country": "USA", "lat": 25.7617, "lon": -80.1918, "currency": "USD"},
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060, "currency": "USD"},
    {"city": "Madrid", "country": "ESP", "lat": 40.4168, "lon": -3.7038, "currency": "EUR"},
    {"city": "CDMX", "country": "MEX", "lat": 19.4326, "lon": -99.1332, "currency": "USD"},
]

MERCHANTS = [
    {"id": "MERC-101", "name": "Exito Supermercados", "category": "grocery"},
    {"id": "MERC-202", "name": "Starbucks Coffee", "category": "restaurant"},
    {"id": "MERC-303", "name": "Amazon Online Store", "category": "electronics"},
    {"id": "MERC-404", "name": "Texaco Gas Station", "category": "gas_station"},
    {"id": "MERC-505", "name": "Zara Fashion", "category": "luxury"},
    {"id": "MERC-606", "name": "Apple Store", "category": "electronics"},
    {"id": "MERC-707", "name": "Cajero Bancolombia", "category": "atm_cash"},
]


class FinancialProducerSimulator:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        target_eps: int = 1000,
        dry_run: bool = False,
        config_path: str = "config/fraud_rules_config.yaml",
    ):
        self.cfg = load_fraud_rules_config(config_path)
        kafka_cfg = self.cfg.get("kafka", {})

        self.bootstrap_servers = bootstrap_servers or kafka_cfg.get("bootstrap_servers", "localhost:9092")
        self.topic = topic or kafka_cfg.get("transactions_topic", "financial-transactions")
        self.target_eps = int(target_eps)
        self.dry_run = dry_run or (not KAFKA_AVAILABLE)
        self.producer = None

        # Asignar ciudad base fija para cada tarjeta (comportamiento bancario real)
        self.card_home_cities: Dict[str, Dict] = {}
        self._fraud_cycle_idx = 0

        if not self.dry_run:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                    acks=1,
                    linger_ms=5,
                    batch_size=32768,
                )
                logger.info(f"Conectado a Kafka Broker ({self.bootstrap_servers}). Tópico: '{self.topic}'")
            except Exception as e:
                logger.error(f"No se pudo conectar a Kafka ({e}). Pasando a modo Dry-Run.")
                self.dry_run = True

    def create_transaction(
        self,
        card_id: str,
        customer_id: Optional[str],
        city_info: Dict,
        amount: float,
        timestamp: datetime,
        channel: str = "POS",
        scenario: str = "NONE",
    ) -> Dict:
        """Crea un registro de transacción estandarizado conforme al esquema JSON."""
        currency = city_info["currency"]
        if currency == "COP":
            amount_usd = round(amount / 4100.0, 2)
        elif currency == "EUR":
            amount_usd = round(amount * 1.08, 2)
        else:
            amount_usd = round(amount, 2)

        merchant = random.choice(MERCHANTS)

        # Perturbación leve de coordenadas dentro de la ciudad (+/- 4 km)
        lat = city_info["lat"] + random.uniform(-0.03, 0.03)
        lon = city_info["lon"] + random.uniform(-0.03, 0.03)

        return {
            "transaction_id": str(uuid.uuid4()),
            "card_id": card_id,
            "customer_id": customer_id,
            "amount": round(amount, 2),
            "currency": currency,
            "amount_usd": amount_usd,
            "merchant_id": merchant["id"],
            "merchant_name": merchant["name"],
            "merchant_category": merchant["category"],
            "channel": channel,
            "country": city_info["country"],
            "city": city_info["city"],
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "is_fraud_scenario": scenario,
        }

    def generate_fraud_burst_scenario(self, base_time: datetime) -> List[Dict]:
        """Genera un ataque de bots con micro-transacciones en ráfaga sobre una misma tarjeta."""
        events = []
        card_id = f"CARD-BOT-{random.randint(1000, 9999)}"
        cust_id = f"CUST-VICTIM-{random.randint(100, 999)}"
        city = CITIES[3]  # Miami (USD)

        t = base_time
        for _ in range(5):
            t += timedelta(seconds=random.uniform(1.0, 2.5))
            amount = random.uniform(1.10, 3.80)
            events.append(self.create_transaction(
                card_id, cust_id, city, amount, t, channel="WEB", scenario="BOT_BURST"
            ))
        return events

    def generate_impossible_travel_scenario(self, base_time: datetime) -> List[Dict]:
        """Genera dos transacciones con velocidad física imposible (Bogotá -> Madrid en 5 min)."""
        card_id = f"CARD-CLONED-{random.randint(1000, 9999)}"
        cust_id = f"CUST-TRAVEL-{random.randint(100, 999)}"

        city_col = CITIES[0]  # Bogotá
        city_esp = CITIES[5]  # Madrid

        t1 = base_time
        tx1 = self.create_transaction(
            card_id, cust_id, city_col, amount=85000.0, timestamp=t1, channel="POS", scenario="NONE"
        )

        t2 = t1 + timedelta(minutes=random.randint(4, 7))
        tx2 = self.create_transaction(
            card_id, cust_id, city_esp, amount=620.0, timestamp=t2, channel="POS", scenario="IMPOSSIBLE_TRAVEL"
        )
        return [tx1, tx2]

    def generate_aml_structuring_scenario(self, base_time: datetime) -> List[Dict]:
        """Genera 3 transferencias justo por debajo del límite regulatorio de $10M COP."""
        events = []
        card_id = f"CARD-AML-{random.randint(1000, 9999)}"
        cust_id = f"CUST-SMURF-{random.randint(100, 999)}"
        city = CITIES[0]  # Bogotá (COP)

        t = base_time
        for _ in range(3):
            t += timedelta(minutes=random.randint(5, 12))
            amount = random.uniform(9400000.0, 9850000.0)
            events.append(self.create_transaction(
                card_id, cust_id, city, amount, t, channel="ATM", scenario="AML_STRUCTURING"
            ))
        return events

    def generate_dirty_quality_records(self, sample_valid_tx: Dict, base_time: datetime) -> List[Dict]:
        """
        Inyecta registros con anomalías técnicas de calidad para demostrar la
        Zona de Cuarentena y el Reporte de Reconciliación en la capa Batch Medallion:
        1. Duplicado exacto de clave primaria (DUPLICATE_RECORD)
        2. Monto negativo o cero (INVALID_AMOUNT)
        3. Campo obligatorio nulo (NULL_VALUE)
        4. Coordenadas GPS fuera de rango [-90, 90] (INVALID_COORDINATES)
        """
        # 1. Duplicado exacto
        dup_tx = copy.deepcopy(sample_valid_tx)

        # 2. Monto inválido (<= 0)
        neg_tx = self.create_transaction(
            "CARD-ERR-01", "CUST-ERR-01", CITIES[0], amount=-250.0, timestamp=base_time
        )
        neg_tx["amount_usd"] = -250.0

        # 3. Campo requerido nulo
        null_tx = self.create_transaction(
            "CARD-ERR-02", None, CITIES[1], amount=50000.0, timestamp=base_time
        )

        # 4. Coordenadas corruptas
        coord_tx = self.create_transaction(
            "CARD-ERR-03", "CUST-ERR-03", CITIES[2], amount=100.0, timestamp=base_time
        )
        coord_tx["latitude"] = 145.999  # Imposible (> 90 grados)

        return [dup_tx, neg_tx, null_tx, coord_tx]

    def run_stream(
        self,
        duration_seconds: int = 12,
        fraud_frequency_seconds: int = 2,
        output_file: Optional[str] = None,
        inject_dirty: bool = True,
    ):
        """Ejecuta el generador continuo de transacciones controlando la tasa EPS."""
        random.seed(42)
        logger.info(f"Iniciando emisión a {self.target_eps} EPS | Duración: {duration_seconds}s")
        start_time = time.time()
        sent_events = 0
        last_fraud_injection = start_time
        dirty_injected = False
        file_handle = open(output_file, "w", encoding="utf-8") if output_file else None

        current_sim_time = datetime(2026, 11, 12, 14, 0, 0)
        scenarios_order = ["IMPOSSIBLE_TRAVEL", "BOT_BURST", "AML"]

        try:
            while (time.time() - start_time) < duration_seconds:
                batch_start = time.time()
                events_in_batch = []

                # Inyección determinística rotativa de los 3 escenarios de fraude
                if (time.time() - last_fraud_injection) >= fraud_frequency_seconds:
                    scenario_choice = scenarios_order[self._fraud_cycle_idx % len(scenarios_order)]
                    self._fraud_cycle_idx += 1

                    if scenario_choice == "IMPOSSIBLE_TRAVEL":
                        events_in_batch.extend(self.generate_impossible_travel_scenario(current_sim_time))
                        logger.warning(">>> Inyectando Escenario de Fraude: IMPOSSIBLE TRAVEL (Bogota -> Madrid)")
                    elif scenario_choice == "BOT_BURST":
                        events_in_batch.extend(self.generate_fraud_burst_scenario(current_sim_time))
                        logger.warning(">>> Inyectando Escenario de Fraude: BOT CARD TESTING BURST")
                    else:
                        events_in_batch.extend(self.generate_aml_structuring_scenario(current_sim_time))
                        logger.warning(">>> Inyectando Escenario de Fraude: AML STRUCTURING (Pitufeo)")
                    last_fraud_injection = time.time()

                # Rellenar con tráfico normal legítimo
                target_normal_count = max(1, int(self.target_eps * 0.1))
                for _ in range(target_normal_count):
                    card_id = f"CARD-{random.randint(1000, 3000)}"
                    cust_id = f"CUST-{random.randint(100, 800)}"
                    if card_id not in self.card_home_cities:
                        self.card_home_cities[card_id] = random.choice(CITIES)
                    city = self.card_home_cities[card_id]
                    amount = random.uniform(8.0, 250.0) if city["currency"] != "COP" else random.uniform(25000, 350000)
                    channel = random.choice(["POS", "POS", "WEB", "ATM"])
                    current_sim_time += timedelta(milliseconds=random.randint(10, 50))
                    events_in_batch.append(
                        self.create_transaction(card_id, cust_id, city, amount, current_sim_time, channel, "NONE")
                    )

                # Inyectar un lote controlado de registros corruptos/duplicados para prueba de Cuarentena
                if inject_dirty and not dirty_injected and len(events_in_batch) > 0:
                    dirty_records = self.generate_dirty_quality_records(events_in_batch[-1], current_sim_time)
                    events_in_batch.extend(dirty_records)
                    dirty_injected = True
                    logger.info(">>> Inyectados 4 registros con anomalías de calidad (Duplicado, Null, Monto<=0, GPS inválido)")

                # Envío de eventos (a Kafka o archivo)
                for tx in events_in_batch:
                    if not self.dry_run and self.producer:
                        self.producer.send(self.topic, key=tx["card_id"], value=tx)
                    if file_handle:
                        file_handle.write(json.dumps(tx) + "\n")
                    sent_events += 1

                elapsed_batch = time.time() - batch_start
                sleep_time = 0.1 - elapsed_batch
                if sleep_time > 0:
                    time.sleep(sleep_time)

        finally:
            if not self.dry_run and self.producer:
                self.producer.flush()
            if file_handle:
                file_handle.close()

        total_elapsed = time.time() - start_time
        actual_eps = sent_events / total_elapsed if total_elapsed > 0 else 0
        logger.info(
            f"[Finalizado] Total eventos: {sent_events:,} en {total_elapsed:.2f}s "
            f"(Promedio: {actual_eps:.1f} EPS)"
        )


def main():
    parser = argparse.ArgumentParser(description="Simulador de Transacciones Bancarias")
    parser.add_argument("--config", default="config/fraud_rules_config.yaml", help="Ruta a configuración YAML")
    parser.add_argument("--servers", default=None, help="Kafka Bootstrap Servers")
    parser.add_argument("--topic", default=None, help="Tópico de salida")
    parser.add_argument("--eps", type=int, default=1000, help="Transacciones por segundo")
    parser.add_argument("--duration", type=int, default=12, help="Duración en segundos")
    parser.add_argument("--fraud-interval", type=int, default=2, help="Intervalo de inyección de fraude")
    parser.add_argument("--save-file", default=None, help="Ruta para guardar copia NDJSON")
    parser.add_argument("--no-dirty", action="store_true", help="No inyectar registros sucios de prueba")
    parser.add_argument("--dry-run", action="store_true", help="Ejecutar sin Kafka")
    args = parser.parse_args()

    producer = FinancialProducerSimulator(
        bootstrap_servers=args.servers,
        topic=args.topic,
        target_eps=args.eps,
        dry_run=args.dry_run,
        config_path=args.config,
    )
    producer.run_stream(
        duration_seconds=args.duration,
        fraud_frequency_seconds=args.fraud_interval,
        output_file=args.save_file,
        inject_dirty=not args.no_dirty,
    )


if __name__ == "__main__":
    main()
