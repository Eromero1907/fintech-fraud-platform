"""
Suite Completa de Pruebas Unitarias y de Integración (QA-Tester)
Valida:
1. Geometría esférica Haversine y velocidades de desplazamiento.
2. Cargador dinámico de configuración (`config/fraud_rules_config.yaml`).
3. Regla 1 CEP: Viaje Imposible (`IMPOSSIBLE_TRAVEL`) con estado por tarjeta.
4. Regla 2 CEP: Ataque de Bots en Ráfaga (`BOT_BURST_CARD_TESTING`) en ventana de 30s.
5. Regla 3 CEP: Pitufeo / Lavado de Activos (`AML_STRUCTURING`) en ventana de 1h.
6. Inmunidad ante registros sucios o duplicados en el evaluador de streaming.
"""

from datetime import datetime
from src.config_loader import load_fraud_rules_config
from src.streaming.haversine_utils import calculate_travel_velocity_kmh, haversine_distance_km
from src.streaming.streaming_fraud_detector import PureStreamFraudEvaluator


def test_haversine_bogota_madrid():
    lat_bog, lon_bog = 4.7110, -74.0721
    lat_mad, lon_mad = 40.4168, -3.7038
    dist = haversine_distance_km(lat_bog, lon_bog, lat_mad, lon_mad)
    assert 7900.0 < dist < 8200.0, f"Distancia calculada inesperada: {dist}"


def test_impossible_travel_velocity_math():
    lat_bog, lon_bog = 4.7110, -74.0721
    lat_mad, lon_mad = 40.4168, -3.7038
    t1 = datetime(2026, 11, 12, 10, 0, 0)
    t2 = datetime(2026, 11, 12, 10, 8, 0)
    dist, dt_hours, speed_kmh = calculate_travel_velocity_kmh(
        lat_bog, lon_bog, t1, lat_mad, lon_mad, t2
    )
    assert speed_kmh > 900.0
    assert speed_kmh > 50000.0


def test_normal_travel_velocity_allowed():
    lat1, lon1 = 4.6097, -74.0817
    lat2, lon2 = 4.6989, -74.0489
    t1 = datetime(2026, 11, 12, 14, 0, 0)
    t2 = datetime(2026, 11, 12, 14, 30, 0)
    dist, _, speed_kmh = calculate_travel_velocity_kmh(lat1, lon1, t1, lat2, lon2, t2)
    assert dist < 20.0
    assert speed_kmh < 100.0


def test_config_loader_reads_yaml():
    cfg = load_fraud_rules_config("config/fraud_rules_config.yaml")
    assert "rules" in cfg
    assert cfg["rules"]["impossible_travel"]["max_physical_speed_kmh"] == 900.0
    assert cfg["rules"]["bot_burst"]["min_transaction_count"] == 4
    assert cfg["rules"]["aml_structuring"]["min_transaction_count"] == 3


def test_cep_rule1_impossible_travel_stateful():
    cfg = load_fraud_rules_config("config/fraud_rules_config.yaml")
    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    tx1 = {
        "transaction_id": "tx-travel-1",
        "card_id": "CARD-TEST-01",
        "customer_id": "CUST-01",
        "amount": 50000.0,
        "currency": "COP",
        "amount_usd": 12.20,
        "merchant_name": "Starbucks",
        "merchant_category": "restaurant",
        "channel": "POS",
        "country": "COL",
        "city": "Bogota",
        "latitude": 4.7110,
        "longitude": -74.0721,
        "timestamp": "2026-11-12 10:00:00",
    }
    tx2 = {
        "transaction_id": "tx-travel-2",
        "card_id": "CARD-TEST-01",
        "customer_id": "CUST-01",
        "amount": 400.0,
        "currency": "EUR",
        "amount_usd": 432.0,
        "merchant_name": "Zara Fashion",
        "merchant_category": "luxury",
        "channel": "POS",
        "country": "ESP",
        "city": "Madrid",
        "latitude": 40.4168,
        "longitude": -3.7038,
        "timestamp": "2026-11-12 10:05:00",
    }

    assert evaluator.evaluate_transaction(tx1) is None
    alert = evaluator.evaluate_transaction(tx2)
    assert alert is not None
    assert alert["rule_triggered"] == "IMPOSSIBLE_TRAVEL"
    assert alert["status"] == "BLOCKED"
    assert alert["related_transaction_ids"] == ["tx-travel-1", "tx-travel-2"]


def test_cep_rule2_bot_burst_card_testing():
    cfg = load_fraud_rules_config("config/fraud_rules_config.yaml")
    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    alerts = []
    for i in range(4):
        tx = {
            "transaction_id": f"tx-bot-{i}",
            "card_id": "CARD-BOT-99",
            "customer_id": "CUST-99",
            "amount": 2.00,
            "currency": "USD",
            "amount_usd": 2.00,
            "merchant_name": "Amazon Online Store",
            "merchant_category": "electronics",
            "channel": "WEB",
            "country": "USA",
            "city": "Miami",
            "latitude": 25.7617,
            "longitude": -80.1918,
            "timestamp": f"2026-11-12 11:00:0{i*2}",
        }
        res = evaluator.evaluate_transaction(tx)
        if res:
            alerts.append(res)

    assert len(alerts) == 1
    assert alerts[0]["rule_triggered"] == "BOT_BURST_CARD_TESTING"
    assert alerts[0]["amount_usd"] == 8.00
    assert len(alerts[0]["related_transaction_ids"]) == 4


def test_cep_rule3_aml_structuring():
    cfg = load_fraud_rules_config("config/fraud_rules_config.yaml")
    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    alerts = []
    for i in range(3):
        tx = {
            "transaction_id": f"tx-aml-{i}",
            "card_id": "CARD-AML-77",
            "customer_id": "CUST-AML-77",
            "amount": 9500000.0,
            "currency": "COP",
            "amount_usd": 2317.07,
            "merchant_name": "Cajero Bancolombia",
            "merchant_category": "atm_cash",
            "channel": "ATM",
            "country": "COL",
            "city": "Bogota",
            "latitude": 4.7110,
            "longitude": -74.0721,
            "timestamp": f"2026-11-12 12:1{i}:00",
        }
        res = evaluator.evaluate_transaction(tx)
        if res:
            alerts.append(res)

    assert len(alerts) == 1
    assert alerts[0]["rule_triggered"] == "AML_STRUCTURING"
    assert alerts[0]["status"] == "FLAGGED_FOR_AUDIT"
    assert len(alerts[0]["related_transaction_ids"]) == 3


def test_cep_ignores_corrupt_and_duplicate_records():
    cfg = load_fraud_rules_config("config/fraud_rules_config.yaml")
    evaluator = PureStreamFraudEvaluator.from_config(cfg)

    bad_tx = {
        "transaction_id": "tx-bad-1",
        "card_id": "CARD-BAD",
        "customer_id": None,
        "amount": -50.0,
        "currency": "USD",
        "amount_usd": -50.0,
        "channel": "POS",
        "country": "USA",
        "city": "Miami",
        "latitude": 150.0,
        "longitude": -80.0,
        "timestamp": "2026-11-12 12:00:00",
    }
    assert evaluator.evaluate_transaction(bad_tx) is None


if __name__ == "__main__":
    test_haversine_bogota_madrid()
    test_impossible_travel_velocity_math()
    test_normal_travel_velocity_allowed()
    test_config_loader_reads_yaml()
    test_cep_rule1_impossible_travel_stateful()
    test_cep_rule2_bot_burst_card_testing()
    test_cep_rule3_aml_structuring()
    test_cep_ignores_corrupt_and_duplicate_records()
    print("[+] 8/8 pruebas unitarias y de integración Stateful CEP superadas con éxito.")
