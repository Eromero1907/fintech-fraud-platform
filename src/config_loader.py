"""
Cargador Centralizado de Configuración y Variables de Entorno.
Lee `config/fraud_rules_config.yaml` y `.env` sin requerir dependencias externas obligatorias,
garantizando portabilidad tanto en contenedores locales como en nodos de Amazon EMR / Glue.
"""

import os
from typing import Any, Dict


def load_env_file(env_path: str = ".env") -> None:
    """Carga variables de un archivo .env en os.environ si no están definidas."""
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


def _parse_scalar(val_str: str) -> Any:
    """Convierte un escalar YAML simple a bool, int, float o str."""
    if "#" in val_str:
        val_str = val_str.split("#", 1)[0].strip()
    val_str = val_str.strip().strip('"').strip("'")
    if val_str.lower() == "true":
        return True
    if val_str.lower() == "false":
        return False
    try:
        if "." in val_str:
            return float(val_str)
        return int(val_str)
    except ValueError:
        return val_str


def load_fraud_rules_config(config_path: str = "config/fraud_rules_config.yaml") -> Dict[str, Any]:
    """
    Carga la configuración jerárquica desde `config/fraud_rules_config.yaml`.
    Usa PyYAML si está instalado o un parser jerárquico nativo por indentación.
    """
    load_env_file()

    # Intentar localizar el archivo desde la raíz del proyecto
    if not os.path.exists(config_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        alt_path = os.path.join(base_dir, config_path)
        if os.path.exists(alt_path):
            config_path = alt_path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")

    try:
        import yaml  # type: ignore
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except ImportError:
        cfg = {}
        stack = [(-1, cfg)]
        with open(config_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                indent = len(raw_line) - len(raw_line.lstrip(" "))
                if ":" not in stripped:
                    continue
                key, rest = stripped.split(":", 1)
                key = key.strip()
                rest = rest.strip()
                if rest.startswith("#"):
                    rest = ""

                while len(stack) > 1 and stack[-1][0] >= indent:
                    stack.pop()

                current_dict = stack[-1][1]
                if not rest:
                    new_dict: Dict[str, Any] = {}
                    current_dict[key] = new_dict
                    stack.append((indent, new_dict))
                else:
                    current_dict[key] = _parse_scalar(rest)

    # Sobrescribir con variables de entorno si están presentes
    if "kafka" in cfg:
        cfg["kafka"]["bootstrap_servers"] = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"].get("bootstrap_servers", "localhost:9092")
        )
        cfg["kafka"]["transactions_topic"] = os.getenv(
            "KAFKA_TRANSACTIONS_TOPIC", cfg["kafka"].get("transactions_topic", "financial-transactions")
        )
        cfg["kafka"]["alerts_topic"] = os.getenv(
            "KAFKA_ALERTS_TOPIC", cfg["kafka"].get("alerts_topic", "fraud-alerts")
        )

    if "s3_medallion" in cfg:
        cfg["s3_medallion"]["bucket"] = os.getenv(
            "S3_BUCKET_NAME", cfg["s3_medallion"].get("bucket", "book-store-datalake-182832138582")
        )
        cfg["s3_medallion"]["prefix"] = os.getenv(
            "S3_PROJECT_PREFIX", cfg["s3_medallion"].get("prefix", "fintech_project")
        )

    return cfg
