"""
Utilidades Geográficas y Cálculo de Velocidad (Fórmula del Semiverseno / Haversine)
Utilizado para la Regla 1: Detección de Viaje Imposible (Impossible Travel Velocity).
"""

import math
from datetime import datetime
from typing import Optional, Tuple


def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Calcula la distancia en kilómetros entre dos coordenadas geográficas
    sobre la esfera terrestre (Radio = 6371.0 km).
    """
    R = 6371.0  # Radio de la Tierra en km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return R * c


def calculate_travel_velocity_kmh(
    lat1: float,
    lon1: float,
    ts1: datetime,
    lat2: float,
    lon2: float,
    ts2: datetime,
) -> Tuple[float, float, float]:
    """
    Calcula la distancia (km), tiempo transcurrido (horas) y velocidad aparente (km/h)
    entre dos eventos sucesivos.

    :return: (distancia_km, delta_horas, velocidad_kmh)
    """
    distance_km = haversine_distance_km(lat1, lon1, lat2, lon2)
    delta_seconds = abs((ts2 - ts1).total_seconds())

    # Si ocurren simultáneamente (0 segundos) con distancia distinta de cero, velocidad infinita
    if delta_seconds <= 0:
        return distance_km, 0.0, float("inf") if distance_km > 0 else 0.0

    delta_hours = delta_seconds / 3600.0
    velocity_kmh = distance_km / delta_hours

    return distance_km, delta_hours, velocity_kmh
