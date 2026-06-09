"""
Sensor de Qualidade do Ar (AIR_QUALITY)
---------------------------------------
Fonte CONTROLAVEL com LIMIAR DE ALERTA ajustavel. Mede CO2 (ppm) e material
particulado (ug/m3). Quando o CO2 ultrapassa o limiar, dispara um envio
IMEDIATO marcado como alerta (evento relevante), alem dos envios periodicos.

O cliente pode ajustar o limiar de alerta, a frequencia, ativar/desativar
e simular falha.

Uso:
    python sources/air_quality.py [source_id] [frequencia_s] [limiar_co2]
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smartcity_pb2 as pb
from base_source import BaseSource


class AirQuality(BaseSource):
    SOURCE_TYPE = pb.AIR_QUALITY
    CONTROLLABLE = True
    DESCRIPTION = "Sensor de qualidade do ar (CO2 + material particulado)"
    DEFAULT_FREQUENCY = 10.0
    DEFAULT_THRESHOLD = 1000.0        # limiar de CO2 (ppm)
    ALERT_METRIC = "co2"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._co2 = 600.0
        self._pm = 25.0

    def generate_readings(self):
        self._co2 = max(400.0, min(2500.0, self._co2 + random.uniform(-60, 90)))
        self._pm = max(5.0, min(180.0, self._pm + random.uniform(-8, 10)))
        return [
            ("co2", round(self._co2, 1), "ppm"),
            ("material_particulado", round(self._pm, 1), "ug/m3"),
        ]


if __name__ == "__main__":
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    freq = float(sys.argv[2]) if len(sys.argv) > 2 else None
    thr = float(sys.argv[3]) if len(sys.argv) > 3 else None
    AirQuality(source_id=sid, frequency=freq, threshold=thr).run()
