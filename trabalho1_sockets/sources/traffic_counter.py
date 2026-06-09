"""
Contador de Trafego (TRAFFIC_COUNTER)
-------------------------------------
Fonte CONTROLAVEL: conta veiculos por minuto. Frequencia de envio configuravel.
Dispara alerta quando o fluxo ultrapassa o limiar (congestionamento).

Uso:
    python sources/traffic_counter.py [source_id] [frequencia_s] [limiar_veic]
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smartcity_pb2 as pb
from base_source import BaseSource


class TrafficCounter(BaseSource):
    SOURCE_TYPE = pb.TRAFFIC_COUNTER
    CONTROLLABLE = True
    DESCRIPTION = "Contador de trafego (veiculos/min)"
    DEFAULT_FREQUENCY = 8.0
    DEFAULT_THRESHOLD = 120.0         # limiar de congestionamento (veic/min)
    ALERT_METRIC = "veiculos_por_min"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._flow = 40.0

    def generate_readings(self):
        self._flow = max(0.0, min(200.0, self._flow + random.uniform(-15, 20)))
        return [("veiculos_por_min", round(self._flow, 0), "veic/min")]


if __name__ == "__main__":
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    freq = float(sys.argv[2]) if len(sys.argv) > 2 else None
    thr = float(sys.argv[3]) if len(sys.argv) > 3 else None
    TrafficCounter(source_id=sid, frequency=freq, threshold=thr).run()
