"""
Estacao Meteorologica (WEATHER_STATION)
---------------------------------------
Fonte CONTINUA e CONTROLAVEL: envia temperatura + umidade a cada `frequency`
segundos. O cliente pode alterar a frequencia (ex.: de 15s para 5s),
ativar/desativar e simular falha.

Uso:
    python sources/weather_station.py [source_id] [frequencia_s]
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smartcity_pb2 as pb
from base_source import BaseSource


class WeatherStation(BaseSource):
    SOURCE_TYPE = pb.WEATHER_STATION
    CONTROLLABLE = True
    DESCRIPTION = "Estacao meteorologica (temperatura + umidade)"
    DEFAULT_FREQUENCY = 15.0
    DEFAULT_THRESHOLD = 38.0          # alerta de temperatura alta (C)
    ALERT_METRIC = "temperatura"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._temp = 26.0
        self._hum = 60.0

    def generate_readings(self):
        # passeio aleatorio suave para simular medicoes realistas
        self._temp = max(10.0, min(45.0, self._temp + random.uniform(-1.5, 1.8)))
        self._hum = max(20.0, min(100.0, self._hum + random.uniform(-3, 3)))
        return [
            ("temperatura", round(self._temp, 2), "C"),
            ("umidade", round(self._hum, 2), "%"),
        ]


if __name__ == "__main__":
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    freq = float(sys.argv[2]) if len(sys.argv) > 2 else None
    WeatherStation(source_id=sid, frequency=freq).run()
