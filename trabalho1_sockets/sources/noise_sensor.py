"""
Sensor de Ruido (NOISE_SENSOR)
------------------------------
Fonte CONTINUA / NAO CONTROLAVEL (sensor continuo puro): envia o nivel de
ruido em dB periodicamente via UDP. Demonstra o caso de fonte que apenas
publica leituras e participa da descoberta, sem aceitar comandos de
reconfiguracao (ainda assim responde a GET_STATUS).

Uso:
    python sources/noise_sensor.py [source_id] [frequencia_s]
"""

import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smartcity_pb2 as pb
from base_source import BaseSource


class NoiseSensor(BaseSource):
    SOURCE_TYPE = pb.NOISE_SENSOR
    CONTROLLABLE = False
    DESCRIPTION = "Sensor de ruido (dB) - sensor continuo"
    DEFAULT_FREQUENCY = 12.0
    ALERT_METRIC = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db = 55.0

    def generate_readings(self):
        self._db = max(30.0, min(110.0, self._db + random.uniform(-6, 6)))
        return [("ruido", round(self._db, 1), "dB")]


if __name__ == "__main__":
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    freq = float(sys.argv[2]) if len(sys.argv) > 2 else None
    NoiseSensor(source_id=sid, frequency=freq).run()
