#!/usr/bin/env bash
#
# run_all.sh - sobe o sistema inteiro num unico terminal (para teste rapido).
#
# Sobe: gateway (com dashboard) + 5 fontes (4 Python + 1 Node.js).
# Pressione Ctrl+C para encerrar TODOS os processos de uma vez.
#
# Para a apresentacao/video, prefira abrir cada processo em um terminal
# separado (veja o README) para visualizar os logs individualmente.
#
set -euo pipefail
cd "$(dirname "$0")/.."

PIDS=()
cleanup() {
  echo
  echo "[run_all] encerrando..."
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

echo "[run_all] iniciando gateway (dashboard em http://localhost:8080)..."
python gateway.py &
PIDS+=($!)
sleep 2

echo "[run_all] iniciando fontes Python..."
python sources/weather_station.py wx-1 15 38   & PIDS+=($!)
python sources/air_quality.py     air-1 10 800  & PIDS+=($!)
python sources/traffic_counter.py traf-1 8      & PIDS+=($!)
python sources/noise_sensor.py    noise-1 12    & PIDS+=($!)

echo "[run_all] iniciando fonte Node.js (medidor de energia)..."
( cd node_source && node energy_meter.js energy-1 10 ) & PIDS+=($!)

echo
echo "[run_all] tudo no ar. Em outro terminal rode:  python client.py"
echo "[run_all] dashboard web:                        http://localhost:8080"
echo "[run_all] Ctrl+C encerra tudo."
wait
