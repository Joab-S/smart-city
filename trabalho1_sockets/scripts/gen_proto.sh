#!/usr/bin/env bash
#
# gen_proto.sh - regenera smartcity_pb2.py a partir de smartcity.proto.
# Necessario apenas se voce alterar o .proto. O pacote ja vem com o
# arquivo gerado.
#
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc -I. --python_out=. smartcity.proto
echo "[gen_proto] smartcity_pb2.py regenerado."
