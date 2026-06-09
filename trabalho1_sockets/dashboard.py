"""
dashboard.py
------------
Dashboard web do Gateway (PONTUACAO EXTRA: interface grafica + series temporais).

Sobe um servidor HTTP simples (somente biblioteca padrao) que expoe:
  GET /                       -> pagina HTML (web/index.html)
  GET /api/sources            -> fontes conhecidas e estado (registro em memoria)
  GET /api/metrics            -> metricas disponiveis (do SQLite)
  GET /api/timeseries?metric=&window=  -> series temporais por fonte (do SQLite)

A pagina usa Chart.js (CDN) para plotar as series temporais das leituras.
Os dados de historico vem da persistencia em SQLite (storage.py).
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import smartcity_pb2 as pb

STATUS_TXT = {pb.STATUS_UNKNOWN: "?", pb.ACTIVE: "ATIVA",
              pb.INACTIVE: "INATIVA", pb.FAILED: "FALHA"}

_WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")


def _make_handler(gateway):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass  # silencia o log do http.server

        def _json(self, obj, code=200):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _file(self, path, ctype):
            try:
                with open(path, "rb") as f:
                    body = f.read()
            except OSError:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            qs = parse_qs(url.query)

            if url.path in ("/", "/index.html"):
                return self._file(os.path.join(_WEB_DIR, "index.html"),
                                  "text/html; charset=utf-8")

            if url.path == "/api/sources":
                out = []
                with gateway.lock:
                    for rec in sorted(gateway.sources.values(),
                                      key=lambda r: r.source_id):
                        out.append({
                            "source_id": rec.source_id,
                            "type": pb.SourceType.Name(rec.type),
                            "ip": rec.ip,
                            "control_port": rec.control_port,
                            "status": STATUS_TXT.get(rec.status, "?"),
                            "controllable": rec.controllable,
                            "description": rec.description,
                            "frequency": rec.frequency,
                            "threshold": rec.threshold,
                            "readings_count": rec.readings_count,
                            "last_seen": rec.last_seen,
                            "age": round(time.time() - rec.last_seen, 1),
                        })
                return self._json(out)

            if url.path == "/api/metrics":
                return self._json(gateway.storage.metrics_list())

            if url.path == "/api/timeseries":
                metric = qs.get("metric", [""])[0]
                window = int(qs.get("window", ["1800"])[0])
                if not metric:
                    return self._json({})
                series = gateway.storage.timeseries(metric, window)
                return self._json(series)

            self.send_error(404)

    return Handler


def start_dashboard(gateway, port):
    server = ThreadingHTTPServer(("", port), _make_handler(gateway))
    server.serve_forever()
