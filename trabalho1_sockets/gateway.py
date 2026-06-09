"""
gateway.py
----------
GATEWAY INTELIGENTE - ponto central do sistema.

Responsabilidades (todas as mensagens em Protocol Buffers):
  * DESCOBERTA (multicast UDP): emite DiscoveryRequest periodicamente no grupo
    multicast; as fontes respondem com SourceAnnounce.
  * DADOS (UDP): recebe UdpEnvelope das fontes -> announce (cadastro) ou
    data (leitura). Persiste leituras no SQLite.
  * CONTROLE (TCP): atende o Cliente Analitico (LIST_SOURCES, SEND_COMMAND,
    QUERY). Encaminha comandos para as fontes abrindo conexao TCP ate elas.
  * LIVENESS: marca como FAILED a fonte que ficar sem contato (timeout) ->
    detecta tanto a falha simulada quanto o desligamento do processo.

Uso:
    python gateway.py [--no-dashboard]
"""

import os
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import smartcity_pb2 as pb
import protocol
from storage import Storage
from google.protobuf.timestamp_pb2 import Timestamp


class SourceRecord:
    """Estado em memoria de uma fonte conhecida pelo gateway."""
    def __init__(self, ann):
        self.source_id = ann.source_id
        self.type = ann.type
        self.ip = ann.ip
        self.control_port = ann.control_port
        self.status = ann.status
        self.controllable = ann.controllable
        self.description = ann.description
        self.frequency = ann.frequency
        self.threshold = ann.threshold
        self.last_seen = time.time()
        self.readings_count = 0


class Gateway:
    def __init__(self, with_dashboard=True):
        self.ip = protocol.get_local_ip()
        self.tcp_port = protocol.GATEWAY_TCP_PORT
        self.udp_port = protocol.GATEWAY_UDP_PORT
        self.with_dashboard = with_dashboard

        self.sources = {}                 # source_id -> SourceRecord
        self.lock = threading.RLock()
        self.storage = Storage(os.path.join(os.path.dirname(__file__),
                                            "gateway_data.db"))
        self._running = True

    # ===================================================================
    # DESCOBERTA - emissor multicast
    # ===================================================================
    def _discovery_sender(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL,
                        protocol.MCAST_TTL)
        req = pb.DiscoveryRequest(
            gateway_ip=self.ip, gateway_tcp_port=self.tcp_port,
            gateway_udp_data_port=self.udp_port,
        )
        payload = req.SerializeToString()
        while self._running:
            try:
                sock.sendto(payload, (protocol.MCAST_GRP, protocol.MCAST_PORT))
            except OSError as e:
                print(f"[gateway] erro no multicast: {e}")
            time.sleep(protocol.DISCOVERY_INTERVAL)
        sock.close()

    # ===================================================================
    # DADOS - receptor UDP (announce + data na mesma porta, via UdpEnvelope)
    # ===================================================================
    def _udp_receiver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.udp_port))
        sock.settimeout(1.0)
        print(f"[gateway] recebendo dados/anuncios via UDP na porta {self.udp_port}")
        while self._running:
            try:
                data, addr = sock.recvfrom(8192)
            except socket.timeout:
                continue
            except OSError:
                break
            env = pb.UdpEnvelope()
            try:
                env.ParseFromString(data)
            except Exception:
                continue
            kind = env.WhichOneof("payload")
            if kind == "announce":
                self._on_announce(env.announce)
            elif kind == "data":
                self._on_data(env.data)
        sock.close()

    def _on_announce(self, ann):
        with self.lock:
            new = ann.source_id not in self.sources
            rec = SourceRecord(ann)
            if not new:
                # preserva contagem acumulada e status corrente se ja ativo
                old = self.sources[ann.source_id]
                rec.readings_count = old.readings_count
            self.sources[ann.source_id] = rec
        self.storage.upsert_source(ann.source_id, ann.type, ann.ip,
                                   ann.control_port, ann.controllable,
                                   ann.description, ann.status)
        if new:
            print(f"[gateway] >> fonte DESCOBERTA: {ann.source_id} "
                  f"({pb.SourceType.Name(ann.type)}) @ {ann.ip}:{ann.control_port} "
                  f"controlavel={ann.controllable}")

    def _on_data(self, data):
        ts = data.timestamp.seconds + data.timestamp.nanos / 1e9
        if ts <= 0:
            ts = time.time()
        with self.lock:
            rec = self.sources.get(data.source_id)
            if rec:
                rec.last_seen = time.time()
                rec.readings_count += 1
                if rec.status == pb.FAILED:
                    rec.status = pb.ACTIVE   # voltou a dar sinal de vida
        for r in data.readings:
            self.storage.insert_reading(data.source_id, data.type, r.name,
                                        r.value, r.unit, data.alert, ts)
        self.storage.touch_source(data.source_id, pb.ACTIVE)
        tag = "  [ALERTA] " + data.alert_msg if data.alert else ""
        vals = ", ".join(f"{r.name}={r.value:g}{r.unit}" for r in data.readings)
        print(f"[gateway] dado de {data.source_id}: {vals}{tag}")

    # ===================================================================
    # LIVENESS - detecta falha/queda de fontes
    # ===================================================================
    def _liveness_checker(self):
        while self._running:
            time.sleep(protocol.LIVENESS_CHECK)
            now = time.time()
            with self.lock:
                for rec in self.sources.values():
                    if rec.status in (pb.ACTIVE,) and \
                       now - rec.last_seen > protocol.SOURCE_TIMEOUT:
                        rec.status = pb.FAILED
                        self.storage.set_status(rec.source_id, pb.FAILED)
                        print(f"[gateway] !! fonte SEM CONTATO (timeout): "
                              f"{rec.source_id} marcada como FAILED")

    # ===================================================================
    # CONTROLE - servidor TCP para o Cliente Analitico
    # ===================================================================
    def _tcp_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.tcp_port))
        sock.listen(8)
        sock.settimeout(1.0)
        print(f"[gateway] atendendo o cliente via TCP na porta {self.tcp_port}")
        while self._running:
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            print(f"[gateway] cliente conectado: {addr}")
            threading.Thread(target=self._handle_client, args=(conn, addr),
                             daemon=True).start()
        sock.close()

    def _handle_client(self, conn, addr):
        try:
            while self._running:
                req = protocol.recv_msg(conn, pb.ClientRequest())
                if req is None:
                    break
                resp = self._dispatch_request(req)
                protocol.send_msg(conn, resp)
        except OSError:
            pass
        finally:
            conn.close()
            print(f"[gateway] cliente desconectado: {addr}")

    def _dispatch_request(self, req):
        if req.type == pb.LIST_SOURCES:
            return self._handle_list()
        if req.type == pb.SEND_COMMAND:
            return self._handle_send_command(req.source_id, req.command)
        if req.type == pb.QUERY:
            return self._handle_query(req.query)
        return pb.ClientResponse(success=False, message="requisicao desconhecida")

    # ---- LIST_SOURCES ----
    def _handle_list(self):
        resp = pb.ClientResponse(success=True, message="fontes conectadas")
        with self.lock:
            for rec in sorted(self.sources.values(), key=lambda r: r.source_id):
                info = resp.sources.add()
                info.source_id = rec.source_id
                info.type = rec.type
                info.ip = rec.ip
                info.control_port = rec.control_port
                info.status = rec.status
                info.controllable = rec.controllable
                info.description = rec.description
                info.frequency = rec.frequency
                info.threshold = rec.threshold
                info.readings_count = rec.readings_count
                ts = Timestamp(); ts.FromSeconds(int(rec.last_seen))
                info.last_seen.CopyFrom(ts)
        return resp

    # ---- SEND_COMMAND (gateway -> fonte via TCP) ----
    def _handle_send_command(self, source_id, command):
        with self.lock:
            rec = self.sources.get(source_id)
            if not rec:
                return pb.ClientResponse(success=False,
                                         message=f"fonte '{source_id}' nao encontrada")
            ip, port, controllable = rec.ip, rec.control_port, rec.controllable
        if not controllable and command.type in (pb.SET_FREQUENCY, pb.SET_THRESHOLD):
            return pb.ClientResponse(success=False,
                                     message=f"fonte '{source_id}' nao e controlavel")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)
            s.connect((ip, port))
            protocol.send_msg(s, command)
            result = protocol.recv_msg(s, pb.CommandResult())
            s.close()
        except OSError as e:
            return pb.ClientResponse(success=False,
                                     message=f"falha ao contatar a fonte: {e}")
        if result is None:
            return pb.ClientResponse(success=False, message="sem resposta da fonte")
        # atualiza o estado local com o retorno da fonte
        with self.lock:
            rec = self.sources.get(source_id)
            if rec:
                rec.status = result.status
                rec.frequency = result.frequency
                rec.threshold = result.threshold
        self.storage.set_status(source_id, result.status)
        resp = pb.ClientResponse(success=result.success, message=result.message)
        resp.command_result.CopyFrom(result)
        return resp

    # ---- QUERY (consultas analiticas agregadas) ----
    def _handle_query(self, q):
        resp = pb.ClientResponse(success=True)
        ws = q.window_seconds
        m = q.metric
        sid = q.source_id or None

        if q.type in (pb.AVG, pb.STDDEV, pb.MIN, pb.MAX, pb.COUNT):
            func = {pb.AVG: "avg", pb.STDDEV: "stddev", pb.MIN: "min",
                    pb.MAX: "max", pb.COUNT: "count"}[q.type]
            val, n = self.storage.aggregate(func, m, sid, ws)
            if val is None and func != "count":
                resp.message = f"sem dados para '{m}' na janela informada"
            else:
                row = resp.rows.add()
                row.label = f"{func.upper()} de {m}"
                row.value = float(val if val is not None else 0)
                row.extra = f"n={n}" + (f" janela={ws}s" if ws else "")
                resp.message = "ok"
        elif q.type == pb.MAX_VARIATION:
            results = self.storage.max_variation(m, ws)
            if not results:
                resp.message = f"sem dados para '{m}'"
            else:
                resp.message = f"fontes ordenadas por variacao (desvio padrao) de {m}"
                for sid_, std, n in results:
                    row = resp.rows.add()
                    row.label = sid_
                    row.value = std
                    row.extra = f"n={n}"
        elif q.type == pb.HISTORY:
            rows = self.storage.history(m, sid, ws, limit=50)
            resp.message = f"historico ({len(rows)} leituras)"
            for r in rows:
                row = resp.rows.add()
                row.label = f"{r['source_id']}/{r['metric']}"
                row.value = r["value"]
                row.extra = time.strftime("%H:%M:%S", time.localtime(r["ts"]))
        elif q.type == pb.LATEST:
            rows = self.storage.latest_per_source()
            resp.message = "ultima leitura por fonte"
            for r in rows:
                row = resp.rows.add()
                row.label = f"{r['source_id']}/{r['metric']}"
                row.value = r["value"]
                row.extra = (r.get("unit") or "") + " @ " + \
                    time.strftime("%H:%M:%S", time.localtime(r["ts"]))
        else:
            resp.success = False
            resp.message = "tipo de consulta desconhecido"
        return resp

    # ===================================================================
    # ciclo de vida
    # ===================================================================
    def run(self):
        print("=" * 64)
        print(f" GATEWAY INTELIGENTE  ip={self.ip}")
        print(f"   TCP (cliente)={self.tcp_port}  UDP (dados)={self.udp_port}")
        print(f"   multicast={protocol.MCAST_GRP}:{protocol.MCAST_PORT}")
        print("=" * 64)
        threads = [
            threading.Thread(target=self._discovery_sender, daemon=True),
            threading.Thread(target=self._udp_receiver, daemon=True),
            threading.Thread(target=self._tcp_server, daemon=True),
            threading.Thread(target=self._liveness_checker, daemon=True),
        ]
        if self.with_dashboard:
            try:
                from dashboard import start_dashboard
                threads.append(threading.Thread(
                    target=start_dashboard,
                    args=(self, protocol.DASHBOARD_PORT), daemon=True))
                print(f"[gateway] dashboard web em http://localhost:"
                      f"{protocol.DASHBOARD_PORT}")
            except Exception as e:
                print(f"[gateway] dashboard indisponivel: {e}")
        for t in threads:
            t.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[gateway] encerrando...")
            self._running = False
            time.sleep(0.3)
            self.storage.close()


if __name__ == "__main__":
    with_dash = "--no-dashboard" not in sys.argv
    Gateway(with_dashboard=with_dash).run()
