"""
base_source.py
--------------
Logica comum a todas as Fontes de Dados (sensores) escritas em Python.

Cada fonte e um PROCESSO separado que executa 3 threads:
  1. discovery_listener : escuta o grupo multicast; ao receber DiscoveryRequest
     do Gateway, responde (unicast UDP) com um SourceAnnounce e passa a conhecer
     o endereco UDP de dados do Gateway.
  2. control_server     : servidor TCP que recebe Command do Gateway
     (frequencia, limiar, ativar/desativar, simular falha) e devolve CommandResult.
  3. data_loop          : periodicamente (a cada `frequency` s), se ACTIVE, gera
     leituras e as envia ao Gateway via UDP. Tambem dispara envio IMEDIATO quando
     uma leitura cruza o limiar de alerta (evento relevante).

As subclasses implementam apenas `generate_readings()` e definem os metadados.
"""

import os
import socket
import struct
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smartcity_pb2 as pb               # noqa: E402
import protocol                          # noqa: E402
from google.protobuf.timestamp_pb2 import Timestamp  # noqa: E402


class BaseSource:
    # --- atributos definidos pelas subclasses ---
    SOURCE_TYPE = pb.SOURCE_UNKNOWN
    CONTROLLABLE = False
    DESCRIPTION = "fonte generica"
    DEFAULT_FREQUENCY = 15.0     # segundos entre envios
    DEFAULT_THRESHOLD = 0.0      # limiar de alerta (metrica monitorada)
    ALERT_METRIC = None          # nome da metrica monitorada para alerta

    def __init__(self, source_id=None, frequency=None, threshold=None):
        type_name = pb.SourceType.Name(self.SOURCE_TYPE).lower()
        self.source_id = source_id or f"{type_name}-{uuid.uuid4().hex[:6]}"
        self.frequency = float(frequency) if frequency else self.DEFAULT_FREQUENCY
        self.threshold = float(threshold) if threshold is not None else self.DEFAULT_THRESHOLD
        self.status = pb.ACTIVE
        self.ip = protocol.get_local_ip()

        # endereco UDP de dados do gateway (preenchido na descoberta)
        self._gateway_data_addr = None
        self._lock = threading.Lock()
        self._running = True

        # socket UDP usado para anunciar e enviar dados
        self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # servidor TCP de comandos (porta escolhida pelo SO)
        self._tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._tcp.bind(("", 0))
        self._tcp.listen(5)
        self.control_port = self._tcp.getsockname()[1]

    # ----------------------------------------------------------------- API
    def generate_readings(self):
        """Subclasse retorna lista de (name, value, unit)."""
        raise NotImplementedError

    # ------------------------------------------------------- descoberta
    def _discovery_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.bind(("", protocol.MCAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(protocol.MCAST_GRP),
                           socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(1.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            req = pb.DiscoveryRequest()
            try:
                req.ParseFromString(data)
            except Exception:
                continue
            # aprende onde mandar os dados e responde com o anuncio
            with self._lock:
                self._gateway_data_addr = (req.gateway_ip,
                                           req.gateway_udp_data_port)
            self._send_announce()
            print(f"[{self.source_id}] descoberta recebida do gateway "
                  f"{req.gateway_ip}:{req.gateway_udp_data_port} -> anunciado")
        sock.close()

    def _send_announce(self):
        with self._lock:
            addr = self._gateway_data_addr
            status = self.status
            freq = self.frequency
            thr = self.threshold
        if not addr:
            return
        ann = pb.SourceAnnounce(
            source_id=self.source_id, type=self.SOURCE_TYPE, ip=self.ip,
            control_port=self.control_port, status=status,
            controllable=self.CONTROLLABLE, description=self.DESCRIPTION,
            frequency=freq, threshold=thr,
        )
        env = pb.UdpEnvelope(announce=ann)
        try:
            self._udp.sendto(env.SerializeToString(), addr)
        except OSError:
            pass

    # ------------------------------------------------------- controle TCP
    def _control_server(self):
        self._tcp.settimeout(1.0)
        while self._running:
            try:
                conn, addr = self._tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_command, args=(conn,),
                             daemon=True).start()

    def _handle_command(self, conn):
        try:
            cmd = protocol.recv_msg(conn, pb.Command())
            if cmd is None:
                return
            result = self._apply_command(cmd)
            protocol.send_msg(conn, result)
        except OSError:
            pass
        finally:
            conn.close()

    def _apply_command(self, cmd):
        msg = ""
        with self._lock:
            if cmd.type == pb.SET_FREQUENCY:
                self.frequency = max(0.5, cmd.value)
                msg = f"frequencia ajustada para {self.frequency:g}s"
            elif cmd.type == pb.SET_THRESHOLD:
                self.threshold = cmd.value
                msg = f"limiar de alerta ajustado para {self.threshold:g}"
            elif cmd.type == pb.ACTIVATE:
                self.status = pb.ACTIVE
                msg = "fonte ativada"
            elif cmd.type == pb.DEACTIVATE:
                self.status = pb.INACTIVE
                msg = "fonte desativada"
            elif cmd.type == pb.SIMULATE_FAILURE:
                self.status = pb.FAILED
                msg = "falha simulada: a fonte parou de enviar dados"
            elif cmd.type == pb.GET_STATUS:
                msg = "estado atual"
            else:
                return pb.CommandResult(success=False, message="comando desconhecido",
                                        status=self.status, frequency=self.frequency,
                                        threshold=self.threshold)
            status, freq, thr = self.status, self.frequency, self.threshold
        print(f"[{self.source_id}] comando {pb.CommandType.Name(cmd.type)} -> {msg}")
        return pb.CommandResult(success=True, message=msg, status=status,
                                frequency=freq, threshold=thr)

    # ------------------------------------------------------- envio de dados
    def _build_sensor_data(self, readings, alert=False, alert_msg=""):
        ts = Timestamp(); ts.GetCurrentTime()
        data = pb.SensorData(source_id=self.source_id, type=self.SOURCE_TYPE,
                             timestamp=ts, alert=alert, alert_msg=alert_msg)
        for name, value, unit in readings:
            data.readings.add(name=name, value=value, unit=unit)
        return data

    def _send_data(self, data):
        with self._lock:
            addr = self._gateway_data_addr
        if not addr:
            return
        env = pb.UdpEnvelope(data=data)
        try:
            self._udp.sendto(env.SerializeToString(), addr)
        except OSError:
            pass

    def _data_loop(self):
        while self._running:
            with self._lock:
                status = self.status
                freq = self.frequency
                thr = self.threshold
                has_gw = self._gateway_data_addr is not None
            if status == pb.ACTIVE and has_gw:
                readings = self.generate_readings()
                # checa limiar -> alerta imediato
                alert, alert_msg = False, ""
                if self.ALERT_METRIC:
                    for name, value, _ in readings:
                        if name == self.ALERT_METRIC and thr and value >= thr:
                            alert = True
                            alert_msg = (f"{name}={value:g} ultrapassou o "
                                         f"limiar {thr:g}")
                            break
                data = self._build_sensor_data(readings, alert, alert_msg)
                self._send_data(data)
                tag = " [ALERTA]" if alert else ""
                vals = ", ".join(f"{n}={v:g}{u}" for n, v, u in readings)
                print(f"[{self.source_id}] enviou: {vals}{tag}")
            # dorme em fatias para reagir rapido a mudancas de frequencia/parada
            slept = 0.0
            step = 0.25
            while self._running and slept < freq:
                time.sleep(step)
                slept += step
                with self._lock:
                    freq = self.frequency  # re-le caso tenha mudado

    # ------------------------------------------------------- ciclo de vida
    def run(self):
        print(f"=== Fonte '{self.source_id}' ({self.DESCRIPTION}) ===")
        print(f"    tipo={pb.SourceType.Name(self.SOURCE_TYPE)} "
              f"controlavel={self.CONTROLLABLE} "
              f"porta_controle_TCP={self.control_port}")
        print(f"    frequencia={self.frequency:g}s limiar={self.threshold:g}")
        print("    aguardando descoberta do gateway (multicast)...")
        threads = [
            threading.Thread(target=self._discovery_listener, daemon=True),
            threading.Thread(target=self._control_server, daemon=True),
            threading.Thread(target=self._data_loop, daemon=True),
        ]
        for t in threads:
            t.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.source_id}] encerrando...")
            self._running = False
            time.sleep(0.3)
