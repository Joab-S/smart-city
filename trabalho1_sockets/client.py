"""
client.py
---------
CLIENTE ANALITICO - processo separado que conversa com o Gateway via TCP
(mensagens Protocol Buffers, framing length-prefixed).

Permite:
  1. Listar as fontes conectadas e seus estados;
  2. Enviar comandos de controle a uma fonte (ativar/desativar, mudar
     frequencia, ajustar limiar de alerta, simular falha, consultar estado);
  3. Executar consultas analiticas agregadas (media, desvio padrao, min, max,
     contagem, fonte de maior variacao, historico, ultima leitura).

Uso:
    python client.py [host] [porta]      (padrao: localhost 5000)
"""

import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import smartcity_pb2 as pb
import protocol


STATUS_TXT = {pb.STATUS_UNKNOWN: "?", pb.ACTIVE: "ATIVA",
              pb.INACTIVE: "INATIVA", pb.FAILED: "FALHA"}


class AnalyticClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def _request(self, req):
        protocol.send_msg(self.sock, req)
        return protocol.recv_msg(self.sock, pb.ClientResponse())

    # ----------------------------------------------------- acoes
    def list_sources(self):
        resp = self._request(pb.ClientRequest(type=pb.LIST_SOURCES))
        if not resp or not resp.sources:
            print("  (nenhuma fonte conectada ainda)")
            return []
        print("\n  ID                       TIPO              ESTADO   CTRL  "
              "FREQ   LIMIAR  LEITURAS")
        print("  " + "-" * 78)
        ids = []
        for s in resp.sources:
            ids.append(s.source_id)
            print(f"  {s.source_id:<24} {pb.SourceType.Name(s.type):<16} "
                  f"{STATUS_TXT.get(s.status,'?'):<8} "
                  f"{'sim' if s.controllable else 'nao':<5} "
                  f"{s.frequency:<5.0f} {s.threshold:<7.0f} {s.readings_count}")
        return ids

    def send_command(self, source_id, cmd_type, value=0.0):
        req = pb.ClientRequest(type=pb.SEND_COMMAND, source_id=source_id)
        req.command.type = cmd_type
        req.command.value = value
        resp = self._request(req)
        if resp is None:
            print("  sem resposta do gateway")
            return
        print(f"  -> {resp.message}")
        if resp.success and resp.HasField("command_result"):
            cr = resp.command_result
            print(f"     estado={STATUS_TXT.get(cr.status,'?')} "
                  f"freq={cr.frequency:g}s limiar={cr.threshold:g}")

    def query(self, qtype, metric="", source_id="", window=0):
        req = pb.ClientRequest(type=pb.QUERY)
        req.query.type = qtype
        req.query.metric = metric
        req.query.source_id = source_id
        req.query.window_seconds = window
        resp = self._request(req)
        if resp is None:
            print("  sem resposta do gateway")
            return
        print(f"  {resp.message}")
        for row in resp.rows:
            extra = f"  ({row.extra})" if row.extra else ""
            print(f"    {row.label:<30} {row.value:>12.3f}{extra}")
        if not resp.rows:
            print("    (sem resultados)")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ----------------------------------------------------- menu interativo
def menu(cli):
    while True:
        print("\n" + "=" * 50)
        print(" CLIENTE ANALITICO - Cidade Inteligente")
        print("=" * 50)
        print(" 1) Listar fontes e estados")
        print(" 2) Enviar comando a uma fonte")
        print(" 3) Consulta analitica agregada")
        print(" 0) Sair")
        op = input(" opcao> ").strip()

        if op == "1":
            cli.list_sources()

        elif op == "2":
            ids = cli.list_sources()
            if not ids:
                continue
            sid = input("\n  source_id alvo> ").strip()
            print("   a) Ativar   b) Desativar   c) Mudar frequencia (s)")
            print("   d) Ajustar limiar de alerta   e) Simular falha   f) Estado")
            c = input("   comando> ").strip().lower()
            if c == "a":
                cli.send_command(sid, pb.ACTIVATE)
            elif c == "b":
                cli.send_command(sid, pb.DEACTIVATE)
            elif c == "c":
                v = float(input("   nova frequencia (s)> "))
                cli.send_command(sid, pb.SET_FREQUENCY, v)
            elif c == "d":
                v = float(input("   novo limiar> "))
                cli.send_command(sid, pb.SET_THRESHOLD, v)
            elif c == "e":
                cli.send_command(sid, pb.SIMULATE_FAILURE)
            elif c == "f":
                cli.send_command(sid, pb.GET_STATUS)
            else:
                print("  comando invalido")

        elif op == "3":
            print("   a) Media        b) Desvio padrao   c) Minimo")
            print("   d) Maximo       e) Contagem        f) Fonte de maior variacao")
            print("   g) Historico    h) Ultima leitura por fonte")
            c = input("   consulta> ").strip().lower()
            simple = {"a": pb.AVG, "b": pb.STDDEV, "c": pb.MIN,
                      "d": pb.MAX, "e": pb.COUNT}
            if c in simple:
                metric = input("   metrica (ex: temperatura, co2, ruido)> ").strip()
                w = input("   janela em segundos (vazio=tudo; 3600=1h)> ").strip()
                sid = input("   filtrar por source_id (vazio=todas)> ").strip()
                cli.query(simple[c], metric, sid, int(w) if w else 0)
            elif c == "f":
                metric = input("   metrica> ").strip()
                w = input("   janela em segundos (vazio=tudo)> ").strip()
                cli.query(pb.MAX_VARIATION, metric, "", int(w) if w else 0)
            elif c == "g":
                metric = input("   metrica (vazio=todas)> ").strip()
                w = input("   janela em segundos (vazio=tudo)> ").strip()
                cli.query(pb.HISTORY, metric, "", int(w) if w else 0)
            elif c == "h":
                cli.query(pb.LATEST)
            else:
                print("  consulta invalida")

        elif op == "0":
            print("  ate logo!")
            cli.close()
            return
        else:
            print("  opcao invalida")


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else protocol.GATEWAY_TCP_PORT
    print(f"conectando ao gateway {host}:{port} ...")
    try:
        cli = AnalyticClient(host, port)
    except OSError as e:
        print(f"nao foi possivel conectar: {e}")
        sys.exit(1)
    print("conectado!")
    try:
        menu(cli)
    except (KeyboardInterrupt, EOFError):
        print("\nencerrando cliente.")
        cli.close()
