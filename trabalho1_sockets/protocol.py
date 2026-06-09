"""
protocol.py
-----------
Modulo comum a Gateway, Fontes e Cliente.

Contem:
  * Constantes de rede (grupo multicast, portas padrao, timeouts);
  * Funcoes de "framing" para TCP (TCP e um fluxo de bytes, entao cada
    mensagem Protocol Buffers e prefixada por seu tamanho de 4 bytes);
  * Deteccao do IP local da maquina.

Decisao de projeto: como o TCP nao preserva fronteiras de mensagem, usamos
o esquema classico length-prefix => [4 bytes big-endian = N][N bytes protobuf].
No UDP cada datagrama ja e uma mensagem completa, entao nao ha framing.
"""

import socket
import struct

# --- Grupo/porta multicast usados na DESCOBERTA (mesmos do exemplo da disciplina) ---
MCAST_GRP = "228.0.0.8"
MCAST_PORT = 6789
MCAST_TTL = 2

# --- Portas padrao do Gateway ---
GATEWAY_TCP_PORT = 5000   # atende o Cliente Analitico (controle)
GATEWAY_UDP_PORT = 6000   # recebe anuncios e fluxo de dados das fontes
DASHBOARD_PORT = 8080     # dashboard web (extra)

# --- Tempos (segundos) ---
DISCOVERY_INTERVAL = 8    # de quanto em quanto o gateway re-emite a descoberta
SOURCE_TIMEOUT = 25       # sem contato por mais que isso => fonte marcada FAILED
LIVENESS_CHECK = 5        # periodo da verificacao de liveness no gateway


# ---------------------------------------------------------------------------
# Framing TCP (length-prefixed)
# ---------------------------------------------------------------------------
def send_msg(sock, message):
    """Serializa um protobuf e envia prefixado pelo tamanho (4 bytes BE)."""
    data = message.SerializeToString()
    sock.sendall(struct.pack(">I", len(data)) + data)


def _recv_exactly(sock, n):
    """Le exatamente n bytes do socket; retorna None se a conexao fechar."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock, message):
    """Le uma mensagem prefixada e faz parse no objeto protobuf `message`.

    Retorna o proprio `message` em sucesso, ou None se a conexao fechou.
    """
    header = _recv_exactly(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack(">I", header)
    payload = _recv_exactly(sock, length)
    if payload is None:
        return None
    message.ParseFromString(payload)
    return message


# ---------------------------------------------------------------------------
# Descoberta do IP local (para anunciar endereco real na rede)
# ---------------------------------------------------------------------------
def get_local_ip():
    """Descobre o IP da interface usada para sair na rede.

    Truque: "conecta" um socket UDP a um destino externo (sem enviar nada)
    e le o endereco local escolhido pelo SO. Cai para 127.0.0.1 se offline.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip
