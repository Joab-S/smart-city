# Trabalho 1 — Sockets: Cidade Inteligente

Sistema distribuído que simula uma **cidade inteligente**, com um **Gateway**
central, **fontes de dados** (sensores) distribuídas e um **Cliente Analítico**
para monitoramento, controle e consultas agregadas.

> Disciplina: Distribuição de Processos e Dados — Prof. Dr. Paulo A. L. Rego (UFC)

Toda a comunicação usa **Protocol Buffers**. Os transportes seguem exatamente o
que a especificação pede: **TCP** para controle, **UDP** para o fluxo de dados e
**UDP Multicast** para a descoberta inicial.

---

## Arquitetura

```
                          Cliente Analítico
                          (CLI + dashboard)
                                  |
                        TCP (Protocol Buffers)
                                  |
                                  v
        +--------------------- Gateway ----------------------+
        |  - descoberta (multicast)                          |
        |  - recebe dados (UDP)                              |
        |  - controla fontes (TCP)                           |
        |  - persiste em SQLite + consultas analíticas       |
        |  - dashboard web (extra)                           |
        +----------------------------------------------------+
            ^  multicast UDP (descoberta)       ^  UDP (dados)
            |  TCP (comandos)                   |
   +--------+--------+--------+--------+--------+
   |        |        |        |        |        |
 weather  air    traffic   noise    energy (Node.js)
 station quality counter  sensor    meter
 (Py)    (Py)    (Py)     (Py)      (JS)
```

### Fluxos de comunicação

| Fluxo | Transporte | Mensagens (proto) |
|---|---|---|
| Descoberta: Gateway → fontes | **UDP Multicast** (228.0.0.8:6789) | `DiscoveryRequest` |
| Resposta da fonte → Gateway | **UDP** unicast | `UdpEnvelope{announce: SourceAnnounce}` |
| Dados: fontes → Gateway | **UDP** | `UdpEnvelope{data: SensorData}` |
| Comando: Gateway → fonte | **TCP** | `Command` / `CommandResult` |
| Cliente ↔ Gateway | **TCP** | `ClientRequest` / `ClientResponse` |

O Gateway **re-emite** o `DiscoveryRequest` periodicamente (a cada 8 s), o que
torna o sistema imune à ordem de inicialização: uma fonte que suba depois do
Gateway é descoberta no próximo ciclo.

---

## Componentes

### Gateway (`gateway.py`)
Ponto central. Roda quatro threads: emissor de descoberta (multicast), receptor
UDP (anúncios + dados), servidor TCP (atende o cliente) e verificador de
*liveness* (marca como `FAILED` quem fica >25 s sem contato — cobre tanto a
falha simulada quanto a morte do processo). Persiste todas as leituras em SQLite
e responde às consultas analíticas.

### Fontes de dados
Cada fonte é um **processo separado**. As quatro fontes Python herdam de
`base_source.py` (que cuida de descoberta, servidor de controle TCP e laço de
envio UDP); cada subclasse só implementa como gerar suas leituras.

| Fonte | Arquivo | Métricas | Controlável? |
|---|---|---|---|
| Estação meteorológica | `sources/weather_station.py` | temperatura, umidade | sim |
| Qualidade do ar | `sources/air_quality.py` | co2, material_particulado | sim |
| Contador de tráfego | `sources/traffic_counter.py` | veiculos_por_min | sim |
| Sensor de ruído | `sources/noise_sensor.py` | ruido | **não** (sensor contínuo) |
| Medidor de energia | `node_source/energy_meter.js` | consumo | sim (**Node.js**) |

Fontes controláveis aceitam: `SET_FREQUENCY`, `SET_THRESHOLD`, `ACTIVATE`,
`DEACTIVATE`, `SIMULATE_FAILURE`, `GET_STATUS`. Ao ultrapassar o limiar, a fonte
envia uma leitura **imediata** marcada com `alert=true` (envio por evento).

### Cliente Analítico (`client.py`)
CLI interativa que conecta no Gateway via TCP e permite: listar fontes e estados,
enviar comandos a uma fonte e executar consultas agregadas (média, desvio padrão,
mín., máx., contagem, **fonte com maior variação**, histórico, última leitura).

### Dashboard web (`dashboard.py` + `web/index.html`)
Servido pelo próprio Gateway em `http://localhost:8080`. Mostra os cards das
fontes com status em tempo real e um gráfico de séries temporais (Chart.js),
com *polling* a cada 3 s.

---

## Como executar

### Pré-requisitos
- Python 3.10+
- Node.js 18+ (apenas para a fonte em Node.js)

### 1. Instalar dependências

```bash
pip install -r requirements.txt          # protobuf (e grpcio-tools p/ regerar o proto)
cd node_source && npm install && cd ..    # protobufjs (fonte Node.js)
```

> O `smartcity_pb2.py` já vem gerado. Para regerar após editar o `.proto`:
> `bash scripts/gen_proto.sh`

### 2. Subir o sistema

**Opção A — tudo de uma vez (teste rápido):**
```bash
bash scripts/run_all.sh        # Ctrl+C encerra tudo
```

**Opção B — um terminal por processo (recomendado para a demo/vídeo):**
```bash
# terminal 1 — gateway (sobe também o dashboard em http://localhost:8080)
python gateway.py

# terminais 2..5 — fontes Python:  python sources/<fonte>.py <id> <freq_s> [limiar]
python sources/weather_station.py wx-1 15 38
python sources/air_quality.py     air-1 10 800
python sources/traffic_counter.py traf-1 8
python sources/noise_sensor.py    noise-1 12

# terminal 6 — fonte Node.js:  node energy_meter.js <id> <freq_s>
cd node_source && node energy_meter.js energy-1 10

# terminal 7 — cliente analítico
python client.py
```

Para desativar o dashboard: `python gateway.py --no-dashboard`.

### 3. Usar o cliente
O menu cobre os três grupos de operações: **listar fontes**, **enviar comando**
(ativar/desativar, mudar frequência, ajustar limiar, simular falha, consultar
estado) e **consultas analíticas**. Exemplos diretos da especificação:
- média de temperatura da última hora → `QUERY → AVG → metric=temperatura → janela=3600`
- desvio padrão de CO₂ nas últimas 24 h → `QUERY → STDDEV → metric=co2 → janela=86400`
- fonte com maior variação → `QUERY → MAX_VARIATION → metric=co2`

---

## Mapeamento requisitos → implementação

| Requisito da especificação | Onde |
|---|---|
| Protobuf em **todas** as mensagens | `smartcity.proto` (usado por todos os componentes) |
| TCP no controle cliente ↔ gateway | `client.py`, `gateway.py` (servidor TCP + `protocol.send/recv_msg`) |
| UDP no fluxo de dados fontes → gateway | `base_source.py` (laço de envio), `gateway.py` (receptor UDP) |
| UDP Multicast na descoberta | gateway emite `DiscoveryRequest`; fontes respondem `SourceAnnounce` |
| Fonte anuncia tipo, IP+porta e estado | `SourceAnnounce` |
| Pelo menos uma fonte controlável | 4 das 5 fontes são controláveis |
| Cliente lista fontes e estados | `LIST_SOURCES` |
| Cliente envia comandos | `SEND_COMMAND` (gateway encaminha à fonte via TCP) |
| Consultas agregadas (média/desvio/maior variação) | `QUERY` → `AVG`/`STDDEV`/`MAX_VARIATION` |
| Simular falha | `SIMULATE_FAILURE` + *liveness* do gateway |

### Pontuação extra (tudo implementado)
1. **Mais de uma linguagem** — fonte do medidor de energia em **Node.js**
   (protobufjs), interoperando com o Gateway em Python via os mesmos `.proto`.
2. **Persistência + histórico** — **SQLite** (`storage.py`); o cliente consulta o
   histórico com `QUERY → HISTORY`.
3. **Interface gráfica com séries temporais** — **dashboard web** (`dashboard.py`
   + `web/index.html`, Chart.js).

---

## Formato das mensagens

Definição única em [`smartcity.proto`](smartcity.proto). Destaques:

- **`DiscoveryRequest`** — anuncia IP/portas do gateway no grupo multicast.
- **`SourceAnnounce`** — identidade da fonte: tipo, IP, porta de controle, estado,
  se é controlável, frequência e limiar.
- **`SensorData`** — `source_id`, tipo, `Timestamp`, lista de `Reading`
  (nome/valor/unidade) e flags de alerta.
- **`UdpEnvelope`** — `oneof` que permite anúncios e dados na **mesma** porta UDP.
- **`Command` / `CommandResult`** — controle gateway ↔ fonte.
- **`ClientRequest` / `ClientResponse`** — protocolo cliente ↔ gateway
  (lista de fontes, resultado de comando, linhas de consulta).

**Framing TCP:** como o TCP é um fluxo de bytes sem fronteiras de mensagem, cada
mensagem é prefixada por seu tamanho em 4 bytes *big-endian* (`protocol.py`).
No UDP cada datagrama já é uma mensagem completa.

---

## Estrutura do projeto

```
trabalho1_sockets/
├── smartcity.proto         # definição de todas as mensagens
├── smartcity_pb2.py        # gerado a partir do .proto
├── protocol.py             # constantes de rede + framing TCP + IP local
├── storage.py              # persistência SQLite + agregações
├── base_source.py          # classe base das fontes Python
├── gateway.py              # Gateway central
├── client.py               # Cliente Analítico (CLI)
├── dashboard.py            # servidor HTTP do dashboard
├── web/index.html          # dashboard web (Chart.js)
├── sources/                # 4 fontes Python
│   ├── weather_station.py
│   ├── air_quality.py
│   ├── traffic_counter.py
│   └── noise_sensor.py
├── node_source/            # fonte em Node.js (extra: 2ª linguagem)
│   ├── energy_meter.js
│   └── package.json
├── scripts/
│   ├── run_all.sh          # sobe tudo num terminal
│   └── gen_proto.sh        # regenera o _pb2.py
└── requirements.txt
```
