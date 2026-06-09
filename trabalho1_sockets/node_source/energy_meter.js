/*
 * energy_meter.js
 * ---------------
 * Fonte de dados escrita em Node.js (PONTUACAO EXTRA: mais de uma linguagem).
 *
 * Demonstra interoperabilidade: troca exatamente as mesmas mensagens
 * Protocol Buffers (smartcity.proto) que as fontes Python, falando com o
 * mesmo Gateway via:
 *   - Multicast UDP (descoberta)  -> recebe DiscoveryRequest, responde SourceAnnounce
 *   - UDP (dados)                 -> envia SensorData (consumo em kW) periodicamente
 *   - TCP (controle)              -> recebe Command, responde CommandResult
 *
 * Uso:  node energy_meter.js [source_id] [frequencia_s]
 */
"use strict";

const dgram = require("dgram");
const net = require("net");
const os = require("os");
const path = require("path");
const protobuf = require("protobufjs");

// --- constantes (espelham protocol.py) ---
const MCAST_GRP = "228.0.0.8";
const MCAST_PORT = 6789;

// --- enums (valores numericos do proto3) ---
const SourceType = { ENERGY_METER: 5 };
const SourceStatus = { ACTIVE: 1, INACTIVE: 2, FAILED: 3 };
const CommandType = {
  SET_FREQUENCY: 1, SET_THRESHOLD: 2, ACTIVATE: 3,
  DEACTIVATE: 4, SIMULATE_FAILURE: 5, GET_STATUS: 6,
};

function localIP() {
  const ifs = os.networkInterfaces();
  for (const name of Object.keys(ifs)) {
    for (const i of ifs[name]) {
      if (i.family === "IPv4" && !i.internal) return i.address;
    }
  }
  return "127.0.0.1";
}

// --- framing TCP: [4 bytes BE length][payload] ---
function frame(buf) {
  const head = Buffer.alloc(4);
  head.writeUInt32BE(buf.length, 0);
  return Buffer.concat([head, buf]);
}

async function main() {
  const sourceId = process.argv[2] || `energy_meter-${Math.random().toString(16).slice(2, 8)}`;
  let frequency = parseFloat(process.argv[3]) || 10.0;
  let threshold = 50.0;            // limiar de alerta de consumo (kW)
  let status = SourceStatus.ACTIVE;
  let consumption = 18.0;          // kW
  let gatewayDataAddr = null;      // {ip, port}

  // carrega o MESMO .proto usado pelas fontes Python
  const root = await protobuf.load(path.join(__dirname, "..", "smartcity.proto"));
  const DiscoveryRequest = root.lookupType("smartcity.DiscoveryRequest");
  const SourceAnnounce = root.lookupType("smartcity.SourceAnnounce");
  const SensorData = root.lookupType("smartcity.SensorData");
  const UdpEnvelope = root.lookupType("smartcity.UdpEnvelope");
  const Command = root.lookupType("smartcity.Command");
  const CommandResult = root.lookupType("smartcity.CommandResult");

  const ip = localIP();
  const udp = dgram.createSocket("udp4");

  // ---- servidor TCP de controle (porta escolhida pelo SO) ----
  const tcp = net.createServer((sock) => {
    let buf = Buffer.alloc(0);
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 4) {
        const len = buf.readUInt32BE(0);
        if (buf.length < 4 + len) break;
        const payload = buf.subarray(4, 4 + len);
        buf = buf.subarray(4 + len);
        const cmd = Command.decode(payload);
        let msg = "";
        if (cmd.type === CommandType.SET_FREQUENCY) {
          frequency = Math.max(0.5, cmd.value); msg = `frequencia ajustada para ${frequency}s`;
        } else if (cmd.type === CommandType.SET_THRESHOLD) {
          threshold = cmd.value; msg = `limiar ajustado para ${threshold}`;
        } else if (cmd.type === CommandType.ACTIVATE) {
          status = SourceStatus.ACTIVE; msg = "fonte ativada";
        } else if (cmd.type === CommandType.DEACTIVATE) {
          status = SourceStatus.INACTIVE; msg = "fonte desativada";
        } else if (cmd.type === CommandType.SIMULATE_FAILURE) {
          status = SourceStatus.FAILED; msg = "falha simulada: parou de enviar dados";
        } else if (cmd.type === CommandType.GET_STATUS) {
          msg = "estado atual";
        } else { msg = "comando desconhecido"; }
        console.log(`[${sourceId}] comando -> ${msg}`);
        const res = CommandResult.encode(CommandResult.create({
          success: true, message: msg, status, frequency, threshold,
        })).finish();
        sock.write(frame(Buffer.from(res)));
      }
    });
    sock.on("error", () => {});
  });
  tcp.listen(0, () => {
    const controlPort = tcp.address().port;

    function sendAnnounce() {
      if (!gatewayDataAddr) return;
      const ann = SourceAnnounce.create({
        sourceId, type: SourceType.ENERGY_METER, ip, controlPort,
        status, controllable: true,
        description: "Medidor de consumo energetico (Node.js)",
        frequency, threshold,
      });
      const env = UdpEnvelope.encode(UdpEnvelope.create({ announce: ann })).finish();
      udp.send(Buffer.from(env), gatewayDataAddr.port, gatewayDataAddr.ip);
    }

    // ---- escuta multicast de descoberta ----
    const mc = dgram.createSocket({ type: "udp4", reuseAddr: true });
    mc.bind(MCAST_PORT, () => {
      try { mc.addMembership(MCAST_GRP); } catch (e) {}
    });
    mc.on("message", (data) => {
      let req;
      try { req = DiscoveryRequest.decode(data); } catch (e) { return; }
      gatewayDataAddr = { ip: req.gatewayIp, port: req.gatewayUdpDataPort };
      sendAnnounce();
      console.log(`[${sourceId}] descoberta do gateway ${req.gatewayIp}:${req.gatewayUdpDataPort} -> anunciado`);
    });

    // ---- envio periodico de dados ----
    function dataTick() {
      if (status === SourceStatus.ACTIVE && gatewayDataAddr) {
        consumption = Math.max(2, Math.min(120, consumption + (Math.random() * 16 - 8)));
        const value = Math.round(consumption * 10) / 10;
        const alert = threshold && value >= threshold;
        const data = SensorData.create({
          sourceId, type: SourceType.ENERGY_METER,
          timestamp: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
          readings: [{ name: "consumo", value, unit: "kW" }],
          alert: !!alert,
          alertMsg: alert ? `consumo=${value}kW ultrapassou o limiar ${threshold}` : "",
        });
        const env = UdpEnvelope.encode(UdpEnvelope.create({ data })).finish();
        udp.send(Buffer.from(env), gatewayDataAddr.port, gatewayDataAddr.ip);
        console.log(`[${sourceId}] enviou: consumo=${value}kW${alert ? " [ALERTA]" : ""}`);
      }
      setTimeout(dataTick, frequency * 1000);
    }

    console.log(`=== Fonte '${sourceId}' (medidor de energia, Node.js) ===`);
    console.log(`    tipo=ENERGY_METER controlavel=true porta_controle_TCP=${controlPort}`);
    console.log(`    frequencia=${frequency}s limiar=${threshold}`);
    console.log("    aguardando descoberta do gateway (multicast)...");
    dataTick();
  });
}

main().catch((e) => { console.error("erro:", e); process.exit(1); });
