"""
storage.py
----------
Persistencia do Gateway em SQLite (PONTUACAO EXTRA).

Guarda:
  * Cada leitura recebida das fontes (tabela `readings`), permitindo
    consultas sobre o historico (media da ultima hora, desvio padrao, etc.);
  * O cadastro das fontes descobertas (tabela `sources`).

SQLite + threads: como o Gateway acessa o banco a partir de varias threads
(recepcao UDP, atendimento ao cliente, dashboard), abrimos a conexao com
check_same_thread=False e serializamos todo acesso com um Lock -- padrao de
exclusao mutua visto nos exemplos de paralelismo da disciplina.
"""

import math
import sqlite3
import threading
import time


class Storage:
    def __init__(self, path="gateway_data.db"):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS readings (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT    NOT NULL,
                    type      INTEGER NOT NULL,
                    metric    TEXT    NOT NULL,
                    value     REAL    NOT NULL,
                    unit      TEXT,
                    alert     INTEGER DEFAULT 0,
                    ts        REAL    NOT NULL          -- epoch (segundos)
                );
                CREATE INDEX IF NOT EXISTS idx_readings_metric
                    ON readings(metric, ts);
                CREATE INDEX IF NOT EXISTS idx_readings_source
                    ON readings(source_id, ts);

                CREATE TABLE IF NOT EXISTS sources (
                    source_id    TEXT PRIMARY KEY,
                    type         INTEGER,
                    ip           TEXT,
                    control_port INTEGER,
                    controllable INTEGER,
                    description  TEXT,
                    first_seen   REAL,
                    last_seen    REAL,
                    status       INTEGER
                );
                """
            )
            self._conn.commit()

    # ---------------------- escrita ----------------------
    def insert_reading(self, source_id, type_, metric, value, unit, alert, ts):
        with self._lock:
            self._conn.execute(
                "INSERT INTO readings(source_id,type,metric,value,unit,alert,ts)"
                " VALUES (?,?,?,?,?,?,?)",
                (source_id, type_, metric, value, unit, 1 if alert else 0, ts),
            )
            self._conn.commit()

    def upsert_source(self, source_id, type_, ip, control_port, controllable,
                      description, status):
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                "SELECT first_seen FROM sources WHERE source_id=?", (source_id,)
            )
            row = cur.fetchone()
            first_seen = row["first_seen"] if row else now
            self._conn.execute(
                "INSERT OR REPLACE INTO sources"
                "(source_id,type,ip,control_port,controllable,description,"
                " first_seen,last_seen,status) VALUES (?,?,?,?,?,?,?,?,?)",
                (source_id, type_, ip, control_port, 1 if controllable else 0,
                 description, first_seen, now, status),
            )
            self._conn.commit()

    def touch_source(self, source_id, status=None):
        with self._lock:
            if status is None:
                self._conn.execute(
                    "UPDATE sources SET last_seen=? WHERE source_id=?",
                    (time.time(), source_id),
                )
            else:
                self._conn.execute(
                    "UPDATE sources SET last_seen=?, status=? WHERE source_id=?",
                    (time.time(), status, source_id),
                )
            self._conn.commit()

    def set_status(self, source_id, status):
        with self._lock:
            self._conn.execute(
                "UPDATE sources SET status=? WHERE source_id=?",
                (status, source_id),
            )
            self._conn.commit()

    # ---------------------- consultas analiticas ----------------------
    def _window_clause(self, window_seconds):
        if window_seconds and window_seconds > 0:
            return " AND ts >= ?", [time.time() - window_seconds]
        return "", []

    def aggregate(self, func, metric, source_id=None, window_seconds=0):
        """func in {'avg','min','max','count','stddev'}. Retorna (valor, n)."""
        where = "WHERE metric = ?"
        params = [metric]
        if source_id:
            where += " AND source_id = ?"
            params.append(source_id)
        wclause, wparams = self._window_clause(window_seconds)
        where += wclause
        params += wparams

        with self._lock:
            if func == "stddev":
                # SQLite nao tem STDDEV nativo: calculamos em Python.
                rows = self._conn.execute(
                    f"SELECT value FROM readings {where}", params
                ).fetchall()
                vals = [r["value"] for r in rows]
                n = len(vals)
                if n == 0:
                    return None, 0
                mean = sum(vals) / n
                var = sum((v - mean) ** 2 for v in vals) / n
                return math.sqrt(var), n
            else:
                sqlfn = {"avg": "AVG", "min": "MIN",
                         "max": "MAX", "count": "COUNT"}[func]
                col = "*" if func == "count" else "value"
                row = self._conn.execute(
                    f"SELECT {sqlfn}({col}) AS v, COUNT(*) AS n FROM readings {where}",
                    params,
                ).fetchone()
                return row["v"], row["n"]

    def max_variation(self, metric, window_seconds=0):
        """Fonte com maior variacao (desvio padrao) da metrica na janela.

        Retorna lista de (source_id, stddev, n) ordenada desc.
        """
        wclause, wparams = self._window_clause(window_seconds)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT source_id, value FROM readings WHERE metric=?{wclause}",
                [metric] + wparams,
            ).fetchall()
        groups = {}
        for r in rows:
            groups.setdefault(r["source_id"], []).append(r["value"])
        result = []
        for sid, vals in groups.items():
            n = len(vals)
            mean = sum(vals) / n
            std = math.sqrt(sum((v - mean) ** 2 for v in vals) / n)
            result.append((sid, std, n))
        result.sort(key=lambda x: x[1], reverse=True)
        return result

    def history(self, metric=None, source_id=None, window_seconds=0, limit=200):
        clauses = []
        params = []
        if metric:
            clauses.append("metric = ?"); params.append(metric)
        if source_id:
            clauses.append("source_id = ?"); params.append(source_id)
        if window_seconds and window_seconds > 0:
            clauses.append("ts >= ?"); params.append(time.time() - window_seconds)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT source_id, metric, value, unit, alert, ts FROM readings "
                f"{where} ORDER BY ts DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        return [dict(r) for r in rows]

    def latest_per_source(self):
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT r.source_id, r.metric, r.value, r.unit, r.ts
                FROM readings r
                JOIN (SELECT source_id, MAX(ts) AS mts
                      FROM readings GROUP BY source_id) m
                  ON r.source_id = m.source_id AND r.ts = m.mts
                ORDER BY r.source_id
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def metrics_list(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT metric FROM readings ORDER BY metric"
            ).fetchall()
        return [r["metric"] for r in rows]

    def timeseries(self, metric, window_seconds=3600):
        """Series temporais por fonte para o dashboard."""
        since = time.time() - window_seconds
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_id, value, ts FROM readings "
                "WHERE metric=? AND ts>=? ORDER BY ts ASC",
                (metric, since),
            ).fetchall()
        series = {}
        for r in rows:
            series.setdefault(r["source_id"], []).append(
                {"ts": r["ts"], "value": r["value"]}
            )
        return series

    def close(self):
        with self._lock:
            self._conn.close()
