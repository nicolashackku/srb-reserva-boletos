"""Servicio de Persistencia: único proceso con acceso a la base de datos SQLite."""
import argparse
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

import zmq

from comun import (AccionPersistencia, Config, Estado, Mensaje, Parada,
                   configurar_log)

ESQUEMA = """
CREATE TABLE IF NOT EXISTS evento (
    id_evento TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    num_ocurrencias INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ocurrencia (
    id_evento TEXT NOT NULL REFERENCES evento(id_evento),
    fecha TEXT NOT NULL,
    capacidad INTEGER NOT NULL,
    ocupados INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (id_evento, fecha),
    CHECK (ocupados >= 0 AND ocupados <= capacidad)
);
CREATE TABLE IF NOT EXISTS reserva (
    id_reserva INTEGER PRIMARY KEY AUTOINCREMENT,
    id_cliente TEXT NOT NULL,
    id_evento TEXT NOT NULL,
    fecha TEXT NOT NULL,
    asientos INTEGER NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ACTIVA',
    creada_en TEXT NOT NULL
);
"""


@dataclass
class Ocurrencia:
    id_evento: str
    fecha: str
    capacidad: int
    ocupados: int

    @property
    def libres(self):
        return self.capacidad - self.ocupados

    def to_dict(self):
        return {"fecha": self.fecha, "capacidad": self.capacidad,
                "ocupados": self.ocupados, "libres": self.libres}


@dataclass
class Evento:
    id_evento: str
    nombre: str
    num_ocurrencias: int
    ocurrencias: list = field(default_factory=list)

    def to_dict(self):
        return {"id_evento": self.id_evento, "nombre": self.nombre,
                "num_ocurrencias": self.num_ocurrencias,
                "ocurrencias": [o.to_dict() for o in self.ocurrencias]}


@dataclass
class Reserva:
    id_reserva: int
    id_cliente: str
    id_evento: str
    fecha: str
    asientos: int
    estado: str = "ACTIVA"


class ReservaRechazada(Exception):
    pass


class RepositorioSRB:
    def __init__(self, ruta_bd):
        self._con = sqlite3.connect(ruta_bd, isolation_level=None, timeout=10)
        self._con.execute("PRAGMA journal_mode=WAL")

    def crear_esquema(self):
        self._con.executescript(ESQUEMA)

    def esta_vacia(self):
        return self._con.execute("SELECT COUNT(*) FROM evento").fetchone()[0] == 0

    def vaciar(self):
        self._con.executescript(
            "DROP TABLE IF EXISTS reserva; DROP TABLE IF EXISTS ocurrencia;"
            "DROP TABLE IF EXISTS evento;")
        self.crear_esquema()

    def insertar_evento(self, evento):
        self._con.execute("INSERT INTO evento VALUES (?,?,?)",
                          (evento.id_evento, evento.nombre, evento.num_ocurrencias))
        for o in evento.ocurrencias:
            self._con.execute("INSERT INTO ocurrencia VALUES (?,?,?,?)",
                              (o.id_evento, o.fecha, o.capacidad, o.ocupados))

    def transaccion(self):
        return _Transaccion(self._con)

    def eventos_del_mes(self, mes, anio):
        prefijo = f"{anio:04d}-{mes:02d}-"
        filas = self._con.execute(
            "SELECT e.id_evento, e.nombre, e.num_ocurrencias, o.fecha, o.capacidad, o.ocupados "
            "FROM evento e JOIN ocurrencia o ON o.id_evento = e.id_evento "
            "WHERE o.fecha LIKE ? ORDER BY CAST(SUBSTR(e.id_evento, 2) AS INTEGER), o.fecha",
            (prefijo + "%",)).fetchall()
        eventos = {}
        for id_ev, nombre, n, fecha, cap, ocu in filas:
            ev = eventos.setdefault(id_ev, Evento(id_ev, nombre, n))
            ev.ocurrencias.append(Ocurrencia(id_ev, fecha, cap, ocu))
        return list(eventos.values())

    def disponibilidad(self, id_evento, fecha):
        fila = self._con.execute(
            "SELECT capacidad - ocupados FROM ocurrencia WHERE id_evento=? AND fecha=?",
            (id_evento, fecha)).fetchone()
        return None if fila is None else fila[0]

    def crear_reserva(self, id_cliente, id_evento, fecha, asientos):
        # El UPDATE condicional revalida el cupo dentro de la misma transacción.
        con = self._con
        con.execute("BEGIN IMMEDIATE")
        try:
            cur = con.execute(
                "UPDATE ocurrencia SET ocupados = ocupados + ? "
                "WHERE id_evento=? AND fecha=? AND capacidad - ocupados >= ?",
                (asientos, id_evento, fecha, asientos))
            if cur.rowcount == 0:
                existe = con.execute(
                    "SELECT 1 FROM ocurrencia WHERE id_evento=? AND fecha=?",
                    (id_evento, fecha)).fetchone()
                con.execute("ROLLBACK")
                raise ReservaRechazada(
                    "sin disponibilidad" if existe else "evento o fecha inexistente")
            cur = con.execute(
                "INSERT INTO reserva (id_cliente, id_evento, fecha, asientos, creada_en) "
                "VALUES (?,?,?,?,?)",
                (id_cliente, id_evento, fecha, asientos,
                 datetime.now().isoformat(timespec="seconds")))
            id_reserva = cur.lastrowid
            con.execute("COMMIT")
        except ReservaRechazada:
            raise
        except Exception:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        return Reserva(id_reserva, id_cliente, id_evento, fecha, asientos)

    def resumen(self):
        capacidad, ocupados = self._con.execute(
            "SELECT COALESCE(SUM(capacidad),0), COALESCE(SUM(ocupados),0) FROM ocurrencia"
        ).fetchone()
        n_eventos = self._con.execute("SELECT COUNT(*) FROM evento").fetchone()[0]
        n_ocurr = self._con.execute("SELECT COUNT(*) FROM ocurrencia").fetchone()[0]
        n_res = self._con.execute(
            "SELECT COUNT(*) FROM reserva WHERE estado='ACTIVA'").fetchone()[0]
        return {"eventos": n_eventos, "ocurrencias": n_ocurr, "capacidad": capacidad,
                "ocupados": ocupados, "reservas_activas": n_res}

    def lineas_estado(self, solo_ocupados=False):
        r = self.resumen()
        lineas = [
            f"Eventos: {r['eventos']} | ocurrencias: {r['ocurrencias']} | "
            f"asientos ocupados: {r['ocupados']}/{r['capacidad']} | "
            f"reservas activas: {r['reservas_activas']}"]
        filas = self._con.execute(
            "SELECT id_evento, fecha, capacidad, ocupados FROM ocurrencia "
            "ORDER BY CAST(SUBSTR(id_evento, 2) AS INTEGER), fecha").fetchall()
        for id_ev, fecha, cap, ocu in filas:
            if solo_ocupados and ocu == 0:
                continue
            lineas.append(f"  {id_ev:<5} {fecha}  ocupados={ocu:>3}  libres={cap - ocu:>3}  "
                          f"capacidad={cap}")
        return lineas

    def cerrar(self):
        self._con.close()


class _Transaccion:
    """Agrupa las inserciones de la carga inicial en una sola transacción."""

    def __init__(self, con):
        self._con = con

    def __enter__(self):
        self._con.execute("BEGIN")

    def __exit__(self, tipo, valor, tb):
        self._con.execute("ROLLBACK" if tipo else "COMMIT")


class CargadorDatos:
    @staticmethod
    def cargar_por_defecto(repo, mes, anio, n_eventos=50, n_dobles=20, capacidad=100):
        with repo.transaccion():
            for i in range(1, n_eventos + 1):
                id_ev = f"E{i}"
                dia1 = 1 + (i % 14)
                fechas = [f"{anio:04d}-{mes:02d}-{dia1:02d}"]
                if i <= n_dobles:
                    fechas.append(f"{anio:04d}-{mes:02d}-{dia1 + 14:02d}")
                evento = Evento(id_ev, f"Evento {i}", len(fechas),
                                [Ocurrencia(id_ev, f, capacidad, 0) for f in fechas])
                repo.insertar_evento(evento)

    @staticmethod
    def cargar_desde_archivo(repo, ruta):
        with open(ruta, encoding="utf-8") as f:
            datos = json.load(f)
        with repo.transaccion():
            for e in datos["eventos"]:
                ocurrencias = [Ocurrencia(e["id_evento"], o["fecha"], o.get("capacidad", 100),
                                          o.get("ocupados", 0)) for o in e["ocurrencias"]]
                repo.insertar_evento(Evento(e["id_evento"], e.get("nombre", e["id_evento"]),
                                            len(ocurrencias), ocurrencias))


class ServicioPersistencia:
    def __init__(self, config, repo):
        self._config = config
        self._repo = repo
        self._log = configurar_log("persistencia")

    def iniciar(self):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
        sock.bind(self._config.bind("persistencia", "rep"))
        parada = Parada()
        self._log.info("escuchando en %s", self._config.bind("persistencia", "rep"))
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        while not parada.activa:
            if not dict(poller.poll(500)):
                continue
            try:
                resp = self._despachar(Mensaje.decodificar(sock.recv()))
            except Exception as exc:
                self._log.exception("error procesando solicitud")
                resp = Mensaje.respuesta("", Estado.ERROR, motivo=str(exc))
            sock.send(Mensaje.codificar(resp))
        self._log.info("detenido")
        sock.close(0)
        ctx.term()
        self._repo.cerrar()

    def _despachar(self, msg):
        accion = AccionPersistencia(msg["accion"])
        d = msg.get("datos", {})
        if accion is AccionPersistencia.EVENTOS_MES:
            eventos = self._repo.eventos_del_mes(int(d["mes"]), int(d["anio"]))
            return Mensaje.respuesta("", Estado.OK, [e.to_dict() for e in eventos])
        if accion is AccionPersistencia.DISPONIBILIDAD:
            libres = self._repo.disponibilidad(d["id_evento"], d["fecha"])
            if libres is None:
                return Mensaje.respuesta("", Estado.RECHAZADA,
                                         motivo="evento o fecha inexistente")
            return Mensaje.respuesta("", Estado.OK, {"libres": libres})
        if accion is AccionPersistencia.CREAR_RESERVA:
            return self._crear_reserva(d)
        return Mensaje.respuesta("", Estado.ERROR, motivo="acción no soportada")

    def _crear_reserva(self, d):
        asientos = int(d["asientos"])
        if asientos <= 0:
            return Mensaje.respuesta("", Estado.RECHAZADA, motivo="número de asientos inválido")
        try:
            r = self._repo.crear_reserva(d["id_cliente"], d["id_evento"], d["fecha"], asientos)
        except ReservaRechazada as exc:
            self._log.info("RESERVA rechazada: %s %s %s x%d (%s)", d["id_cliente"],
                           d["id_evento"], d["fecha"], asientos, exc)
            return Mensaje.respuesta("", Estado.RECHAZADA, motivo=str(exc))
        libres = self._repo.disponibilidad(r.id_evento, r.fecha)
        self._log.info("RESERVA #%d creada: %s %s %s x%d | libres=%d | %s",
                       r.id_reserva, r.id_cliente, r.id_evento, r.fecha, r.asientos, libres,
                       self._repo.lineas_estado()[0])
        return Mensaje.respuesta("", Estado.OK, {"id_reserva": r.id_reserva, "libres": libres})


def main():
    ap = argparse.ArgumentParser(description="Servicio de Persistencia del SRB")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--reiniciar", action="store_true",
                    help="borra la base y la vuelve a cargar con los datos iniciales")
    ap.add_argument("--archivo", help="JSON con el caso de prueba a cargar")
    args = ap.parse_args()

    config = Config.cargar(args.config)
    log = configurar_log("persistencia")
    repo = RepositorioSRB(config.obtener("bd", "srb.db"))
    repo.crear_esquema()
    if args.reiniciar:
        repo.vaciar()
    if repo.esta_vacia():
        if args.archivo:
            CargadorDatos.cargar_desde_archivo(repo, args.archivo)
        else:
            CargadorDatos.cargar_por_defecto(repo, config.obtener("mes"), config.obtener("anio"))
        log.info("datos iniciales cargados")
    log.info("estado inicial de la BD:")
    for linea in repo.lineas_estado():
        print(linea, flush=True)
    ServicioPersistencia(config, repo).iniciar()


if __name__ == "__main__":
    main()
