"""Cliente del SRB: generador de carga, métricas de latencia y consulta/reserva."""
import argparse
import csv
import json
import os
import random
import statistics
import time
from dataclasses import dataclass

import zmq

from comun import (Config, Estado, Mensaje, Operacion, Parada, configurar_log)

OPERACIONES_SOPORTADAS = (Operacion.CONSULTA, Operacion.RESERVA)


@dataclass
class RegistroLatencia:
    operacion: str
    t_inicio: float
    latencia_ms: float
    estado: str


class MetricasCliente:
    def __init__(self):
        self._registros = []

    def registrar(self, operacion, t_inicio, latencia_ms, estado):
        self._registros.append(RegistroLatencia(operacion, t_inicio, latencia_ms, estado))

    def resumen(self):
        """Promedio y desviación estándar de la latencia, y TPM, por operación."""
        if not self._registros:
            return {}
        duracion_min = max(
            (max(r.t_inicio + r.latencia_ms / 1000 for r in self._registros)
             - min(r.t_inicio for r in self._registros)) / 60, 1e-9)
        salida = {}
        for op in sorted({r.operacion for r in self._registros}):
            rs = [r for r in self._registros if r.operacion == op]
            lat = [r.latencia_ms for r in rs]
            salida[op] = {
                "n": len(rs),
                "ok": sum(r.estado == Estado.OK.value for r in rs),
                "rechazadas": sum(r.estado == Estado.RECHAZADA.value for r in rs),
                "errores": sum(r.estado == Estado.ERROR.value for r in rs),
                "media_ms": statistics.fmean(lat),
                "desv_ms": statistics.stdev(lat) if len(lat) > 1 else 0.0,
                "tpm": len(rs) / duracion_min,
            }
        return salida

    def exportar_csv(self, ruta):
        os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["t_inicio", "operacion", "latencia_ms", "estado"])
            for r in self._registros:
                w.writerow([f"{r.t_inicio:.3f}", r.operacion, f"{r.latencia_ms:.3f}", r.estado])


class ClienteSRB:
    def __init__(self, id_cliente, config):
        self.id_cliente = id_cliente
        self._endpoint = config.endpoint("gestor", "sync")
        self._timeout_ms = config.obtener("timeout_ms", 5000)
        self._ctx = zmq.Context()
        self._secuencia = 0
        self._socket = None
        self.metricas = MetricasCliente()
        self._conectar()

    def _conectar(self):
        self._socket = self._ctx.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)

    def consultar(self, mes, anio):
        return self._enviar(Operacion.CONSULTA, {"mes": mes, "anio": anio})

    def reservar(self, id_evento, fecha, asientos):
        return self._enviar(Operacion.RESERVA,
                            {"id_evento": id_evento, "fecha": fecha, "asientos": asientos})

    def _enviar(self, operacion, datos):
        self._secuencia += 1
        id_sol = f"{self.id_cliente}-{self._secuencia:06d}"
        msg = Mensaje.solicitud(operacion, self.id_cliente, id_sol, datos)
        t0 = time.time()
        inicio = time.perf_counter()
        self._socket.send(Mensaje.codificar(msg))
        if self._socket.poll(self._timeout_ms) == 0:
            # Un REQ sin respuesta queda bloqueado: se recrea el socket.
            self._socket.close()
            self._conectar()
            resp = Mensaje.respuesta(id_sol, Estado.ERROR, motivo="sin respuesta del gestor")
        else:
            resp = Mensaje.decodificar(self._socket.recv())
        latencia = (time.perf_counter() - inicio) * 1000
        self.metricas.registrar(Operacion(operacion).value, t0, latencia, resp["estado"])
        return resp

    def cerrar(self):
        self._socket.close(0)
        self._ctx.term()


class GeneradorCarga:
    """Envía solicitudes a ritmo constante, con mezcla parametrizable o desde un script."""

    def __init__(self, cliente, config, solicitudes_por_minuto, mezcla, duracion_s,
                 script=None, semilla=None):
        self._cliente = cliente
        self._mes = config.obtener("mes")
        self._anio = config.obtener("anio")
        self._max_asientos = config.obtener("max_asientos_por_reserva", 4)
        self._intervalo = 60.0 / solicitudes_por_minuto
        self._mezcla = mezcla
        self._duracion = duracion_s
        self._script = script
        self._rng = random.Random(semilla)
        self._catalogo = []
        self._log = configurar_log(cliente.id_cliente)

    def ejecutar(self):
        parada = Parada()
        inicio = time.monotonic()
        indice = 0
        while not parada.activa:
            turno = inicio + indice * self._intervalo
            if self._script is None and turno - inicio >= self._duracion:
                break
            peticion = self._siguiente(indice)
            if peticion is None:
                break
            espera = turno - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            self._ejecutar_peticion(peticion)
            indice += 1

    def _siguiente(self, indice):
        if self._script is not None:
            return self._script[indice] if indice < len(self._script) else None
        if not self._catalogo:
            return {"operacion": "CONSULTA", "mes": self._mes, "anio": self._anio}
        ops, pesos = zip(*self._mezcla.items())
        op = self._rng.choices(ops, weights=pesos)[0]
        if op == Operacion.CONSULTA.value:
            return {"operacion": op, "mes": self._mes, "anio": self._anio}
        evento = self._rng.choice(self._catalogo)
        ocurrencia = self._rng.choice(evento["ocurrencias"])
        return {"operacion": op, "id_evento": evento["id_evento"],
                "fecha": ocurrencia["fecha"],
                "asientos": self._rng.randint(1, self._max_asientos)}

    def _ejecutar_peticion(self, p):
        op = p["operacion"]
        if op == Operacion.CONSULTA.value:
            resp = self._cliente.consultar(p.get("mes", self._mes), p.get("anio", self._anio))
            if resp["estado"] == Estado.OK.value:
                self._catalogo = resp["datos"]
            detalle = f"{len(resp['datos'])} eventos" if resp["estado"] == "OK" else ""
        elif op == Operacion.RESERVA.value:
            resp = self._cliente.reservar(p["id_evento"], p["fecha"], p["asientos"])
            detalle = f"{p['id_evento']} {p['fecha']} x{p['asientos']}"
        else:
            raise ValueError(f"operación no disponible en esta entrega: {op}")
        self._log.info("%s %s -> %s %s", op, detalle, resp["estado"], resp["motivo"] or "")


def parsear_mezcla(texto):
    mezcla = {}
    for parte in texto.split(","):
        nombre, _, valor = parte.partition("=")
        op = Operacion(nombre.strip().upper())
        if op not in OPERACIONES_SOPORTADAS:
            raise ValueError(f"{op.value} no está disponible en esta entrega")
        mezcla[op.value] = float(valor)
    if not mezcla or sum(mezcla.values()) <= 0:
        raise ValueError("mezcla inválida")
    return mezcla


def imprimir_resumen(id_cliente, resumen):
    print(f"\nResumen de {id_cliente}")
    print(f"{'operación':<12}{'n':>6}{'ok':>6}{'rech.':>7}{'err.':>6}"
          f"{'media ms':>12}{'desv ms':>10}{'TPM':>8}")
    for op, r in resumen.items():
        print(f"{op:<12}{r['n']:>6}{r['ok']:>6}{r['rechazadas']:>7}{r['errores']:>6}"
              f"{r['media_ms']:>12.2f}{r['desv_ms']:>10.2f}{r['tpm']:>8.1f}")


def main():
    ap = argparse.ArgumentParser(description="Cliente / generador de carga del SRB")
    ap.add_argument("--id", required=True, help="identificador del cliente, ej. C1")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--spm", type=int, default=30, help="solicitudes por minuto")
    ap.add_argument("--duracion", type=int, default=120, help="segundos de carga")
    ap.add_argument("--mezcla", default="consulta=70,reserva=30",
                    help="proporción por operación, ej. consulta=100 o consulta=60,reserva=40")
    ap.add_argument("--script", help="archivo JSON con la lista de operaciones a enviar")
    ap.add_argument("--semilla", type=int, help="semilla del generador aleatorio")
    args = ap.parse_args()

    config = Config.cargar(args.config)
    if args.spm < 20:
        print("Aviso: el enunciado pide al menos 20 solicitudes por minuto por cliente.")
    script = None
    if args.script:
        with open(args.script, encoding="utf-8") as f:
            script = json.load(f)

    cliente = ClienteSRB(args.id, config)
    generador = GeneradorCarga(cliente, config, args.spm, parsear_mezcla(args.mezcla),
                               args.duracion, script, args.semilla)
    try:
        generador.ejecutar()
    finally:
        carpeta = config.obtener("carpeta_resultados", "resultados")
        ruta = os.path.join(carpeta, f"metricas_{args.id}.csv")
        cliente.metricas.exportar_csv(ruta)
        imprimir_resumen(args.id, cliente.metricas.resumen())
        print(f"Latencias por solicitud en {ruta}")
        cliente.cerrar()


if __name__ == "__main__":
    main()
