"""Gestor de Transacciones: puerta de entrada y enrutador del SRB."""
import argparse
import csv
import os
import time
from collections import defaultdict

import zmq

from comun import Config, Estado, Mensaje, Operacion, Parada, configurar_log

SINCRONAS = {Operacion.CONSULTA.value, Operacion.RESERVA.value, Operacion.MODIFICACION.value}


class ContadorTPM:
    """Cuenta transacciones completadas por operación y por minuto de ejecución."""

    def __init__(self):
        self._inicio = time.time()
        self._conteos = defaultdict(int)

    def registrar(self, operacion, estado):
        minuto = int((time.time() - self._inicio) // 60)
        self._conteos[(minuto, operacion, estado)] += 1

    def exportar_csv(self, ruta):
        os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["minuto", "operacion", "estado", "transacciones"])
            for (minuto, op, estado), n in sorted(self._conteos.items()):
                w.writerow([minuto, op, estado, n])

    def resumen(self):
        totales = defaultdict(int)
        for (_, op, _), n in self._conteos.items():
            totales[op] += n
        minutos = max((time.time() - self._inicio) / 60, 1e-9)
        return {op: (n, n / minutos) for op, n in totales.items()}


class GestorTransacciones:
    def __init__(self, config):
        self._config = config
        self._log = configurar_log("gestor")
        self._contador = ContadorTPM()
        self._pendientes = {}

    def iniciar(self):
        ctx = zmq.Context()
        frontend = ctx.socket(zmq.ROUTER)
        frontend.bind(self._config.bind("gestor", "sync"))
        entrada_cancel = ctx.socket(zmq.PULL)
        entrada_cancel.bind(self._config.bind("gestor", "async"))
        backend = ctx.socket(zmq.DEALER)
        backend.connect(self._config.endpoint("rmc", "rep"))
        salida_cancel = ctx.socket(zmq.PUSH)
        salida_cancel.connect(self._config.endpoint("cancelaciones", "pull"))

        self._log.info("canal síncrono en %s, asíncrono en %s",
                       self._config.bind("gestor", "sync"),
                       self._config.bind("gestor", "async"))
        parada = Parada()
        poller = zmq.Poller()
        for s in (frontend, backend, entrada_cancel):
            poller.register(s, zmq.POLLIN)

        while not parada.activa:
            eventos = dict(poller.poll(500))
            if backend in eventos:
                self._devolver_respuesta(backend, frontend)
            if frontend in eventos:
                self._enrutar_sincrona(frontend, backend)
            if entrada_cancel in eventos:
                self._enrutar_cancelacion(entrada_cancel, salida_cancel)

        self._cerrar(ctx, frontend, backend, entrada_cancel, salida_cancel)

    def _enrutar_sincrona(self, frontend, backend):
        # El ROUTER antepone la identidad del cliente; se conserva hasta devolver la respuesta.
        frames = frontend.recv_multipart()
        ident, payload = frames[0], frames[-1]
        try:
            msg = Mensaje.decodificar(payload)
            op = msg["operacion"]
            id_sol = msg["id_solicitud"]
            if op not in SINCRONAS:
                raise ValueError(f"operación no soportada en el canal síncrono: {op}")
        except (ValueError, KeyError) as exc:
            resp = Mensaje.respuesta("", Estado.ERROR, motivo=f"solicitud inválida: {exc}")
            frontend.send_multipart([ident, b"", Mensaje.codificar(resp)])
            return
        self._pendientes[id_sol] = (op, msg.get("id_cliente"), time.time())
        backend.send_multipart([ident, b"", payload])

    def _devolver_respuesta(self, backend, frontend):
        frames = backend.recv_multipart()
        try:
            resp = Mensaje.decodificar(frames[-1])
            op, cliente, t0 = self._pendientes.pop(resp["id_solicitud"])
            self._contador.registrar(op, resp["estado"])
            self._log.info("%s %s de %s -> %s (%.1f ms)", op, resp["id_solicitud"], cliente,
                           resp["estado"], (time.time() - t0) * 1000)
        except (ValueError, KeyError):
            self._log.warning("respuesta sin solicitud pendiente")
        frontend.send_multipart(frames)

    def _enrutar_cancelacion(self, entrada, salida):
        payload = entrada.recv()
        try:
            msg = Mensaje.decodificar(payload)
        except ValueError:
            self._log.warning("cancelación inválida descartada")
            return
        self._contador.registrar(Operacion.CANCELACION.value, Estado.ACEPTADA.value)
        self._log.info("CANCELACION %s de %s aceptada y reenviada",
                       msg.get("id_solicitud"), msg.get("id_cliente"))
        salida.send(payload)

    def _cerrar(self, ctx, *sockets):
        carpeta = self._config.obtener("carpeta_resultados", "resultados")
        ruta = os.path.join(carpeta, "tpm_gestor.csv")
        self._contador.exportar_csv(ruta)
        for op, (n, tpm) in self._contador.resumen().items():
            self._log.info("%s: %d transacciones, %.1f TPM", op, n, tpm)
        self._log.info("detenido; métricas en %s", ruta)
        for s in sockets:
            s.close(0)
        ctx.term()


def main():
    ap = argparse.ArgumentParser(description="Gestor de Transacciones del SRB")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    GestorTransacciones(Config.cargar(args.config)).iniciar()


if __name__ == "__main__":
    main()
