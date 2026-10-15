"""Servicio de Procesamiento de Reservas, Modificaciones y Consultas (RMC)."""
import argparse

import zmq

from comun import (AccionPersistencia, ClientePersistencia, Config, Estado, Mensaje,
                   Operacion, Parada, configurar_log)


class ServicioRMC:
    def __init__(self, config):
        self._config = config
        self._log = configurar_log("rmc")
        self._ctx = zmq.Context()
        self._persistencia = ClientePersistencia(
            self._ctx, config.endpoint("persistencia", "rep"),
            config.obtener("timeout_ms", 5000))

    def iniciar(self):
        sock = self._ctx.socket(zmq.REP)
        sock.bind(self._config.bind("rmc", "rep"))
        self._log.info("escuchando en %s", self._config.bind("rmc", "rep"))
        parada = Parada()
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        while not parada.activa:
            if not dict(poller.poll(500)):
                continue
            # REP conserva el sobre (identidad del cliente) y lo devuelve en la respuesta.
            raw = sock.recv()
            try:
                resp = self._atender(Mensaje.decodificar(raw))
            except Exception as exc:
                self._log.exception("error procesando solicitud")
                resp = Mensaje.respuesta("", Estado.ERROR, motivo=str(exc))
            sock.send(Mensaje.codificar(resp))
        self._log.info("detenido")
        sock.close(0)
        self._persistencia.cerrar()
        self._ctx.term()

    def _atender(self, msg):
        op = msg.get("operacion")
        id_sol = msg.get("id_solicitud", "")
        if op == Operacion.CONSULTA.value:
            resp = self._consulta(msg)
        elif op == Operacion.RESERVA.value:
            resp = self._reserva(msg)
        else:
            resp = Mensaje.respuesta(id_sol, Estado.ERROR,
                                     motivo=f"operación {op} no implementada en esta entrega")
        resp["id_solicitud"] = id_sol
        self._log.info("%s %s de %s -> %s%s", op, id_sol, msg.get("id_cliente"),
                       resp["estado"], f" ({resp['motivo']})" if resp["motivo"] else "")
        return resp

    def _consulta(self, msg):
        d = msg["datos"]
        resp = self._persistencia.solicitar(
            AccionPersistencia.EVENTOS_MES, {"mes": d["mes"], "anio": d["anio"]})
        return Mensaje.respuesta("", resp["estado"], resp["datos"], resp["motivo"])

    def _reserva(self, msg):
        d = msg["datos"]
        try:
            asientos = int(d["asientos"])
        except (KeyError, TypeError, ValueError):
            return Mensaje.respuesta("", Estado.RECHAZADA, motivo="número de asientos inválido")
        if asientos <= 0:
            return Mensaje.respuesta("", Estado.RECHAZADA, motivo="número de asientos inválido")

        disp = self._persistencia.solicitar(
            AccionPersistencia.DISPONIBILIDAD,
            {"id_evento": d["id_evento"], "fecha": d["fecha"]})
        if disp["estado"] != Estado.OK.value:
            return Mensaje.respuesta("", disp["estado"], motivo=disp["motivo"])
        if disp["datos"]["libres"] < asientos:
            return Mensaje.respuesta("", Estado.RECHAZADA, motivo="sin disponibilidad")

        resp = self._persistencia.solicitar(
            AccionPersistencia.CREAR_RESERVA,
            {"id_cliente": msg["id_cliente"], "id_evento": d["id_evento"],
             "fecha": d["fecha"], "asientos": asientos})
        return Mensaje.respuesta("", resp["estado"], resp["datos"], resp["motivo"])


def main():
    ap = argparse.ArgumentParser(description="Servicio RMC del SRB")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    ServicioRMC(Config.cargar(args.config)).iniciar()


if __name__ == "__main__":
    main()
