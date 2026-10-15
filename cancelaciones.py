"""Servicio de Cancelaciones y Notificaciones.

En la primera entrega el proceso forma parte de la arquitectura y recibe los
mensajes del canal asíncrono, pero la operación de cancelación aún no se ejecuta.
"""
import argparse

import zmq

from comun import Config, Mensaje, Parada, configurar_log


class ServicioCancelaciones:
    def __init__(self, config):
        self._config = config
        self._log = configurar_log("cancelaciones")

    def iniciar(self):
        ctx = zmq.Context()
        entrada = ctx.socket(zmq.PULL)
        entrada.bind(self._config.bind("cancelaciones", "pull"))
        self._log.info("escuchando en %s", self._config.bind("cancelaciones", "pull"))
        parada = Parada()
        poller = zmq.Poller()
        poller.register(entrada, zmq.POLLIN)
        while not parada.activa:
            if not dict(poller.poll(500)):
                continue
            msg = Mensaje.decodificar(entrada.recv())
            self._log.info("cancelación %s de %s recibida; no se procesa en esta entrega",
                           msg.get("id_solicitud"), msg.get("id_cliente"))
        self._log.info("detenido")
        entrada.close(0)
        ctx.term()


def main():
    ap = argparse.ArgumentParser(description="Servicio de Cancelaciones del SRB")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    ServicioCancelaciones(Config.cargar(args.config)).iniciar()


if __name__ == "__main__":
    main()
