"""Utilidades compartidas por todos los procesos del SRB."""
import json
import logging
import signal
import time
from enum import Enum

import zmq


class Operacion(str, Enum):
    CONSULTA = "CONSULTA"
    RESERVA = "RESERVA"
    MODIFICACION = "MODIFICACION"
    CANCELACION = "CANCELACION"


class Estado(str, Enum):
    OK = "OK"
    RECHAZADA = "RECHAZADA"
    ACEPTADA = "ACEPTADA"
    ERROR = "ERROR"


class AccionPersistencia(str, Enum):
    EVENTOS_MES = "EVENTOS_MES"
    DISPONIBILIDAD = "DISPONIBILIDAD"
    CREAR_RESERVA = "CREAR_RESERVA"


class Config:
    """Lee config.json y construye los endpoints de cada servicio."""

    def __init__(self, valores):
        self.valores = valores

    @staticmethod
    def cargar(ruta="config.json"):
        with open(ruta, encoding="utf-8") as f:
            return Config(json.load(f))

    def obtener(self, clave, defecto=None):
        return self.valores.get(clave, defecto)

    def endpoint(self, servicio, rol):
        """Dirección para conectarse (connect) a un servicio."""
        s = self.valores[servicio]
        return f"tcp://{s['host']}:{s[rol]}"

    def bind(self, servicio, rol):
        """Dirección en la que el propio servicio escucha (bind)."""
        return f"tcp://*:{self.valores[servicio][rol]}"


class Mensaje:
    @staticmethod
    def solicitud(operacion, id_cliente, id_solicitud, datos):
        return {
            "id_solicitud": id_solicitud,
            "id_cliente": id_cliente,
            "operacion": Operacion(operacion).value,
            "ts_envio": time.time(),
            "datos": datos,
        }

    @staticmethod
    def respuesta(id_solicitud, estado, datos=None, motivo=None):
        return {
            "id_solicitud": id_solicitud,
            "estado": Estado(estado).value,
            "datos": datos if datos is not None else {},
            "motivo": motivo,
        }

    @staticmethod
    def codificar(msg):
        return json.dumps(msg, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def decodificar(raw):
        return json.loads(raw.decode("utf-8"))


class ClientePersistencia:
    """REQ hacia el servicio de persistencia; se recrea el socket si no hay respuesta."""

    def __init__(self, ctx, endpoint, timeout_ms=5000):
        self._ctx = ctx
        self._endpoint = endpoint
        self._timeout_ms = timeout_ms
        self._socket = None
        self._conectar()

    def _conectar(self):
        self._socket = self._ctx.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)

    def solicitar(self, accion, datos):
        msg = {"accion": AccionPersistencia(accion).value, "datos": datos}
        self._socket.send(Mensaje.codificar(msg))
        if self._socket.poll(self._timeout_ms) == 0:
            self._socket.close()
            self._conectar()
            return Mensaje.respuesta("", Estado.ERROR, motivo="persistencia no responde")
        return Mensaje.decodificar(self._socket.recv())

    def cerrar(self):
        self._socket.close()


class Parada:
    """Bandera de parada activada con Ctrl+C / SIGTERM."""

    def __init__(self):
        self.activa = False
        for nombre in ("SIGINT", "SIGTERM", "SIGBREAK"):
            if hasattr(signal, nombre):
                signal.signal(getattr(signal, nombre), self._marcar)

    def _marcar(self, *_):
        self.activa = True


def configurar_log(nombre):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(nombre)
