"""Prueba funcional de consulta y reserva. Requiere los servicios activos y la BD recién cargada
(persistencia.py --reiniciar)."""
import argparse
import sys
import threading

from comun import Config
from cliente import ClienteSRB

fallos = []


def verificar(descripcion, condicion):
    print(("OK    " if condicion else "FALLO ") + descripcion)
    if not condicion:
        fallos.append(descripcion)


def libres(cliente, mes, anio, id_evento, fecha):
    for ev in cliente.consultar(mes, anio)["datos"]:
        if ev["id_evento"] == id_evento:
            for o in ev["ocurrencias"]:
                if o["fecha"] == fecha:
                    return o["libres"]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    config = Config.cargar(ap.parse_args().config)
    mes, anio = config.obtener("mes"), config.obtener("anio")
    c = ClienteSRB("PF", config)

    eventos = c.consultar(mes, anio)["datos"]
    verificar("la consulta devuelve 50 eventos", len(eventos) == 50)
    verificar("20 eventos tienen dos ocurrencias",
              sum(e["num_ocurrencias"] == 2 for e in eventos) == 20)
    verificar("cada ocurrencia inicia con 100 asientos libres",
              all(o["libres"] == 100 for e in eventos for o in e["ocurrencias"]))

    ev1 = eventos[0]
    f1 = ev1["ocurrencias"][0]["fecha"]
    r = c.reservar(ev1["id_evento"], f1, 3)
    verificar("reserva de 3 asientos es exitosa", r["estado"] == "OK")
    verificar("la consulta refleja 97 libres", libres(c, mes, anio, ev1["id_evento"], f1) == 97)
    r = c.reservar(ev1["id_evento"], f1, 98)
    verificar("reservar más asientos de los libres se rechaza", r["estado"] == "RECHAZADA")
    verificar("el rechazo no modifica la disponibilidad",
              libres(c, mes, anio, ev1["id_evento"], f1) == 97)
    r = c.reservar("E999", f1, 1)
    verificar("evento inexistente se rechaza", r["estado"] == "RECHAZADA")
    r = c.reservar(ev1["id_evento"], f1, 0)
    verificar("cero asientos se rechaza", r["estado"] == "RECHAZADA")

    ev2 = eventos[1]
    f2 = ev2["ocurrencias"][0]["fecha"]
    resultados = []

    def competir(i):
        cl = ClienteSRB(f"PF{i}", config)
        for _ in range(3):
            resultados.append(cl.reservar(ev2["id_evento"], f2, 10)["estado"])
        cl.cerrar()

    hilos = [threading.Thread(target=competir, args=(i,)) for i in range(5)]
    [h.start() for h in hilos]
    [h.join() for h in hilos]
    verificar("15 intentos concurrentes de 10 asientos: exactamente 10 exitosos",
              resultados.count("OK") == 10)
    verificar("no hay sobreventa (0 libres)", libres(c, mes, anio, ev2["id_evento"], f2) == 0)

    c.cerrar()
    print(f"\n{len(fallos)} fallo(s)")
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
