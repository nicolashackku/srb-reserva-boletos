"""Muestra el estado actual de la base de datos (para la sustentación)."""
import argparse
import time

from comun import Config
from persistencia import RepositorioSRB


def main():
    ap = argparse.ArgumentParser(description="Estado de la BD del SRB")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--todo", action="store_true",
                    help="incluye las ocurrencias sin asientos ocupados")
    ap.add_argument("--cada", type=float, help="repite la consulta cada N segundos")
    args = ap.parse_args()

    config = Config.cargar(args.config)
    try:
        while True:
            repo = RepositorioSRB(config.obtener("bd", "srb.db"))
            print(time.strftime("--- %H:%M:%S ---"))
            print("\n".join(repo.lineas_estado(solo_ocupados=not args.todo)), flush=True)
            repo.cerrar()
            if not args.cada:
                break
            time.sleep(args.cada)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
