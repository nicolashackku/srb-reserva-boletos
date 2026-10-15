# Sistema de Reserva de Boletos (SRB) – Primera entrega

Pontificia Universidad Javeriana – Introducción a Sistemas Distribuidos (2026-30)
Integrante: Nicolas Esteban Jimenez Castiblanco

Arquitectura base de tres capas (Figura 1 del enunciado) sobre ZeroMQ. En esta entrega funcionan las operaciones de **consulta** y **reserva**. La modificación y la cancelación están diseñadas (ver `docs/`) pero no se ejecutan; el proceso de Cancelaciones existe y recibe los mensajes del canal asíncrono. No hay tolerancia a fallas.

## Requisitos

- Python 3.11 o superior
- `pip install -r requirements.txt` (solo `pyzmq`)

## Archivos

| Archivo | Rol |
|---|---|
| `persistencia.py` | Servicio de Persistencia (SQLite `srb.db`) |
| `rmc.py` | Servicio de Reservas/Modificaciones/Consultas |
| `cancelaciones.py` | Servicio de Cancelaciones y Notificaciones (solo recepción en esta entrega) |
| `gestor_transacciones.py` | Gestor de Transacciones (ROUTER/DEALER y cuenta de TPM) |
| `cliente.py` | Cliente con generador de carga y métricas de latencia |
| `ver_estado.py` | Muestra el estado de la BD |
| `prueba_funcional.py` | Prueba de consulta, reserva y concurrencia |
| `comun.py` | Configuración, mensajes y utilidades compartidas |
| `config.json` | Direcciones y puertos de cada servicio |
| `datos/eventos_iniciales.json` | Carga inicial equivalente a la que genera el programa |
| `carga/ejemplo.json` | Ejemplo de script de operaciones para el cliente |
| `docs/` | Informe de arquitectura y diagramas (PlantUML y PNG) |

## Despliegue en dos máquinas

| Máquina | Procesos |
|---|---|
| Máquina 1 | `cliente.py` (uno o varios procesos) |
| Máquina 2 | `persistencia.py`, `rmc.py`, `cancelaciones.py`, `gestor_transacciones.py` |

1. Copiar el proyecto completo a las dos máquinas.
2. En **ambas** máquinas, editar `config.json` y poner la IP de la Máquina 2 en el campo `host` de `gestor`, `rmc`, `cancelaciones` y `persistencia` (por defecto `127.0.0.1`, que sirve para correr todo en una sola máquina).
3. En la Máquina 2 permitir en el firewall los puertos TCP 5555, 5556, 5557, 5558 y 5559.

## Ejecución

**Máquina 2** (una terminal por proceso, en este orden):

```
python persistencia.py --reiniciar
python rmc.py
python cancelaciones.py
python gestor_transacciones.py
```

`--reiniciar` borra la BD y la carga con 50 eventos de un mes (20 con dos ocurrencias, 100 asientos por ocurrencia). Sin esa opción se conserva la BD existente. Para cargar el caso de prueba del profesor: `python persistencia.py --reiniciar --archivo caso.json` (mismo formato que `datos/eventos_iniciales.json`; `ocupados` es opcional).

Al iniciar, la persistencia imprime el estado inicial de la BD y, por cada reserva, la operación y el resumen resultante. Cada proceso registra las operaciones que atiende y su resultado.

**Máquina 1:**

```
python cliente.py --id C1 --spm 30 --duracion 120 --mezcla consulta=100
python cliente.py --id C2 --spm 30 --duracion 120 --mezcla consulta=60,reserva=40
```

| Opción | Descripción |
|---|---|
| `--id` | Identificador del cliente (obligatorio) |
| `--spm` | Solicitudes por minuto (por defecto 30; el enunciado pide mínimo 20) |
| `--duracion` | Segundos de carga (por defecto 120) |
| `--mezcla` | Proporción por operación, p. ej. `consulta=70,reserva=30` |
| `--script` | Archivo JSON con operaciones explícitas (ver `carga/ejemplo.json`) |
| `--semilla` | Semilla para reproducir una corrida |

Al terminar, el cliente imprime promedio y desviación estándar de la latencia por operación, y guarda cada medición en `resultados/metricas_<id>.csv`.

## Estado de la BD y métricas

```
python ver_estado.py             # estado actual (solo ocurrencias con asientos ocupados)
python ver_estado.py --todo      # todas las ocurrencias
python ver_estado.py --cada 2    # refresca cada 2 segundos
```

`ver_estado.py` debe ejecutarse en la Máquina 2.

Al detener el Gestor con Ctrl+C escribe `resultados/tpm_gestor.csv` con las transacciones procesadas por minuto y operación, y muestra el total y los TPM en pantalla.

## Prueba funcional

Con los servicios activos y la BD recién cargada (`--reiniciar`):

```
python prueba_funcional.py
```

Verifica la carga inicial, reservas exitosas y rechazadas, y que reservas concurrentes no vendan más asientos de los disponibles.

## Detener los procesos

Ctrl+C en cada terminal. Los servicios cierran sus sockets y el Gestor exporta sus métricas.
