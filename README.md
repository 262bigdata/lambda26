# lambda26

Workspace del curso **Big Data**, UPeU 2026-2.

## Documentación

Sílabo, guías de sesión y arquitectura del curso viven en
[`docs/`](docs/index.md) y se publican como sitio con MkDocs.

## Estructura del repositorio

```text
lambda26/
├── docs/                 # Documentación del curso (MkDocs), sesiones S0X
├── pyspark/              # Entorno Docker + Spark + Jupyter (S1 en adelante)
├── kafka/                # Kafka, Kafka UI y exporter (S6+)
├── obs/                  # Prometheus + Grafana, observa a Kafka (S9+)
├── uso-atmos/             # Caso de uso: sensores/MQTT → Kafka
├── uso-campo-electrico/  # Caso de uso: otro flujo de sensores → Kafka
├── uso-microserv/        # (vacío por diseño) microservicio propio del equipo
└── uso-rapido/           # (vacío por diseño) caso de uso rápido del equipo
```

`uso-microserv/` y `uso-rapido/` quedan vacíos a propósito — cada equipo los
construye en su propia sesión, no vienen prearmados (ver `CLAUDE.md`).

## Requisitos

- Docker Desktop
- Git

## Arranque rápido en DEV

Cada carpeta con un `compose.yml` se levanta por separado, solo cuando la
necesitas — no hay un único comando que levante todo el workspace. El
**orden importa**: algunas carpetas se unen a la red Docker que otra ya creó,
y fallan si esa otra no está arriba primero. Detalle completo en el
`README.md` de cada una:

**1. `pyspark/`** — entorno Spark + Jupyter, base para toda sesión desde S1:

- [`pyspark/README.md`](pyspark/README.md)

**2. `kafka/`** — broker, Kafka UI y *exporter* (S6+); varias carpetas de
abajo dependen de que esta ya esté arriba:

- [`kafka/README.md`](kafka/README.md)

**3. `obs/`** (opcional) — Prometheus + Grafana (S9+); se une a la red de
`kafka/`, así que falla si Kafka no está arriba primero:

- [`obs/README.md`](obs/README.md)

**4. El caso de uso de tu sesión** — cada uno publica a Kafka, así que
también necesita el paso 2 ya arriba:

- [`uso-atmos/README.md`](uso-atmos/README.md) — sensores/MQTT.
- `uso-campo-electrico/` — otro flujo de sensores (S8/S10).

`.\mvnw.cmd spring-boot:run`, `npm start` o cualquier proceso en primer
plano de un caso de uso (`uso-microserv/`) queda corriendo en su propia
terminal — para el siguiente paso, duplica la pestaña en vez de cerrarla
(cerrarla apaga ese proceso).

## Detener

Cada carpeta baja solo sus propios contenedores:

```powershell
docker compose down
```

Todos los contenedores de `lambda26` se nombran con el prefijo `lambda26-`
(`lambda26-kafka`, `lambda26-pyspark`, `lambda26-uso-atmos`...) — para
pararlos todos juntos sin entrar carpeta por carpeta:

```powershell
docker stop (docker ps --filter "name=lambda26-" -q)
```

```bash
docker stop $(docker ps --filter "name=lambda26-" -q)
```

Si tienes **otros cursos** corriendo en paralelo (pagatu, bomerp) y
necesitas liberar memoria de verdad, para absolutamente todo lo que esté
corriendo en Docker, sin filtrar por nombre:

```powershell
docker stop (docker ps -q)
```

```bash
docker stop $(docker ps -q)
```

`docker stop` (no `down`): deja los contenedores creados, listos para un
`docker start (docker ps -a --filter "name=lambda26-" -q)` rápido la
próxima vez, en vez de recrearlos desde cero con `compose up`.
