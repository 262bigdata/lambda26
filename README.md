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

Ya pasamos Unidad 1 (S1-S5, solo `pyspark/`, sin Kafka) — en Unidad 2 el
curso ya es streaming de punta a punta, así que **Kafka va primero**: `obs/`
y `pyspark` (con `compose.kafka.yml`) se unen a su red Docker, y fallan si
Kafka no está arriba todavía.

Todos los comandos corren desde la **raíz del repositorio** (`-f <carpeta>/compose.yml`), sin entrar ni volver a salir de cada carpeta:

**1. Kafka** — broker, Kafka UI y *exporter*:

```powershell
docker compose -f kafka/compose.yml up -d
```

**2. PySpark + Jupyter**, ya unido a la red de Kafka (no `pyspark/compose.yml` solo, ese es el de Unidad 1, sin streaming):

```powershell
docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d
```

Abre JupyterLab en `http://localhost:4488/lab?token=sintoken`.

**3. `obs/`** (opcional) — Prometheus + Grafana:

```powershell
docker compose -f obs/compose.yml up -d
```

**4. El caso de uso de tu sesión** — publica eventos a Kafka; levanta **uno solo**, el que corresponda a la sesión en la que estás, no los dos:

```powershell
# S7-S9: sensores/MQTT
docker compose -f uso-atmos/compose.yml up -d
```

```powershell
# S10: campo eléctrico
docker compose -f uso-campo-electrico/compose.yml up -d
```

Detalle completo (puertos, verificación, *errores frecuentes*) en el
`README.md` de cada carpeta: [`kafka/`](kafka/README.md),
[`pyspark/`](pyspark/README.md), [`obs/`](obs/README.md),
[`uso-atmos/`](uso-atmos/README.md).

`.\mvnw.cmd spring-boot:run`, `npm start` o cualquier proceso en primer
plano de un caso de uso (`uso-microserv/`) queda corriendo en su propia
terminal — para el siguiente paso, duplica la pestaña en vez de cerrarla
(cerrarla apaga ese proceso).

## Detener

Mismo criterio que el arranque: cada stack baja por separado, desde la raíz, con `-f`:

```powershell
docker compose -f kafka/compose.yml down
docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml down
docker compose -f obs/compose.yml down
docker compose -f uso-atmos/compose.yml down
docker compose -f uso-campo-electrico/compose.yml down
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
