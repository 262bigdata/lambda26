# Brief Técnico-Analítico del Proyecto Sello

Este documento es el hito de **S2** del cronograma (ver [Guía del Proyecto Sello](index.md), sección 4): el punto donde el equipo declara, por escrito, qué sistema Big Data va a construir durante el ciclo — antes de avanzar a S3 (calidad de datos y formatos analíticos). No reemplaza el informe final; es la ficha corta que fija el rumbo desde el principio.

`lambda26` es un entorno integrado de procesamiento distribuido en batch y streaming con Spark y Kafka. Las guías de S1 en adelante muestran **cómo** se construye cada capacidad (arquitectura Lambda/Kappa, ETL batch, ingesta y streaming, observabilidad, BI/ML, DataOps) sobre un caso de referencia (ver el caso de Uber en S1, 1.6). Tu equipo **aplica ese mismo patrón** sobre **su propio problema de datos** — no necesariamente sobre el mismo caso de `lambda26`, ni con las mismas fuentes. El repositorio se identifica con su propio `grupo-<numero>-<nombre-proyecto>` en los topics (ver [Guía del Proyecto Sello](index.md), sección 5).

Cada equipo llena una sola copia de este brief, la publica en su repositorio (o en su MkDocs) y la actualiza solo si el alcance cambia de verdad — no en cada sesión.

## 1. Datos del equipo

- Nombre del equipo:
- Sección:
- Repositorio (URL):
- Topics del repositorio configurados (sí/no) — incluye `grupo-<numero>-<nombre-proyecto>` (ver [Guía del Proyecto Sello](index.md), sección 5):

**Integrantes:**

| Integrante | Rol o énfasis previsto (ej. batch/Spark, streaming/Kafka, observabilidad, BI/ML) |
|---|---|
| | |
| | |
| | |
| | |

## 2. Dominio del proyecto

- Nombre del proyecto:
- Problema o necesidad que resuelve (2-4 líneas):
- Dominio de datos (breve — sector o rubro, ej. logística, salud, retail, IoT industrial, finanzas, y qué se mide o registra):
- Usuarios / actores principales (quién consume las salidas del sistema — analista, gerente, otro sistema automatizado, etc.):
- Arquitectura Big Data prevista — Lambda o Kappa, y por qué (justificación breve, criterio de decisión en S1, 2.5.1):
- Fuente de datos batch (dataset real o realista — público, de datos abiertos de gobierno, o real de una organización; no completamente inventado sin fundamento). Indica cuál y de dónde:
- Fuente de eventos en tiempo real (empresariales y/o IoT/sensores; puede ser simulada de forma realista — a diferencia del dataset batch, aquí sí es aceptable simular, como hace `lambda26` con sensores). Indica cuál:
- ¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo? Si continúa, indicar cuál:

## 3. Fuentes de datos y salidas previstas

**Regla de asignación:** cada integrante propone (o hereda) **dos fuentes o salidas de datos**, no una sola. De esas dos, **al menos una debe ser streaming** — un flujo de eventos en tiempo real vía Kafka y Spark Structured Streaming, con ventanas/agregaciones reales, equivalente a la ingesta de eventos empresariales o IoT/sensores en `lambda26` (S6-S9). La segunda **no necesariamente** es streaming — puede ser batch, un pipeline ETL con Spark sobre datos históricos, con salida particionada en Parquet, equivalente a `uso-pyspark` (S1-S3). Ningún integrante se queda con dos fuentes batch solamente.

| Integrante | Fuente/salida streaming | Fuente/salida batch |
|---|---|---|
| | | |
| | | |
| | | |

**Ficha por fuente/salida** — completa un bloque como este por cada fila de la tabla anterior (repite el bloque tantas veces como fuentes/salidas tenga el equipo):

### Fuente/salida: ______ (integrante: ______ · tipo: streaming / batch)

- Descripción breve (2-3 líneas): qué datos trae y por qué existe en el proyecto.
- Esquema principal (campos o columnas clave, con tipo de dato):
- Origen de los datos (dataset público/URL, API, tópico Kafka, sensor simulado, etc.):
- Salida esperada (tabla Parquet particionada, agregación en ventana, panel BI, modelo entrenado, inferencia, etc.):
- ¿Se combina con otra fuente/salida del equipo? ¿Cómo — join, agregación conjunta, alimenta el mismo tablero? Si todavía no lo sabes, escribe "por definir".
- Lista inicial de requisitos (mínimo 3, redactados como "el sistema debe..."):
    1.
    2.
    3.

### Fuente/salida: ______ (integrante: ______ · tipo: streaming / batch)

- Descripción breve (2-3 líneas):
- Esquema principal:
- Origen de los datos:
- Salida esperada:
- ¿Se combina con otra fuente/salida del equipo? ¿Cómo?
- Lista inicial de requisitos:
    1.
    2.
    3.

*(repite este bloque por cada fuente/salida restante del equipo, hasta cubrir la tabla completa)*

- Qué SÍ cubre este proyecto en conjunto:
- Qué NO cubre — fuera de alcance, explícito:

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana, prácticas de DataOps) sigue el mismo patrón que enseña cada sesión sobre `lambda26` — no se declara aquí porque aplica igual sin importar el dominio de cada equipo; se construye en clase, sesión por sesión, sobre las fuentes y salidas de esta ficha.

## 4. Aprobación

- Docente:
- Fecha:
