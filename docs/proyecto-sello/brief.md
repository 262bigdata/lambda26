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

Este rol es un **énfasis de coordinación a nivel de equipo** (quién profundiza más en X aspecto compartido, como la observabilidad común o el empaquetado DataOps), no un reemplazo del trabajo individual: cada integrante igual construye de extremo a extremo su propia dimensión (batch + streaming + BI + ML, sección 3), sin importar el rol que tenga aquí.

## 2. Dominio del proyecto

- Nombre del proyecto:
- Problema o necesidad que resuelve (2-4 líneas):
- Dominio de datos (breve — sector o rubro, ej. logística, salud, retail, IoT industrial, finanzas, y qué se mide o registra):
- **Pregunta central de negocio** que responde el sistema completo (1-2 líneas, redactada como pregunta — ej. "¿cómo optimizar la operación logística de reparto de la empresa X?"). Es el hilo conductor único del proyecto: en la sección 3, cada integrante no trae su propia pregunta independiente, trae una **dimensión** de esta misma pregunta central, y todas convergen en un solo tablero/historia final, no en tableros sueltos:
- Usuarios / actores principales (quién consume las salidas del sistema — analista, gerente, otro sistema automatizado, etc.):
- Arquitectura Big Data prevista — Lambda o Kappa, y por qué (justificación breve, criterio de decisión en S1, 2.5.1):
- Fuente de datos batch (dataset real o realista — público, de datos abiertos de gobierno, o real de una organización; no completamente inventado sin fundamento). Indica cuál y de dónde:
- Fuente de eventos en tiempo real (empresariales y/o IoT/sensores; puede ser simulada de forma realista — a diferencia del dataset batch, aquí sí es aceptable simular, como hace `lambda26` con sensores). Indica cuál:

  La fuente batch y la fuente streaming **no tienen que ser datasets distintos** — pueden ser el mismo sensor u origen, visto en dos momentos: streaming es la lectura en vivo, tal como llega (ej. un sensor atmosférico publicando cada pocos segundos su campo eléctrico, campo magnético, temperatura, humedad, viento, lluvia); batch es la acumulación histórica de esas mismas lecturas, procesada después en bloque con Spark (limpieza, agregación diaria/mensual, formato analítico). Esto es, de hecho, el patrón típico de arquitectura Lambda: una sola fuente alimentando la capa de velocidad (streaming) y la capa batch a la vez.

- ¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo? Si continúa, indicar cuál:

## 3. Dimensiones de análisis y fuentes previstas

**Regla de asignación:** cada integrante propone (o hereda) **una dimensión** de la pregunta central de negocio declarada en la sección 2 — no una pregunta independiente propia. Una dimensión es un ángulo específico de esa misma pregunta (ej. si la pregunta central es "¿cómo optimizar la operación logística de reparto?", las dimensiones podrían ser tiempo de entrega, costo operativo, demanda por zona). Todas las dimensiones del equipo deben converger en **un solo tablero o historia final** — no en tableros sueltos por integrante. Esto evita lo que el Proyecto Sello marca como inválido: "notebooks o jobs Spark aislados sin un flujo de datos común".

Esta estructura es la misma **matriz de operacionalización de variables** de metodología de la investigación, aplicada al proyecto: la pregunta central contiene la(s) **variable(s)** de interés (ej. "operación logística", "condiciones climáticas de riesgo"); cada **dimensión** es un aspecto de esa variable; cada dimensión se mide con uno o más **indicadores** (la métrica concreta, la que termina siendo el KPI); y cada indicador se recoge con un **instrumento** — que aquí es, literalmente, la fuente batch o streaming de la ficha (el sensor, el evento, el dataset).

Para responder su dimensión, lo ideal es apoyarse en **dos fuentes**: una **batch** (histórico, para calibrar, entrenar o comparar contra el pasado) y una **streaming** (en vivo, para el estado actual) — equivalentes a `uso-pyspark` (S1-S3) y a la ingesta de eventos empresariales/IoT en `lambda26` (S6-S9), respectivamente. Ambas pueden venir del **mismo origen físico** (ver sección 2) — ej. un sensor atmosférico: la parte streaming es la lectura en vivo (temperatura, humedad, campo eléctrico, tal como llega), la parte batch es la acumulación histórica de esas mismas variables, procesada por lotes. No es obligatorio que sean fuentes distintas — sí es obligatorio que el tratamiento (streaming vs. batch) sea real y distinto entre las dos.

No toda dimensión necesita las dos obligatoriamente: si la pregunta se responde bien solo con batch (ej. un perfil o segmentación que no cambia minuto a minuto), esa dimensión puede quedarse en batch. Lo que sí es obligatorio es a **nivel de equipo**: entre todas las dimensiones, tiene que haber al menos una con streaming real — nadie construye el sistema completo sin haber tocado Kafka/Spark Structured Streaming en algo.

**Ejemplo de una pregunta central desglosada en dimensiones** (pregunta central: "¿cómo optimizar la operación logística de reparto de la empresa X?"):

| Integrante | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|
| A | ¿Cuál es el tiempo de entrega promedio, por zona? | Minutos promedio de entrega, por zona y por día | Histórico de tiempos de entrega por zona | Eventos de entrega completada en vivo |
| B | ¿Qué unidades están fuera de ruta ahora mismo? | Distancia (m) entre la posición actual y la ruta planificada, por unidad | Rutas planificadas / histórico de recorridos | Ubicaciones GPS en vivo de cada unidad |
| C | ¿Cuál es la demanda esperada de pedidos por zona la próxima semana? | Número de pedidos proyectados, por zona y por día | Histórico de pedidos por zona (para entrenar el modelo) | Pedidos en vivo (para contrastar contra lo predicho) |

Las tres dimensiones son preguntas distintas, pero las tres responden a la **misma** operación logística — juntas arman un solo tablero de "salud de la operación de reparto", no tres proyectos sueltos.

**Otro ejemplo, con sensores atmosféricos** (pregunta central: "¿cómo anticipar condiciones climáticas de riesgo en la zona monitoreada?"):

| Integrante | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|
| A | ¿Cuándo se espera que la temperatura supere el umbral de riesgo (helada o calor extremo)? | Temperatura en °C, medida cada pocos segundos | Histórico de temperatura de la estación | Lecturas en vivo del sensor de temperatura |
| B | ¿Qué tan probable es una tormenta eléctrica en las próximas horas? | Intensidad de campo eléctrico y magnético, medida cada pocos segundos | Histórico de campo eléctrico y magnético asociado a tormentas pasadas | Lecturas en vivo de campo eléctrico y magnético |
| C | ¿Cuál es el riesgo de inundación según la lluvia acumulada? | Milímetros de lluvia acumulada en las últimas 24h | Histórico de precipitación acumulada por temporada | Lecturas en vivo de lluvia y humedad |

Aquí las tres dimensiones pueden salir del **mismo sensor físico** (distintas variables que mide a la vez) — lo que las conecta no es el sensor, es que las tres alimentan la misma pregunta central: el riesgo climático de la zona, visible en un solo tablero con paneles de temperatura, tormenta e inundación.

**Tabla de asignación:**

| Integrante | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|
| | | | | |
| | | | | |
| | | | | |

La salida de cada dimensión tiene dos partes, **ambas obligatorias** — no son rutas alternativas, una se integra a la otra:

- **Tablero BI con KPIs en Grafana.** Si la dimensión tiene streaming, el KPI se refresca por evento — cada vez que ocurre el hecho de negocio (ej. una venta, una lectura de sensor o a partir de logs), a partir del evento directamente; si el caso lo amerita, incluye alertas (ej. temperatura fuera de un rango normal). Si la dimensión es solo batch, el KPI se refresca en cada corrida del pipeline (diaria/por lote) — igual vive en el mismo Grafana, solo que no se actualiza en vivo.
- **Machine Learning sobre esos mismos datos** — series de tiempo (predicción/inferencia) es lo más natural para dimensiones con streaming, pero no es la única opción válida (clasificación, detección de anomalías, regresión sobre datos batch, etc., también cuentan). El modelo no reemplaza el tablero BI: se integra a él — la predicción, la clasificación o la alerta que genera el modelo también se visualiza en el mismo Grafana, no aparte.

Los KPIs de todas las dimensiones del equipo se muestran en el **mismo Grafana**, no en instancias o dashboards separados por integrante — el resultado visible del proyecto es un solo tablero con varios paneles, uno por dimensión.

**Ficha por dimensión** — completa un bloque como este por cada fila de la tabla anterior (repite el bloque tantas veces como integrantes tenga el equipo):

### Dimensión: ______ (integrante: ______)

- Dimensión, redactada como pregunta completa (1-2 líneas), y cómo se relaciona con la pregunta central de la sección 2:
- Indicador(es) — la métrica concreta que responde a la dimensión (la que se convierte en el KPI del tablero):
- Decisión o acción que habilita (qué hace alguien distinto gracias a esta respuesta):
- Instrumento/fuente batch — descripción, esquema (campos/columnas clave) y origen (dataset público/URL, API, exporte periódico, etc.):
- Instrumento/fuente streaming — descripción, esquema y origen (tópico Kafka, sensor simulado, evento de negocio, etc.); si esta dimensión es solo batch, escribe "no aplica — ver justificación abajo":
- Salida: panel(es) de BI/KPIs en el Grafana común **y** el modelo de ML que corresponda (ambos obligatorios, ver arriba) — describe los dos:
- ¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final? (todas deben aportar a la misma historia, no quedar sueltas). Si todavía no lo sabes, escribe "por definir".
- Lista inicial de requisitos (mínimo 3, redactados como "el sistema debe..."):
    1.
    2.
    3.

### Dimensión: ______ (integrante: ______)

- Dimensión, redactada como pregunta completa, y relación con la pregunta central:
- Indicador(es):
- Decisión o acción que habilita:
- Instrumento/fuente batch:
- Instrumento/fuente streaming:
- Salida (BI/KPIs y ML):
- ¿Con qué otra(s) dimensión(es) del equipo se combina?
- Lista inicial de requisitos:
    1.
    2.
    3.

*(repite este bloque por cada integrante restante del equipo, hasta cubrir la tabla completa)*

- Qué SÍ cubre este proyecto en conjunto:
- Qué NO cubre — fuera de alcance, explícito:

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana **y estimación de costos operacionales**, prácticas de DataOps) sigue el mismo patrón que enseña cada sesión sobre `lambda26` (S8-S9) — no se declara aquí porque aplica igual sin importar el dominio de cada equipo; se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- Docente:
- Fecha:
