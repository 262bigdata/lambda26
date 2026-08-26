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

**Regla de asignación:** cada integrante propone (o hereda) **dos dimensiones** de la pregunta central de negocio declarada en la sección 2 — no una pregunta independiente propia, sino dos ángulos de esa misma pregunta. Todas las dimensiones del equipo deben converger en **un solo tablero o historia final** — no en tableros sueltos por integrante. Esto evita lo que el Proyecto Sello marca como inválido: "notebooks o jobs Spark aislados sin un flujo de datos común".

Esta estructura es la misma **matriz de operacionalización de variables** de metodología de la investigación, aplicada al proyecto: la pregunta central contiene la(s) **variable(s)** de interés (ej. "operación logística", "condiciones climáticas de riesgo"); cada **dimensión** es un aspecto de esa variable; cada dimensión se mide con uno o más **indicadores** (la métrica concreta, la que termina siendo el KPI); y cada indicador se recoge con un **instrumento** — que aquí es, literalmente, la fuente batch o streaming de la ficha (el sensor, el evento, el dataset).

De las dos dimensiones de cada integrante, **una es descriptiva/diagnóstica y la otra es predictiva con inferencia de series de tiempo** — no hay condicionales, es siempre así:

- **Dimensión descriptiva o diagnóstica** (¿qué pasó?, ¿por qué pasó?, ¿cómo se compara?): se responde con **batch** — histórico, para calibrar, entrenar o comparar contra el pasado (equivalente a `uso-pyspark`, S1-S3, **Unidad 1**). No es en vivo: el dato se actualiza recién en la siguiente corrida del pipeline, no al instante. También admite ML (clasificación, regresión, segmentación), pero sobre datos históricos, no en vivo.
- **Dimensión predictiva con inferencia de series de tiempo** (¿qué va a pasar?, ¿cuándo se espera que ocurra?): se responde con **streaming** — en vivo, para el estado actual (equivalente a la ingesta de eventos empresariales/IoT en `lambda26`, S6-S9, **Unidad 2**). Una serie de tiempo se alimenta de datos que van llegando, no de una foto fija.

Ambas pueden venir del **mismo origen físico** (ver sección 2) — ej. un sensor atmosférico: la dimensión predictiva usa la lectura en vivo (temperatura, humedad, campo eléctrico, tal como llega), la dimensión descriptiva usa la acumulación histórica de esas mismas variables, procesada por lotes. No es obligatorio que sean fuentes distintas — sí es obligatorio que el tratamiento (streaming vs. batch) sea real y distinto entre las dos.

Esto es, **a nivel de cada integrante** (el curso evalúa [aporte individual](index.md#subaspectos-de-la-sustentacion-integral) — subaspecto 4 de la sustentación integral —, con evidencia de U1 y U2 por separado según el [alineamiento por sesiones](index.md#alineamiento-por-sesiones)), lo que garantiza que todos, sin excepción, construyan y defiendan ambas capas del curso — no solo que "alguien del equipo" haya tocado streaming.

**Ejemplo de una pregunta central desglosada en dimensiones** (pregunta central: "¿cómo optimizar la operación logística de reparto de la empresa X?"):

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| A | Descriptiva | ¿Cuál es el tiempo de entrega promedio, por zona? | Minutos promedio de entrega, por zona y por día | Histórico de tiempos de entrega por zona | — |
| A | Predictiva (series de tiempo) | ¿Cuál será el tiempo de entrega esperado en la próxima hora, por zona, según la tendencia actual? | Minutos de entrega proyectados, por zona | (mismo histórico, para entrenar) | Eventos de entrega en vivo |
| B | Descriptiva | ¿Qué porcentaje de recorridos se desvió de la ruta planificada, históricamente? | % de recorridos con desvío, por unidad y por mes | Histórico de recorridos vs. rutas planificadas | — |
| B | Predictiva (series de tiempo) | ¿Qué unidades es probable que lleguen tarde en los próximos 30 minutos, según su velocidad y trayectoria? | Probabilidad de retraso, por unidad | (mismo histórico, para entrenar) | Ubicaciones y velocidad GPS en vivo |
| C | Descriptiva | ¿Cuál fue la demanda histórica de pedidos, por zona y por mes? | Número de pedidos, por zona y por mes | Histórico de pedidos por zona | — |
| C | Predictiva (series de tiempo) | ¿Cuál es la demanda esperada de pedidos por zona la próxima semana? | Número de pedidos proyectados, por zona y por día | (mismo histórico, para entrenar el modelo) | Pedidos en vivo (para contrastar contra lo predicho) |

Seis dimensiones, tres integrantes, todas responden a la **misma** operación logística — juntas arman un solo tablero de "salud de la operación de reparto", no proyectos sueltos. Cada integrante tiene su par completo: una descriptiva (batch) y una predictiva (streaming), ambas sobre el mismo ángulo de la pregunta central.

**Otro ejemplo, con sensores atmosféricos** (pregunta central: "¿cómo anticipar condiciones climáticas de riesgo en la zona monitoreada?"):

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| A | Descriptiva | ¿Cuál fue la temperatura mínima y máxima registrada, por mes? | Temperatura mín./máx. en °C, por mes | Histórico de temperatura de la estación | — |
| A | Predictiva (series de tiempo) | ¿Cuándo se espera que la temperatura supere el umbral de riesgo (helada o calor extremo)? | Temperatura proyectada en °C, próximas horas | (mismo histórico, para entrenar) | Lecturas en vivo del sensor de temperatura |
| B | Descriptiva | ¿Con qué frecuencia ocurrieron tormentas eléctricas en la zona, históricamente? | Número de tormentas registradas, por temporada | Histórico de campo eléctrico y magnético asociado a tormentas pasadas | — |
| B | Predictiva (series de tiempo) | ¿Qué tan probable es una tormenta eléctrica en las próximas horas? | Probabilidad de tormenta, próximas horas | (mismo histórico, para entrenar) | Lecturas en vivo de campo eléctrico y magnético |
| C | Descriptiva | ¿Cuál es el riesgo de inundación según la lluvia acumulada esta semana? | Milímetros de lluvia acumulada en los últimos 7 días | Histórico de precipitación acumulada por temporada | — |
| C | Predictiva (series de tiempo) | ¿Cuánta lluvia se espera en las próximas horas, según la tendencia actual? | Milímetros de lluvia proyectados, próximas 3-6h | (mismo histórico, para entrenar) | Lecturas de lluvia en vivo |

Aquí las dimensiones pueden salir del **mismo sensor físico** (distintas variables que mide a la vez) — lo que las conecta no es el sensor, es que todas alimentan la misma pregunta central: el riesgo climático de la zona, visible en un solo tablero con paneles de temperatura, tormenta, inundación y su pronóstico.

**Tabla de asignación:**

| Integrante | Tipo (descriptiva/diagnóstica o predictiva) | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| | Descriptiva | | | | |
| | Predictiva (series de tiempo) | | | | |
| | Descriptiva | | | | |
| | Predictiva (series de tiempo) | | | | |

Cada integrante ocupa dos filas seguidas — su dimensión descriptiva y su dimensión predictiva — no una fila suelta.

La salida de cada dimensión tiene dos partes, **ambas obligatorias** — no son rutas alternativas, una se integra a la otra:

- **Tablero BI con KPIs en Grafana**, en las dos dimensiones — Grafana refresca la pantalla en vivo (cada 1-5 segundos, configurable) para **ambos** paneles por igual; eso es del lado del dashboard, no de la fuente. Lo que sí cambia entre las dos es cada cuánto se actualiza el **dato** que ese panel muestra: en la dimensión predictiva, el dato mismo cambia por evento — cada vez que ocurre el hecho de negocio (ej. una venta, una lectura de sensor o a partir de logs); si el caso lo amerita, incluye alertas (ej. temperatura fuera de un rango normal). En la dimensión descriptiva, el dato cambia recién en cada corrida del pipeline (diaria/por lote) — Grafana lo sigue mostrando en vivo, solo que ese valor se mantiene igual hasta la siguiente corrida.
- **Machine Learning**, obligatorio en la dimensión **predictiva** y opcional en la descriptiva (clasificación, detección de anomalías, regresión sobre datos batch, si suma valor). En la predictiva no basta con que la fuente sea streaming — la **inferencia del modelo también debe ejecutarse en tiempo real**, sobre cada dato que va llegando (ej. con Spark Structured Streaming aplicando el modelo ya entrenado a cada micro-lote), no entrenar/predecir una sola vez sobre un histórico y detenerse ahí. El modelo no reemplaza el tablero BI: se integra a él — la predicción, la clasificación o la alerta que genera el modelo también se visualiza en el mismo Grafana, no aparte.

Los KPIs de todas las dimensiones del equipo se muestran en el **mismo Grafana**, no en instancias o dashboards separados por integrante — el resultado visible del proyecto es un solo tablero con varios paneles, uno por dimensión.

**Ficha por dimensión** — completa un bloque como este por cada fila de la tabla anterior (dos bloques por integrante: uno para su dimensión descriptiva, otro para la predictiva):

### Dimensión: ______ (integrante: ______)

- Tipo: descriptiva/diagnóstica (batch) o predictiva con inferencia de series de tiempo (streaming):
- Dimensión, redactada como pregunta completa (1-2 líneas), y cómo se relaciona con la pregunta central de la sección 2:
- Indicador(es) — la métrica concreta que responde a la dimensión (la que se convierte en el KPI del tablero):
- Decisión o acción que habilita (qué hace alguien distinto gracias a esta respuesta):
- Instrumento/fuente batch — descripción, esquema (campos/columnas clave) y origen (dataset público/URL, API, exporte periódico, etc.):
- Instrumento/fuente streaming — descripción, esquema y origen (tópico Kafka, sensor simulado, evento de negocio, etc.); en la dimensión descriptiva, escribe "no aplica — esta dimensión es batch":
- Salida: panel(es) de BI/KPIs en el Grafana común **y** el modelo de ML que corresponda (ambos obligatorios, ver arriba) — describe los dos:
- ¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final? (todas deben aportar a la misma historia, no quedar sueltas). Si todavía no lo sabes, escribe "por definir".
- Lista inicial de requisitos (mínimo 3, redactados como "el sistema debe..."):
    1.
    2.
    3.

### Dimensión: ______ (integrante: ______)

- Tipo: descriptiva/diagnóstica o predictiva (series de tiempo):
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

*(repite este bloque por cada fila restante de la tabla, hasta cubrirla completa — dos bloques por integrante)*

- Qué SÍ cubre este proyecto en conjunto:
- Qué NO cubre — fuera de alcance, explícito:

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana **y estimación de costos operacionales**, prácticas de DataOps) sigue el mismo patrón que enseña cada sesión sobre `lambda26` (S8-S9) — no se declara aquí porque aplica igual sin importar el dominio de cada equipo; se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- Docente:
- Fecha:
