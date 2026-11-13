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

Este rol es un **énfasis de coordinación a nivel de equipo** (quién profundiza más en X aspecto compartido, como la observabilidad común o el empaquetado DataOps), no un reemplazo del trabajo individual: cada integrante igual construye de extremo a extremo su dimensión, medida con sus dos indicadores — U1 y U2 (sección 3) —, sin importar el rol que tenga aquí.

## 2. Dominio del proyecto

- Nombre del proyecto:
- Problema o necesidad que resuelve (2-4 líneas):
- Dominio de datos (breve — sector o rubro, ej. logística, salud, retail, IoT industrial, finanzas, y qué se mide o registra):
- **Variable de interés** — el concepto que contiene la pregunta central (ej. «operación logística», «riesgo eléctrico atmosférico»):
- **Pregunta central de negocio** que responde el sistema completo (1-2 líneas, redactada como pregunta — ej. "¿cómo optimizar la operación logística de reparto de la empresa X?"). Es el hilo conductor único del proyecto: en la sección 3, la variable se **dimensiona** en sus componentes medibles; cada integrante no trae su propia pregunta independiente, toma una **dimensión** de esa variable y la mide con **dos indicadores** (U1 batch y U2 streaming), y todas convergen en un solo tablero/historia final, no en tableros sueltos:
- Usuarios / actores principales (quién consume las salidas del sistema — analista, gerente, otro sistema automatizado, etc.):
- Arquitectura Big Data prevista — Lambda o Kappa, y por qué (justificación breve, criterio de decisión en S1, 2.5.1):
- Fuente de datos batch (dataset real o realista — público, de datos abiertos de gobierno, o real de una organización; no completamente inventado sin fundamento). Indica cuál y de dónde:
- Fuente de eventos en tiempo real (empresariales y/o IoT/sensores; puede ser simulada de forma realista — a diferencia del dataset batch, aquí sí es aceptable simular, como hace `lambda26` con sensores). Indica cuál:

  La fuente batch y la fuente streaming **no tienen que ser datasets distintos** — pueden ser el mismo sensor u origen, visto en dos momentos: streaming es la lectura en vivo, tal como llega (ej. un sensor atmosférico publicando cada pocos segundos su campo eléctrico, campo magnético, temperatura, humedad, viento, lluvia); batch es la acumulación histórica de esas mismas lecturas, procesada después en bloque con Spark (limpieza, agregación diaria/mensual, formato analítico). Esto es, de hecho, el patrón típico de arquitectura Lambda: una sola fuente alimentando la capa de velocidad (streaming) y la capa batch a la vez.

- ¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo? Puede ser un proyecto propio del equipo, o el de **otro grupo** que ya llevó Big Data en un ciclo anterior, si es pertinente darle continuidad. Si continúa, indicar cuál y de qué equipo/ciclo:

## 3. Dimensiones de análisis y fuentes previstas

**Regla de asignación:** la variable de interés declarada en la sección 2 se **dimensiona**: se descompone en sus componentes medibles. Cada integrante propone (o hereda) **una dimensión** y la mide con **dos indicadores**: uno **batch (U1)** y uno **streaming (U2)**. Todas las dimensiones del equipo deben converger en **un solo tablero o historia final** — no en tableros sueltos por integrante. Esto evita lo que el Proyecto Sello marca como inválido: "notebooks o jobs Spark aislados sin un flujo de datos común".

Esta estructura es la misma **matriz de operacionalización de variables** de metodología de la investigación, aplicada al proyecto: la pregunta central contiene la **variable** de interés (ej. «operación logística», «riesgo eléctrico atmosférico»); la variable se divide en **dimensiones**, sus componentes (en el ejemplo atmosférico: campo eléctrico, campo magnético, temperatura, viento, humedad y presión); cada dimensión se mide con **indicadores** (la métrica concreta, la que termina siendo el KPI; aquí, uno batch y uno streaming); y cada indicador se recoge con un **instrumento** — que aquí es, literalmente, la fuente batch o streaming de la ficha (el sensor, el evento, el dataset).

Siguiendo el sílabo tal cual está organizado por unidad, **los dos indicadores son descriptivo/diagnóstico + predictivo** — ninguno se queda solo en lo descriptivo. Lo que los distingue no es si predicen o no, es si esa predicción corre **en tiempo real**:

- **Indicador U1 — descriptivo/diagnóstico + predictivo, sin inferencia en tiempo real** (¿qué pasó?, ¿cómo se compara?, y también ¿qué se puede esperar, en general?): se responde con **batch** — histórico, para describir el pasado y, sobre ese mismo histórico, entrenar un modelo predictivo (regresión, como en S4; una clasificación sigue la plantilla de referencia del curso). La predicción existe, pero es una foto fija: se calcula al correr el pipeline, no se actualiza dato por dato ni reacciona a lo que llega después. Se reporta dentro del propio **notebook Jupyter/PySpark** donde corre el pipeline (tablas, gráficos con pandas/matplotlib, la predicción del modelo incluida) — es el medio ya establecido para esta unidad (S1-S4), no hace falta llevarla a Grafana.
- **Indicador U2 — descriptivo/diagnóstico + predictivo, con inferencia de series de tiempo en tiempo real** (¿qué está pasando ahora?, y además ¿qué va a pasar?, ¿cuándo?): se responde con **streaming** — el indicador describe el estado en vivo, dato por dato (equivalente a la ingesta de eventos empresariales/IoT en `lambda26`, S6-S9), y sobre ese mismo flujo corre el modelo de series de tiempo: hay que **entrenarlo y dejarlo corriendo en Spark Structured Streaming**, aplicando la inferencia a cada dato que llega — la predicción se actualiza en vivo, junto con el indicador, no es una foto fija. Se reporta en vivo en **Grafana** — indicador y predicción actualizándose juntos, dato por dato; si el caso lo amerita, incluye alertas (ej. temperatura fuera de un rango normal).

Ambos indicadores miden la **misma dimensión** y pueden venir del **mismo origen físico** (ver sección 2) — ej. un sensor atmosférico: U2 usa la lectura en vivo del campo eléctrico, tal como llega, y U1 usa la acumulación histórica de esas mismas lecturas, procesada por lotes. No es obligatorio que sean fuentes distintas — sí es obligatorio que el tratamiento (streaming vs. batch, y con o sin inferencia en tiempo real) sea real y distinto entre las dos.

Esto es, **a nivel de cada integrante** (el curso evalúa [aporte individual](index.md#subaspectos-de-la-sustentacion-integral) — subaspecto 4 de la sustentación integral —, con evidencia de U1 y U2 por separado según el [alineamiento por sesiones](index.md#alineamiento-por-sesiones)), lo que garantiza que todos, sin excepción, construyan y defiendan ambas capas del curso — no solo que "alguien del equipo" haya tocado streaming.

**Ejemplo de una variable dimensionada** (pregunta central: "¿cómo optimizar la operación logística de reparto de la empresa X?"; variable: operación logística):

| Integrante | Dimensión | Indicador U1 (batch) | Indicador U2 (streaming) | Fuente/instrumento batch (U1) | Fuente/instrumento streaming (U2) |
|---|---|---|---|---|---|
| A | Tiempo de entrega | Minutos promedio por zona y por día; minutos esperados por un modelo de regresión (distancia, franja horaria) | Minutos de entrega proyectados por zona para el siguiente intervalo | Histórico de tiempos de entrega por zona | Eventos de entrega en vivo |
| B | Cumplimiento de la ruta | % de desvío por unidad y por semana; desvío esperado por un modelo de regresión sobre el histórico | Desvío proyectado de cada unidad en el siguiente intervalo, según su trayectoria | Histórico de recorridos vs. rutas planificadas | Ubicaciones y velocidad GPS en vivo |
| C | Demanda de pedidos | Pedidos por zona y por día; pedidos esperados del día siguiente, por un modelo de regresión | Pedidos proyectados por zona para el siguiente intervalo | Histórico de pedidos por zona | Pedidos en vivo |

Tres dimensiones, tres integrantes, todas componentes de la **misma** operación logística — juntas arman un solo tablero de "salud de la operación de reparto", no proyectos sueltos. Cada integrante mide su dimensión con dos indicadores: uno batch sin tiempo real (U1) y uno con tiempo real (U2).

**Otro ejemplo, con sensores atmosféricos** (pregunta central: "¿cómo anticipar condiciones climáticas de riesgo en la zona monitoreada?"; variable: condiciones climáticas de riesgo):

| Integrante | Dimensión | Indicador U1 (batch) | Indicador U2 (streaming) | Fuente/instrumento batch (U1) | Fuente/instrumento streaming (U2) |
|---|---|---|---|---|---|
| A | Temperatura | Temperatura mín./máx. por día, en °C; temperatura esperada del día siguiente, por un modelo de regresión sobre el histórico diario | Temperatura proyectada en el siguiente intervalo de lectura, con alerta si cruza el umbral (helada o calor extremo) | Histórico de temperatura de la estación | Lecturas en vivo del sensor de temperatura |
| B | Campo eléctrico | % de lecturas fuera de su rango de referencia, por día; campo eléctrico esperado según las condiciones ambientales (regresión) | Campo eléctrico proyectado en el próximo minuto, con alerta si se acerca al umbral de riesgo | Histórico de campo eléctrico y de variables ambientales | Lecturas en vivo del campo eléctrico |
| C | Lluvia | Milímetros acumulados por día y su tendencia; acumulado esperado del día siguiente (regresión) | Milímetros proyectados en el siguiente intervalo, con alerta por acumulación | Histórico de precipitación | Lecturas de lluvia en vivo |

Dos precisiones sobre estos ejemplos. El **rango de referencia** del indicador U1 de la dimensión B se define con un periodo de referencia (por ejemplo, los primeros días del histórico) y se aplica al resto: si se calculara sobre todo el histórico, el porcentaje fuera de rango saldría casi fijo por definición y no informaría nada. Y el **horizonte** del indicador U2 es el siguiente intervalo de lectura — lo que S10 entrena y valida —; un pronóstico más largo es una extensión, no el requisito.

Aquí las dimensiones pueden salir del **mismo sensor físico** (distintas magnitudes que mide a la vez) — lo que las conecta no es el sensor, es que todas componen la misma variable: las condiciones climáticas de riesgo de la zona, visibles en un solo tablero con paneles de temperatura, campo eléctrico, lluvia y su pronóstico.

**Tabla de asignación:**

| Integrante | Dimensión | Indicador U1 (batch) | Indicador U2 (streaming) | Fuente/instrumento batch (U1) | Fuente/instrumento streaming (U2) |
|---|---|---|---|---|---|
| | | | | | |
| | | | | | |
| | | | | | |
| | | | | | |

Cada integrante ocupa **una fila**: su dimensión, medida con un indicador batch (U1) y uno streaming (U2). Un grupo de tres o cuatro integrantes cubre tres o cuatro dimensiones de la misma variable.

En ninguno de los dos indicadores alcanza con la métrica sola — el modelo predictivo (visto arriba) es obligatorio en ambos. Los paneles de **todos** los indicadores U2 del equipo se muestran en el **mismo Grafana**, no en instancias separadas por integrante — el resultado visible en tiempo real del proyecto es un solo tablero con varios paneles, uno por dimensión; los indicadores U1 se documentan en sus respectivos notebooks.

**Ficha por dimensión** — completa un bloque como este por cada integrante (un bloque por dimensión):

### Dimensión: ______ (integrante: ______)

- Dimensión (componente de la variable) y cómo se relaciona con la pregunta central de la sección 2:
- Decisión o acción que habilita (qué hace alguien distinto gracias a esta dimensión):
- **Indicador U1 (batch, sin tiempo real)** — pregunta que responde (1-2 líneas), métrica concreta (la que se convierte en KPI) y valor que entrega el modelo predictivo:
- Modelo predictivo U1 — tipo (regresión o clasificación) y que corre una sola vez:
- Instrumento/fuente batch — descripción, esquema (campos/columnas clave) y origen (dataset público/URL, API, exporte periódico, etc.):
- Salida U1 — cómo se reporta en el notebook (tablas/gráficos, predicción incluida):
- **Indicador U2 (streaming, con tiempo real)** — pregunta que responde, métrica y horizonte del pronóstico (el siguiente intervalo de lectura es suficiente):
- Modelo predictivo U2 — modelo de series de tiempo, comparado contra la persistencia, que corre en vivo sobre el flujo:
- Instrumento/fuente streaming — descripción, esquema y origen (tópico Kafka, sensor simulado, evento de negocio, etc.); si tu histórico es la acumulación del mismo streaming que usa tu indicador U2, indícalo igual aquí (mismo instrumento, solo que en U1 no se procesa en vivo):
- Salida U2 — el panel del indicador y de la predicción en el Grafana común, ambos en vivo:
- ¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final? (todas deben aportar a la misma historia, no quedar sueltas). Si todavía no lo sabes, escribe "por definir".
- Lista inicial de requisitos (mínimo 3, redactados como "el sistema debe..."):
    1.
    2.
    3.

*(repite este bloque por cada integrante del equipo)*

- Qué SÍ cubre este proyecto en conjunto:
- Qué NO cubre — fuera de alcance, explícito:

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana **y estimación de costos operacionales**, prácticas de DataOps) sigue el mismo patrón que enseña cada sesión sobre `lambda26` (S8-S9) — no se declara aquí porque aplica igual sin importar el dominio de cada equipo; se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- Docente:
- Fecha:
