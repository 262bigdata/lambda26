# S12 - Evaluación de la Unidad II

## 1. Propósito de la evaluación

Esta sesión no enseña contenido nuevo: cierra la Unidad II de **Big Data**. El sílabo (sesión 12) define dos actividades para esta evaluación:

1. Resolver la evaluación teórico-práctica de los temas de la Unidad II (sesiones 6 a 11).
2. Presentar y sustentar el Pipeline en tiempo real con ingesta, streaming, observabilidad y salidas BI/ML distribuidas.

**Esta sesión no repite lo evaluado en S5**: la Unidad I (arquitectura, ETL batch, calidad, particionamiento y primer modelo) ya quedó certificada; S12 evalúa exclusivamente lo que se construyó encima en S6-S11.

## 2. Producto evaluado

Del sílabo, el producto de la Unidad II es:

> Pipeline Big Data en tiempo real que procesa eventos, produce resultados analíticos, expone métricas operativas y prepara resultados para BI/ML distribuido.

El producto completo — plantilla-ejemplo con el ejemplo de referencia del docente — vive en [`u2-producto.md`](../proyecto-sello/u2-producto.md): ingesta de eventos empresariales e IoT, procesamiento streaming, observabilidad y costos, series de tiempo, BI/ML distribuido. La estructura es exigible a todos; el contenido se reemplaza por el del propio instrumento de cada equipo.

Lo que sustentas en S12 es **tu propia dimensión U2**: las capacidades que tú construiste en S6-S11, sobre el instrumento de tu Proyecto Sello. `u2-producto.md` (Tabla 1) muestra cómo cada sesión se ensambla en el pipeline terminado.

## 3. Evaluación teórico-práctica (S6-S11)

Cubre los seis temas dictados antes de esta sesión. El docente puede tomarla escrita, oral o mixta.

**Tabla 1. Temario de la evaluación teórico-práctica**

| Sesión | Tema | Qué puede evaluar el docente |
|---|---|---|
| S6 | Ingesta de eventos empresariales en tiempo real | Conceptos de Kafka (*topic*, *broker*, partición, *offset*, *consumer group*, *key*), publicación y consumo, contrato de evento versionable. |
| S7 | Ingesta de eventos IoT/sensores en tiempo real | Diferencia de frecuencia/volumen frente a un evento de negocio (S6), particionado por dispositivo (*key*) verificado en Kafka UI, validación de esquema y de rango físico, y por qué un dispositivo real publica por MQTT hacia un puente, nunca hablando Kafka directo. |
| S8 | Procesamiento en streaming con Spark: ventanas, watermarking y semántica de entrega | Ventanas fijas y deslizantes, tratamiento de datos tardíos con *watermarking*, *checkpointing*, semántica "al menos una vez" y deduplicación, y su efecto en latencia y throughput. |
| S9 | Observabilidad con Grafana y costos | Métricas de streaming (disponibilidad, latencia, throughput, *lag* del consumidor), cómo se conectan Prometheus/Grafana a Kafka y Spark, umbrales de alerta, riesgos operativos y estimación de costos de escalado. |
| S10 | Series de tiempo e inferencia en streaming | Comparación contra un *baseline* de persistencia (por qué compararlo primero), división cronológica de entrenamiento/prueba (nunca aleatoria), e inferencia en tiempo real sin estado: una sola lectura basta, sin mantener historial en memoria. |
| S11 | BI/ML distribuido con Spark: KPIs y visualización de la predicción | Diferencia entre un KPI de negocio y una métrica de infraestructura o de aplicación, *Pushgateway* como atajo pragmático para un *job* que no termina (no la forma recomendada), error de predicción medido en vivo en producción, y un tablero con un panel batch (estático) y uno streaming (en vivo) lado a lado. |

Preguntas de referencia (el docente puede formular equivalentes):

1. ¿Qué diferencia hay entre el contrato de un evento empresarial y uno de un sensor IoT, y por qué esa diferencia importa al diseñar el tópico de Kafka?
2. Si un evento llega tarde a tu ventana de agregación, ¿qué hace el *watermarking* con él, y qué pasaría si no lo tuvieras configurado?
3. ¿Qué métrica de tu panel de observabilidad te alertaría primero si el pipeline empieza a acumular retraso (*backpressure*), y por qué esa y no otra?
4. ¿Por qué estimar el costo de escalar tu pipeline streaming no es lo mismo que estimar el costo de tu pipeline batch de Unidad 1?
5. ¿Qué gana tu tablero BI al mostrar la predicción de series de tiempo junto a los KPIs del negocio, en vez de mostrarlos por separado?
6. ¿Por qué tu modelo de series de tiempo se compara primero contra un baseline de persistencia, y qué significaría que tu modelo no le gane?
7. ¿Qué diferencia hay entre un KPI de negocio y una métrica de infraestructura o de aplicación, y por qué Pushgateway es un atajo pragmático —no la forma recomendada— para exponer la de un job que no termina?

## 4. Sustentación de tu dimensión U2

**Tabla 3. Distribución de tiempo de la sustentación**

| Momento | Tiempo | Quién | Propósito |
|---|---:|---|---|
| Alcance del proyecto (diapositiva) | 2-3 min | Grupal | Pregunta central de negocio (`brief.md`, sección 2), para quién, y su relación con el Proyecto Sello. |
| Arquitectura (diapositiva) | 2-3 min | Grupal | Componentes reales del pipeline (ingesta Kafka empresarial/IoT, Spark streaming, Grafana) — diagrama simple: fuentes → Kafka → Spark → Grafana/BI. |
| Asignación de tareas (diapositiva) | 2-3 min | Grupal | Asignación por dimensión de negocio: cada integrante presenta sus dos dimensiones de la pregunta central (U1 batch y U2 streaming, `brief.md` sección 3), no un módulo aparte. |
| Demo y sustentación | 3 min por integrante | Individual | Cada integrante ejecuta en vivo su propia dimensión U2 (streaming) en el tablero Grafana compartido, y la contrasta con su dimensión U1. |
| Preguntas | 3 min por integrante | Individual | Verificar dominio y aporte propio, con base en la Tabla 1. |

**Tabla 4. Entregables obligatorios**

| Entregable | Evidencia mínima | Criterio de aceptación |
|---|---|---|
| Producto de tu dimensión U2 | [`u2-producto.md`](../proyecto-sello/u2-producto.md), con tu propio instrumento | Coherente con el sílabo y con el pipeline real ejecutable |
| Evidencia de ingesta y streaming | Eventos empresariales e IoT publicados/consumidos en vivo; procesamiento con ventana y *watermarking* verificado | Trazabilidad verificable en logs, no solo documentada |
| Evidencia de observabilidad y costos | Panel operativo con métricas correlacionadas; estimación de costos justificada | Verificable en vivo, no solo descrita |
| Evidencia de BI/ML | Serie de tiempo o inferencia validada, integrada al tablero BI con KPIs | Métricas reales reportadas |
| Sustentación individual | Preguntas y defensa por integrante (sección 3) | Autoría demostrada |

Secuencia sugerida de presentación (referencias a secciones de `u2-producto.md`):

1. Presentar cómo evolucionó tu pipeline desde la Unidad I.
2. Publicar y consumir en vivo un evento empresarial y uno IoT/sensor.
3. Mostrar el procesamiento streaming con ventana, *watermarking* y *checkpointing* activos.
4. Mostrar el panel de observabilidad y la estimación de costos de escalado.
5. Mostrar el modelo o inferencia de series de tiempo y sus métricas de validación.
6. Mostrar el tablero BI con los KPIs y la predicción integrados.
7. Cerrar explicando al menos una decisión propia distinta al ejemplo del docente.

Criterios mínimos de aceptación:

- Al menos un evento empresarial y uno IoT/sensor se publican y consumen en vivo por Kafka.
- El procesamiento streaming aplica ventana, *watermarking* y *checkpointing*, verificable en logs.
- El panel de observabilidad correlaciona métricas reales del pipeline, con costos de escalado estimados y justificados.
- El modelo o inferencia de series de tiempo está validado con métricas, no solo entrenado.
- El tablero BI muestra la predicción junto a los KPIs del negocio, no por separado.
- Respondes individualmente al menos una pregunta de la Tabla 1.

## 5. Rúbrica de evaluación

La rúbrica (8 criterios: 7 cita literal de los criterios de evaluación del producto de la Unidad II en el sílabo de Big Data + sustentación) vive en [`u2-producto.md`](../proyecto-sello/u2-producto.md#2-rubrica-de-evaluacion), junto con la plantilla del producto y su trazabilidad con la malla curricular (segunda mitad de CE043 Nivel 3 + CE044 Nivel 3 completo). Úsala directamente desde ahí para calificar la sustentación de esta sesión — no se duplica aquí.
