# S15 - Evaluación de la Unidad III

## 1. Propósito de la evaluación

Esta sesión no enseña contenido nuevo: cierra la Unidad III de **Big Data** y, con ella, el curso completo. El sílabo (sesión 15) define dos actividades para esta evaluación:

1. Resolver la evaluación teórico-práctica de los temas de la Unidad III.
2. Presentar y sustentar el Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML y visualización BI.

**Esta sesión no repite lo evaluado en S5 ni en S12**: la Unidad I (batch) y la Unidad II (tiempo real) ya quedaron certificadas por separado; S15 evalúa la integración, DataOps y estabilización final de ambas juntas — el sistema completo, no una capacidad nueva.

**S16 (Continuación de la evaluación de la Unidad III)** no es una evaluación distinta ni una rúbrica nueva. Sirve a dos situaciones: completar esta misma evaluación para los equipos o integrantes cuya presentación, sustentación o evidencia quedó pendiente, y, para quienes ya sustentaron, entregar la versión final del producto con las observaciones incorporadas y una reflexión individual de cierre.

## 2. Producto evaluado

Del sílabo, el producto de la Unidad III es:

> Producto final que integra pipelines batch y streaming, observabilidad, analítica/ML, visualización BI y documentación operativa para una demo end-to-end defendida técnicamente.

El producto completo — plantilla-ejemplo con los componentes mínimos, rúbrica y plantillas de documentación — vive en [`u3-producto.md`](../proyecto-sello/u3-producto.md). La estructura es exigible a todos.

**Nota de alcance:** según el sílabo, Unidad 3 se llama "Integración, DataOps y despliegue del sistema final" — el producto de Unidad 3 **es** el Producto Final del curso, no una entrega distinta ni adicional.

## 3. Evaluación teórico-práctica (S13-S14)

Cubre la integración y el hardening final dictados antes de esta sesión. El docente puede tomarla escrita, oral o mixta.

**Tabla 1. Temario de la evaluación teórico-práctica**

| Sesión | Tema | Qué puede evaluar el docente |
|---|---|---|
| S13 | Integración del sistema, DataOps y BI | Cómo se integran batch, streaming, modelo y observabilidad en una sola ejecución; para qué sirven las compuertas previas y de calidad; por qué se publican percentiles (p50/p95) y no el promedio; qué registra el manifiesto de ejecución; y por qué un valor viejo en Prometheus puede parecer vivo. |
| S14 | Revisión técnica final y hardening | Qué diferencia hay entre un contenedor que arrancó y uno que está listo (`healthcheck`, `depends_on` con `service_healthy`, `up -d --wait`); por qué las credenciales se leen de variables; para qué sirve una prueba de no regresión; y qué distingue un riesgo aceptado de un descuido. |

Preguntas de referencia (el docente puede formular equivalentes):

1. ¿Qué componente de tu sistema (batch, streaming, observabilidad o BI/ML) fue el más difícil de integrar con los demás, y por qué?
2. ¿Qué práctica de DataOps/DevOps aplicaste al empaquetado o despliegue de tu sistema, y qué problema real resuelve?
3. ¿Qué tuviste que estabilizar entre la Unidad II y esta evaluación para que el sistema fuera reproducible por otra persona?
4. Si otra persona clona tu repositorio hoy, ¿qué pasos exactos necesita seguir para levantar el sistema completo, y dónde están documentados?
5. ¿Qué limitación real del producto reconoces, y por qué no la resolviste dentro del alcance del curso?
6. En tu pipeline, ¿qué compuerta detiene la ejecución y cuál solo advierte? ¿Por qué elegiste esa diferencia?
7. ¿Por qué el promedio de latencia no representa a tus eventos y qué métrica usas en su lugar?
8. ¿Qué diferencia hay entre un servicio en ejecución y uno listo, y qué configuración de tu sistema lo expresa?
9. Después de un cambio de configuración, ¿qué prueba repetiste para saber que no rompiste nada, y qué diferencias entre antes y después serían esperables?
10. Elige un riesgo que aceptaste en tu sistema: ¿cuál es su justificación y qué cambiarías en producción?
11. En tu matriz de trazabilidad, ¿qué fila tiene la evidencia más débil, y qué harías para fortalecerla?
12. Defiende una decisión técnica propia: ¿qué ganas con ella y qué te cuesta?

## 4. Sustentación del producto final

**Tabla 2. Distribución de tiempo por integrante**

| Momento | Tiempo | Propósito |
|---|---:|---|
| Video pitch / introducción ejecutiva | 3 min | Presentar problema, solución, valor del producto y participación del equipo. |
| Exposición técnica | 10 min | Presentar problema, matriz de trazabilidad, arquitectura, fuentes, pipelines, observabilidad, resultados y valor analítico ([Tabla 3 de `u3-producto.md`](../proyecto-sello/u3-producto.md#4-secuencia-sugerida-de-presentacion)). |
| Demostración end-to-end | 5 min | Ejecutar o evidenciar el flujo batch/streaming completo, salidas generadas, métricas y resultados BI/ML. |
| Preguntas individuales | 5 min | Verificar dominio y aporte propio, con base en la Tabla 1 y el producto completo. |

**Tabla 3. Entregables obligatorios**

| Entregable | Evidencia mínima | Criterio de aceptación |
|---|---|---|
| Producto final | [`u3-producto.md`](../proyecto-sello/u3-producto.md), adaptado al instrumento propio del equipo | Integra y estabiliza lo construido en Unidad 1 y Unidad 2 |
| Matriz de trazabilidad | Variable y pregunta central, dimensión de cada integrante, indicadores U1 y U2, fuente y técnica, y evidencia ([Tabla 3 de `u3-producto.md`](../proyecto-sello/u3-producto.md#4-secuencia-sugerida-de-presentacion)) | Una fila por indicador y por integrante, cada una con su evidencia verificable |
| Evidencia de integración end-to-end | Flujo batch, streaming y BI/ML ejecutados juntos, con al menos un caso de error | Verificable en vivo, no solo documentada |
| Evidencia de DataOps y hardening | Empaquetado/despliegue reproducible; checklist de hardening completado | Reproducible por otra persona desde el repositorio |
| Repositorio y documentación | Topics académicos vigentes, documentación completa publicada en MkDocs o equivalente, informe del proyecto | Reproducible sin ayuda del equipo |
| Sustentación grupal con aporte individual | Video pitch + exposición + demo + defensa por integrante, con los 7 subaspectos de [`u3-producto.md`, Tabla 2](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion) | Aporte individual verificable dentro de la defensa grupal, conectado a valor para la toma de decisiones |

Criterios mínimos de aceptación:

- El sistema completo (Unidad 1 + Unidad 2) se ejecuta de extremo a extremo desde el repositorio, sin pasos no documentados.
- Los tres flujos (batch, streaming, BI/ML) se muestran integrados, no como componentes aislados.
- Al menos una práctica de DataOps/DevOps es verificable (empaquetado, script de despliegue, CI).
- La documentación (MkDocs o equivalente) permite a otra persona reproducir el sistema sin ayuda del equipo.
- El repositorio mantiene los topics académicos y evidencia de commits a lo largo del curso.
- La matriz de trazabilidad relaciona la variable con la dimensión y los indicadores U1 y U2 de cada integrante, y cada fila tiene su evidencia.
- Lo común al grupo (arquitectura, DataOps, hardening, demo y repositorio) se presenta una sola vez; cada integrante responde individualmente sobre ello.
- La sustentación cubre los 7 subaspectos de la sustentación integral (`u3-producto.md`, Tabla 2), conectando resultados con valor real para la toma de decisiones.
- Cada integrante responde individualmente al menos una pregunta de la Tabla 1.

## 5. Rúbrica de evaluación

La rúbrica (6 criterios, cita literal de los criterios de evaluación del producto de la Unidad III en el sílabo de Big Data — el propio criterio 6 ya incluye la sustentación técnica con evidencias, métricas y valor para decisiones, por eso no se agrega un criterio de sustentación aparte) vive en [`u3-producto.md`](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion), junto con la plantilla del producto, los subaspectos de sustentación y su trazabilidad con la malla curricular (CE042, CE043 y CE044, Nivel 3). Úsala directamente desde ahí para calificar la sustentación de esta sesión — no se duplica aquí.
