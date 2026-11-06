# S16 - Continuación de la Evaluación de la Unidad III

## 1. Propósito de la sesión

Esta sesión no agrega contenido ni rúbrica nueva: es la continuación de [S15](S15_Evaluacion_Unidad_3.md), reservada por el sílabo (sesión 16) para dos situaciones, no una sola:

1. Completar la evaluación final individual o las presentaciones y sustentaciones que no alcanzaron a realizarse en S15, mediante demostración en vivo, preguntas técnicas y revisión de evidencias, con el mismo rigor que S15.
2. Para los equipos que ya sustentaron en S15: entregar la versión final del producto, las evidencias y la documentación, incorporando las observaciones del jurado, y cerrar el curso con una reflexión sobre aprendizajes, limitaciones y mejoras futuras del sistema.

No hay un segundo producto ni una segunda rúbrica: todo lo evaluado aquí sigue siendo el mismo [`u3-producto.md`](../proyecto-sello/u3-producto.md) de S15, con la misma rúbrica.

## 2. Para equipos o integrantes pendientes de sustentar

Repite exactamente la secuencia de la sección 4 de [S15](S15_Evaluacion_Unidad_3.md#4-sustentacion-del-producto-final) — video pitch o introducción, exposición técnica, demostración end-to-end y preguntas individuales — y responde a la evaluación teórico-práctica de la sección 3 de esa misma guía, sobre S13-S14.

**Criterios mínimos de aceptación:** los mismos de S15, sección 4. No hay una versión reducida de la exigencia por sustentar una semana después. Antes de presentar, levanta el sistema desde una copia limpia del repositorio con `docker compose up -d --wait` y repite la ejecución integrada de S13: la demostración en vivo depende de que los servicios estén listos y de que las compuertas pasen.

## 3. Para equipos que ya sustentaron en S15

El sílabo pide actualizar la entrega final con lo que el jurado observó. La entrega final no es una versión nueva del producto: es **la misma entrega, corregida**, con la evidencia de que cada observación se atendió.

**Tabla 1. Cierre del producto final**

| Entregable | Qué revisar antes de la entrega final |
|---|---|
| `u3-producto.md` | Observaciones del jurado de S15 incorporadas, con evidencia actualizada si algún comando o prueba se volvió a ejecutar. |
| Ejecución integrada | La de S13, repetida tras los cambios posteriores a S15 y comparada con la anterior (esperable: los valores de streaming varían; regresión: una compuerta que deja de pasar). |
| Revisión y bitácora | La lista de comprobación de configuración y la bitácora de S14 (3.2, 3.10), con una fila nueva por cada hallazgo corregido después de S15 y su prueba de corrección. |
| Riesgos aceptados | La tabla de S14 (3.6) al día: cada riesgo con su justificación y lo que cambiaría en producción. |
| Guion de demostración | El de S14 (3.8), ajustado a lo que pasó realmente en la sustentación: qué paso falló, qué plan alternativo se usó. |
| Documentación (MkDocs o equivalente) | Publicada, navegable y alineada con el producto final — el mismo criterio del subaspecto de documentación de la sustentación ([`u3-producto.md`, Tabla 2](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion)). |
| Repositorio | Topics académicos vigentes, sin archivos que no deben versionarse (por ejemplo `artifacts/` o `.env`), historial de commits coherente con las evidencias; verificado sobre el **remoto**, no sobre tu laptop. |

**Error frecuente**: entregar la versión final sin volver a clonarla. Un cambio hecho después de S15 puede haber dejado la documentación describiendo un estado que el sistema ya no cumple.

## 4. Reflexión final individual

Con esta sesión cierra el curso, y el sílabo pide reflexionar sobre aprendizajes, limitaciones y mejoras futuras del sistema. Se entrega como una página adicional del informe final, de 12 a 20 líneas, con estas tres partes — cada una anclada en algo que hiciste, no en una declaración general:

1. **Aprendizajes.** Dos o tres cosas que ahora sabes hacer y al inicio del curso no. Para cada una, nombra la sesión y la evidencia concreta (una celda que ejecutaste, una métrica que interpretas, un hallazgo que corregiste).
2. **Limitaciones.** Qué parte de tu sistema no resiste una condición real (volumen, falla de un servicio, datos tardíos, seguridad) y cómo lo sabes: con qué prueba o medición lo comprobaste, o por qué no pudiste comprobarlo.
3. **Mejoras futuras.** Una acción concreta con una semana más de trabajo, y qué métrica de tu sistema debería cambiar si funciona. Una mejora sin acción ni métrica no cuenta.

Este texto es un criterio mínimo de aceptación (pasa o no pasa), no una fila de la rúbrica: la rúbrica de [`u3-producto.md`](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion) califica el producto y su defensa.

## 5. Rúbrica de evaluación

La misma rúbrica de [S15](S15_Evaluacion_Unidad_3.md#5-rubrica-de-evaluacion), en [`u3-producto.md`](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion) — no se duplica ni se reemplaza aquí. Para un equipo que sustenta recién en esta sesión, la nota se calcula exactamente igual que si hubiera sustentado en S15.

## 6. Cierre académico

Con esta sesión cierra **Big Data**: la arquitectura y el procesamiento batch de la Unidad I; la ingesta, el streaming, la observabilidad y el BI/ML de la Unidad II; y la integración, el DataOps y la revisión técnica de la Unidad III quedan entregados como un solo sistema — ejecutable de extremo a extremo desde el repositorio, comprobable con sus compuertas y métricas, y defendido con sus decisiones y lo que cada una cuesta.
