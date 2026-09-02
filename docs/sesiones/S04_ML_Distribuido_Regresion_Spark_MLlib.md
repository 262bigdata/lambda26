# S4 - ML Distribuido con Spark MLlib (Regresión)

## 1. Introducción

### 1.1 Presentación de la sesión

S3 cerró la base técnica del pipeline batch: una salida analítica particionada, con esquema validado, nulos tratados y duplicados resueltos. Hasta ahí, todo el trabajo transformaba y verificaba datos — ninguna sesión generaba todavía una predicción a partir de ellos. Esta sesión construye ese Data Lake sobre un segundo caso real (tres fuentes de sensores que hay que integrar antes de tratarlas) y entrena el primer modelo de regresión distribuida con Spark sobre esa salida, comparando configuraciones básicas antes de reportar métricas.

El porqué de comparar configuraciones en vez de conformarse con entrenar un único modelo se desarrolla en 1.6, a partir de un caso real. Esta sesión no busca el mejor modelo posible — ajustar hiperparámetros a fondo excede el alcance de un primer contacto — ni construye pronósticos con historial temporal: eso es contenido de S10.

### 1.2 Índice

1. Preparación de datos: integración y calidad (Bronze, Silver, Gold).
2. Preparación de datos para modelado distribuido.
3. Entrenamiento de un modelo de regresión distribuida.
4. Evaluación con métricas de regresión.
5. Comparación de configuraciones y algoritmos.

### 1.3 Propósito de aprendizaje

Al concluir la clase, estarás en condiciones de:

- **Integrar, validar y particionar** datos provenientes de múltiples fuentes en un Data Lake analítico, y **entrenar y comparar** modelos de regresión distribuida sobre esa salida, reportando métricas iniciales que permitan decidir si un modelo mejora sobre otro, sin necesidad de ajustar hiperparámetros de forma exhaustiva.

### 1.4 Producto de sesión

Notebook de Jupyter con: tres fuentes reales integradas por una clave común, con esquema explícito, duplicados resueltos (`Window`+`row_number()`), un código de error distinguido de un nulo, y una salida particionada en Parquet (Bronze → Silver → Gold); vector de predictores ensamblado (`VectorAssembler`) sobre esa salida; modelo base de regresión lineal entrenado (`LinearRegression`) y evaluado (`RegressionEvaluator`: RMSE, R², MAE); comparación de al menos tres configuraciones de regularización (`regParam`, `elasticNetParam`); comparación con un segundo algoritmo (`RandomForestRegressor`), incluida la importancia de cada variable; modelo seleccionado guardado (`model.write().overwrite().save(...)`).

### 1.5 Metodología

**Tabla 1. Metodología de la sesión**

| Actividades a Realizar en el Periodo | Orientaciones generales (Orientaciones Metodológicas) | Material de estudio recomendado |
|---|---|---|
| Revisión previa individual | Repasar las técnicas de calidad de datos de S3 (esquema, nulos, duplicados, particionado) — confirmar que el entorno `lambda26` sigue funcionando. | Guía S3. |
| Clase presencial | Construcción guiada del notebook `04_ml_distribuido_regresion_practica.ipynb`: integración de tres fuentes en un Data Lake analítico, entrenamiento de un modelo base, evaluación con métricas de regresión, comparación de configuraciones y de un segundo algoritmo. Trabajo individual, siguiendo al docente paso a paso; consulta inmediata ante dudas. | Pasos 3.1 a 3.19 de esta guía. |
| Evaluación formativa | Revisión en clase de la salida particionada, del modelo entrenado y de la tabla comparativa de configuraciones. La evidencia se completa y sustenta de forma individual, fuera del aula, según los criterios mínimos de la sección 4.4. | Indicaciones de entrega (4.3), rúbrica de evaluación (4.6). |

### 1.6 Motivación de la sesión

#### 1.6.1 Caso: el algoritmo que le costó a Zillow 2000 empleos

El 2 de noviembre de 2021, Zillow —la plataforma inmobiliaria más grande de Estados Unidos— anunció el cierre total de Zillow Offers, su negocio de compra automatizada de casas (*iBuying*). El modelo de negocio dependía de una sola variable crítica: la capacidad de su algoritmo de precios (basado en el *Zestimate*) para predecir, con precisión de pocos puntos porcentuales, el valor de una casa entre 3 y 6 meses hacia adelante — el tiempo que Zillow tardaba en comprar, remodelar y revender cada propiedad.

El algoritmo sobreestimó sistemáticamente el valor de miles de casas. Zillow pagó por ellas más de lo que el mercado, ya desacelerándose, estaba dispuesto a pagar de vuelta. El resultado: un castigo contable de **304 millones de dólares** en inventario ese mismo trimestre, y el despido de aproximadamente el **25% de la plantilla de la empresa** (más de 2000 personas). Un modelo de regresión entrenado y desplegado a escala, sin margen para volatilidad de mercado ni un proceso que comparara su desempeño contra alternativas antes de comprometer capital real.

Fuente: incidentdatabase.ai. (2021). *Incident 149: Zillow Shut Down Zillow Offers Division Allegedly Due to Predictive Pricing Tool's Insufficient Accuracy*. https://incidentdatabase.ai/cite/149/

No fue que Zillow usara un modelo "malo" en un sentido técnico simple — fue que confió en un único modelo, entrenado sobre condiciones de mercado que ya habían cambiado, sin un proceso que lo comparara sistemáticamente contra configuraciones alternativas ni midiera qué tan lejos podía estar su error antes de arriesgar dinero real en cada predicción. Esta sesión formaliza justamente ese hábito: ningún modelo se reporta ni se guarda sin antes compararlo, con métricas explícitas, contra al menos otra configuración y otro algoritmo.

**Preguntas de análisis**

**Activación de conocimientos previos**

1. Antes de leer la causa, ¿qué riesgo ves en construir un negocio completo sobre una única predicción numérica, sin ningún mecanismo de comparación o corrección?

**Comprensión de la comparación de modelos**

1. Según el caso, ¿qué hubiera cambiado si Zillow hubiera comparado su modelo de precios contra configuraciones alternativas, y hubiera medido explícitamente su margen de error antes de comprar cada casa?
2. ¿Por qué reportar una sola métrica (por ejemplo, solo R²) puede ocultar un problema que sí aparece en otra métrica (por ejemplo, RMSE)? Relaciónalo con lo que vas a comparar en 3.16-3.17.

### 1.7 Ubicación en el curso

- Unidad: U1 - Arquitecturas Big Data y ETL batch distribuido.
- Producto del curso: Proyecto Sello: sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones.
- Producto de unidad: pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML.
- Avance del producto en esta sesión: Data Lake analítico construido desde tres fuentes reales, con un modelo de regresión distribuida entrenado y comparado sobre esa salida, con métricas iniciales reportadas.

**Figura 1. Roadmap del producto de la unidad U1**

```mermaid
flowchart TB
    Arquitectura["Arquitectura Big Data<br/>Lambda o Kappa<br/>S1"]
    PySpark["Fundamentos PySpark<br/>S2"]
    HDFS["Formatos analíticos y calidad<br/>S3"]
    ML["Data Lake + ML distribuido<br/>HOY"]
    Evaluacion["Evaluación U1<br/>S5"]

    Arquitectura --> PySpark --> HDFS --> ML --> Evaluacion

    classDef today fill:#ffe08a,stroke:#9a6b00,stroke-width:2px,color:#111;
    class ML today;
```

Hoy se cierra el último eslabón que la evaluación de unidad (S5) necesita: sin un Data Lake propio, construido y validado, y sin un modelo entrenado y comparado sobre él, S5 no tendría nada de "ML distribuido" que sustentar como parte del pipeline batch completo.

## 2. Explica

Tiempo: 35 min.

### 2.1 Arquitectura de la sesión

**Figura 2. De tres fuentes crudas al modelo comparado**

```mermaid
flowchart LR
    Fuentes["Tres fuentes reales<br/>(sensores)"] --> Gold["Data Lake Gold<br/>integrado, validado, particionado (2.2)"]
    Gold --> Prep["Preparar y entrenar<br/>VectorAssembler + LinearRegression base (2.3-2.4)"]
    Prep --> Eval["Evaluar<br/>RMSE, R², MAE (2.5)"]
    Eval --> Comp["Comparar configuraciones<br/>y algoritmos (2.6)"]
    Comp --> Guardar["Guardar el modelo<br/>seleccionado"]

    classDef today fill:#ffe08a,stroke:#9a6b00,stroke-width:2px,color:#111;
    class Gold,Prep,Eval,Comp,Guardar today;
```

Cada bloque de esta cadena es un paso necesario, no opcional: sin datos íntegros y de calidad no hay Data Lake confiable; sin `VectorAssembler` no hay entrada válida para MLlib; sin evaluación explícita no hay forma de saber si el modelo sirve; sin comparación no hay manera de distinguir un modelo confiable de uno que solo parece funcionar (el caso de 1.6).

**CRISP-DM** (*Cross-Industry Standard Process for Data Mining*) es el proceso estándar, independiente de cualquier proyecto o herramienta, para organizar un trabajo de minería de datos o aprendizaje automático en seis fases: Business Understanding, Data Understanding, Data Preparation, Modeling, Evaluation y Deployment. No es exclusivo de Spark ni de esta sesión — es el mismo marco que ordenaría un proyecto de ML hecho enteramente en pandas y scikit-learn.

Esta sesión sigue ese mismo orden. Business Understanding ya se resolvió en 1.6 (el caso Zillow: por qué no basta con un único modelo sin comparación). Las cinco fases restantes están marcadas de forma explícita en la sección 3, sobre los pasos 3.1 a 3.19 — con una simplificación deliberada frente a un pipeline de investigación completo: acá no hay una fase de validación interna separada de la prueba final, así que Modeling y Evaluation avanzan juntas (ver la nota antes de 3.14).

### 2.2 Preparación de datos: integración y calidad (Bronze, Silver, Gold)

La **integración de datos** es el proceso de combinar información proveniente de distintos sistemas de origen —cada uno con su propia estructura, calidad y ritmo de actualización— en una sola fuente confiable para análisis. Es distinta de simplemente "cargar un dataset": antes de combinar dos o más fuentes hay que resolver los problemas de cada una por separado (esquema, duplicados, valores fuera de dominio), porque un problema sin resolver en una fuente no desaparece al integrarla — se propaga, y a veces se multiplica, al resto del resultado. Un caso típico, fuera de este curso: un sistema que junta ventas de un CRM y pedidos de un ERP, cada uno con su propio formato de fecha y sus propios duplicados — si el duplicado del CRM no se resuelve antes de unirlo con el ERP, el resultado final duplica pedidos que nunca estuvieron duplicados en ninguna de las dos fuentes por separado.

S3 (H&M) parte de una sola fuente ya lista, así que ese problema no aparecía todavía. El dataset de hoy sí lo tiene, desde el principio: varias fuentes reales que hay que integrar antes de aplicar esquema, nulos y duplicados — la mecánica de cada control de calidad ya la viste en S3 (esquema explícito, `Window`+`row_number()`, particionado, 2.2-2.4 de esa sesión); lo nuevo hoy es aplicarla sobre más de una fuente a la vez, y distinguir un nulo de un código de error de sensor, que no es lo mismo (3.6).

Esta sesión organiza su salida en las mismas capas que ya viste en S3 (2.6): Bronze (crudo), Silver (integrado y validado), Gold (particionado, listo para consumo) — con una diferencia real frente a H&M: hoy no hay una columna categórica ya presente en los datos para particionar, así que se **deriva** una a partir de una fecha (3.9) — patrón igual de común en almacenamiento analítico real, basado en el mismo criterio de siempre (pocos valores distintos, usados seguido en filtros).

### 2.3 Preparación de datos para modelado distribuido

A diferencia de librerías como scikit-learn, donde un modelo acepta directamente una matriz de columnas (`X`), Spark MLlib exige que todos los predictores estén combinados en una **sola columna vectorial**. `VectorAssembler` hace esa combinación:

```python
from pyspark.ml.feature import VectorAssembler

ensamblador = VectorAssembler(inputCols=PREDICTORES, outputCol="features")
df_ml = ensamblador.transform(df).select("features", "Valor_Objetivo")
```

Esta diferencia no es una peculiaridad arbitraria de Spark: en un motor distribuido, cada fila se procesa de forma independiente en un nodo distinto — tener un único objeto `Vector` por fila simplifica cómo Spark distribuye y serializa esa fila entre nodos, en vez de coordinar N columnas sueltas por separado.

La columna objetivo (`label`, la variable que el modelo debe predecir) **no** entra al ensamblador — es un dato de salida, no de entrada. Hoy esa columna es `Valor_CE` (3.12).

**Tabla 2. Misma tabla, dos preguntas distintas: regresión (hoy) y series de tiempo (S10)**

| | Esta sesión (S4) — Regresión | S10 — Series de tiempo |
|---|---|---|
| Pregunta de fondo | ¿Qué otras variables explican `Valor_Objetivo`? | ¿El pasado de `Valor_Objetivo` predice su futuro? |
| Predictores | Las otras variables, en el mismo instante `t` | `Valor_Objetivo` en instantes anteriores (`t`, `t-1`, ...) |
| Objetivo | `Valor_Objetivo` en ese mismo instante `t` | `Valor_Objetivo` en un instante futuro (`t+1`) |
| Orden de las filas | Irrelevante — cada fila es independiente | Crítico — el orden cronológico es el dato |
| División entrenamiento/prueba | Aleatoria (`randomSplit`) | Cronológica (equivalente a `TimeSeriesSplit`) |
| Riesgo si se usa la división del otro caso | Ninguno | Fuga de información: el modelo "vería" el futuro al entrenar |

```python
df_train, df_test = df_ml.randomSplit([0.8, 0.2], seed=42)
```

`seed=42` fija la aleatoriedad — sin ella, cada corrida dividiría los datos distinto, y los resultados no serían comparables entre configuraciones (2.6). Ninguna de las dos preguntas de la Tabla 2 es más difícil de construir en Spark que la otra — la diferencia está en qué significa una fila y qué se le puede hacer a su orden, no en la complejidad del código.

### 2.4 Entrenamiento de un modelo de regresión distribuida

Todo estimador de Spark MLlib sigue el mismo patrón de dos pasos: `fit()` entrena sobre los datos de entrenamiento y devuelve un **modelo** (un `Transformer`); `transform()` aplica ese modelo entrenado sobre datos nuevos para producir predicciones. Es el mismo patrón que reaparecerá en cualquier otro algoritmo de MLlib, no solo en regresión.

Estimar una variable numérica continua a partir de otras variables medidas en el mismo instante es uno de los tipos de problema más comunes en aprendizaje automático, con o sin Spark de por medio. El ejemplo más conocido, fuera de este curso, es la competencia *House Prices* de Kaggle: predecir el precio de venta de una casa a partir de sus características (área, año de construcción, barrio, calidad de materiales, entre otras). La estructura es idéntica a la de hoy — varios predictores numéricos y categóricos, un objetivo numérico continuo, sin ningún componente temporal — solo que ahí la variable a explicar es el precio de una casa, y aquí es `Valor_CE`.

```python
from pyspark.ml.regression import LinearRegression

lr_base = LinearRegression(featuresCol="features", labelCol="Valor_Objetivo")
modelo_base = lr_base.fit(df_train)

predicciones_base = modelo_base.transform(df_test)
```

`modelo_base.coefficients` y `modelo_base.intercept` exponen la ecuación aprendida — útil para verificar rápido si el signo de cada coeficiente coincide con lo que ya sabes del dominio (por ejemplo, si una variable debería aumentar o disminuir el objetivo, según el conocimiento previo del problema).

### 2.5 Evaluación con métricas de regresión

Ninguna métrica sola cuenta toda la historia — el caso de 1.6 es, en el fondo, un caso de confiar en una sola señal de desempeño:

**Tabla 3. RMSE, R² y MAE**

| Métrica | Qué mide | Sensible a errores grandes | Unidad |
|---|---|---|---|
| RMSE (raíz del error cuadrático medio) | Qué tan lejos, en promedio, cae la predicción del valor real, penalizando fuerte los errores grandes. | Sí — un solo error enorme infla el RMSE mucho más que varios errores moderados. | La misma que la variable objetivo (`Valor_Objetivo`). |
| MAE (error absoluto medio) | Qué tan lejos, en promedio, cae la predicción, sin penalizar extra los errores grandes. | No — cada error pesa lo mismo, grande o chico. | La misma que la variable objetivo. |
| R² (coeficiente de determinación) | Qué proporción de la variabilidad de `Valor_Objetivo` explica el modelo, entre 0 y 1 (puede ser negativo si el modelo es peor que predecir siempre el promedio). | No aplica — es una proporción, no un error. | Sin unidad (proporción). |

```python
from pyspark.ml.evaluation import RegressionEvaluator

evaluador_rmse = RegressionEvaluator(labelCol="Valor_Objetivo", predictionCol="prediction", metricName="rmse")
evaluador_r2 = RegressionEvaluator(labelCol="Valor_Objetivo", predictionCol="prediction", metricName="r2")
evaluador_mae = RegressionEvaluator(labelCol="Valor_Objetivo", predictionCol="prediction", metricName="mae")

print(f"RMSE={evaluador_rmse.evaluate(predicciones_base):.4f}")
print(f"R2={evaluador_r2.evaluate(predicciones_base):.4f}")
print(f"MAE={evaluador_mae.evaluate(predicciones_base):.4f}")
```

Que RMSE y MAE difieran bastante entre sí es, por sí solo, una señal: significa que hay al menos algunos errores grandes que MAE está "escondiendo" en el promedio.

### 2.6 Comparación de configuraciones y algoritmos

`regParam` controla cuánto se penaliza la magnitud de los coeficientes — un valor alto reduce el riesgo de que el modelo memorice ruido específico del conjunto de entrenamiento (*overfitting*), a costa de un ajuste menos preciso. `elasticNetParam` mezcla dos formas de esa penalización: `0.0` es Ridge (penalización L2, reduce coeficientes sin llevarlos nunca a cero), `1.0` es Lasso (penalización L1, puede llevar coeficientes irrelevantes exactamente a cero), y cualquier valor entre ambos es una mezcla (*Elastic Net*).

**Tabla 4. Tres configuraciones básicas, sin búsqueda exhaustiva**

| Configuración | `regParam` | `elasticNetParam` | Qué prueba |
|---|---:|---:|---|
| Sin regularización | 0.0 | 0.0 | La línea base, sin ningún ajuste. |
| Ridge (L2) | 0.1 | 0.0 | Si penalizar coeficientes grandes mejora la generalización. |
| Elastic Net (L1+L2) | 0.1 | 0.5 | Una mezcla de ambas penalizaciones. |

`LinearRegression` asume que la relación entre predictores y objetivo es, de fondo, lineal. `RandomForestRegressor` no necesita ese supuesto — construye muchos árboles de decisión sobre subconjuntos aleatorios de datos y variables, y promedia sus predicciones, capturando relaciones no lineales e interacciones que una ecuación lineal no puede representar:

```python
from pyspark.ml.regression import RandomForestRegressor

rf = RandomForestRegressor(featuresCol="features", labelCol="Valor_Objetivo", numTrees=50, maxDepth=8, seed=42)
modelo_rf = rf.fit(df_train)
```

Comparar ambas familias (lineal vs. árboles) es la forma más básica de saber si, de entrada, la relación real es aproximadamente lineal — si `RandomForestRegressor` mejora mucho a `LinearRegression`, es una señal de que la relación no lo es.

## 3. Aplica: actividad práctica guiada

Tiempo: 2h.

**Actividad:** construir el notebook `04_ml_distribuido_regresion_practica.ipynb` sobre el entorno `lambda26` (`uso-pyspark`), integrando tres fuentes reales de sensores en un Data Lake analítico particionado, y entrenando y comparando modelos de regresión distribuida con Spark MLlib sobre esa salida, reportando métricas iniciales (RMSE, R², MAE).

**Propósito de la actividad:** dejar evidencia ejecutable de que dominas la integración y calidad de datos multi-fuente, la preparación de datos para MLlib, el entrenamiento y evaluación de un modelo de regresión, y la comparación sistemática de configuraciones — antes de confiar en un único modelo, como en el caso de 1.6.

**Orientaciones metodológicas:** en clase, el docente guía la construcción del notebook paso a paso, alternando explicación breve y ejecución; los estudiantes replican cada celda en su propio entorno, verificando cada resultado antes de avanzar al siguiente paso.

**Actividades para realizar** (organizadas por fase CRISP-DM, 2.1 — Business Understanding ya se resolvió en 1.6):

- **3.1** Crear el notebook y la `SparkSession`.

*Fase 2 — Data Understanding*

- **3.2** Cargar las tres fuentes con esquema explícito.
- **3.3** Resolver duplicados de `FechaHora` en variables ambientales, antes de integrar.
- **3.4** Integrar las tres fuentes.
- **3.5** Explorar nulos en la tabla integrada.

*Fase 3 — Data Preparation*

- **3.6** Tratar una columna sin valores útiles y un código de error de sensor.
- **3.7** Confirmar ausencia de duplicados en la tabla final.
- **3.8** Nulos finales sobre las variables numéricas y filtrado.
- **3.9** Escritura particionada en Parquet, por mes (Gold).
- **3.10** Leer de vuelta y verificar el particionamiento.
- **3.11** Reutilizar el Data Lake Gold para el modelado.
- **3.12** Ensamblar el vector de predictores (`VectorAssembler`).
- **3.13** Dividir en entrenamiento y prueba.

*Fase 4 y 5 — Modeling y Evaluation*

- **3.14** Entrenar un modelo base de regresión lineal.
- **3.15** Evaluar el modelo base (RMSE, R², MAE).
- **3.16** Comparar tres configuraciones básicas de regularización.
- **3.17** Comparar con un segundo algoritmo (Random Forest) e importancia de variables.

*Fase 6 — Deployment*

- **3.18** Guardar el modelo seleccionado.

*Cierre*

- **3.19** Documentar hallazgos y responder preguntas de reflexión.

### 3.1 Crear el notebook y la `SparkSession`

**Producto del paso:** notebook `04_ml_distribuido_regresion_practica.ipynb` con una `SparkSession` activa.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("sesion4-ml-distribuido-regresion")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark
```

```python
ORIGEN_DATOS = "/opt/s04-ml-distribuido-regresion/data"
```

Nota metodológica: `ARTIFACTS` (la ruta de salida) todavía no se declara acá — recién hace falta en 3.9, cuando empieza a escribirse la primera salida. Cada configuración se declara en el paso donde se usa por primera vez, no de forma anticipada en un bloque global (más sobre esto en 2.1).

#### Fase 2 — Data Understanding (3.2-3.5)

Cargar cada fuente con su esquema, resolver el problema técnico que bloquearía la integración, integrar, y explorar la tabla resultante para conocerla — todavía sin decidir ninguna regla de limpieza.

### 3.2 Cargar las tres fuentes con esquema explícito

**Producto del paso:** `df_ce`, `df_cm`, `df_va` cargados, cada uno con su propio esquema (2.2).

```python
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType

schema_ce = StructType([
    StructField("FechaHora", TimestampType(), nullable=False),
    StructField("Valor_CE", DoubleType(), nullable=True),
])
df_ce = spark.read.csv(f"{ORIGEN_DATOS}/campo_electrico.csv", header=True, schema=schema_ce)

schema_cm = StructType([
    StructField("FechaHora", TimestampType(), nullable=False),
    StructField("Valor_CM", DoubleType(), nullable=True),
])
df_cm = spark.read.csv(f"{ORIGEN_DATOS}/campo_magnetico.csv", header=True, schema=schema_cm)

schema_va = StructType([
    StructField("TempOut", DoubleType(), nullable=True),
    StructField("OutHum", DoubleType(), nullable=True),
    StructField("WindSpeed", DoubleType(), nullable=True),
    StructField("WindDir", DoubleType(), nullable=True),
    StructField("Bar", DoubleType(), nullable=True),
    StructField("Rain", DoubleType(), nullable=True),
    StructField("SolarRad", DoubleType(), nullable=True),
    StructField("UVIndex", DoubleType(), nullable=True),
    StructField("FechaHora", TimestampType(), nullable=False),
])
df_va = spark.read.csv(f"{ORIGEN_DATOS}/variables_ambientales.csv", header=True, schema=schema_va)
```

En una corrida real: `campo_electrico.csv` trajo `186 664` filas, `campo_magnetico.csv` `525 600`, y `variables_ambientales.csv` `708 958` — esta última con `181 918` filas de `FechaHora` duplicada, la única de las tres con ese problema. Es justo la que hay que resolver **antes** de integrar (3.3): un `join` contra una clave duplicada multiplica filas del lado que no lo está, sin ningún error visible.

**Error frecuente**: la fuente original trae esta columna como `SolarRad.` (con un punto al final, tal como la exporta el equipo de medición). Referenciarla luego con `col("SolarRad.")` falla con `AnalysisException` — Spark interpreta el punto como acceso a un campo anidado, no como parte literal del nombre. No hace falta escapar el nombre en cada uso: como `header=True` junto con un `schema` explícito hace que Spark ignore el texto del header para nombrar columnas, basta con declarar el nombre ya limpio (`SolarRad`, sin punto) en el `StructField` de arriba.

### 3.3 Resolver duplicados de `FechaHora` en variables ambientales, antes de integrar

**Producto del paso:** `df_va_unico`, un registro por minuto, eligiendo la fila con menos nulos (2.2).

```python
from pyspark.sql.functions import col, count as spark_count, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

columnas_conteo_nulos = [c for c in df_va.columns if c not in ("FechaHora", "WindDir")]

df_va_con_conteo = df_va.withColumn(
    "CantidadNulos",
    sum(when(col(c).isNull(), 1).otherwise(0) for c in columnas_conteo_nulos),
)

ventana_va = Window.partitionBy("FechaHora").orderBy(col("CantidadNulos").asc())

df_va_unico = (
    df_va_con_conteo
    .withColumn("row_num", row_number().over(ventana_va))
    .filter(col("row_num") == 1)
    .drop("row_num", "CantidadNulos")
)
```

`WindDir` queda fuera del conteo de nulos a propósito — es 100 % nula en las tres fuentes (3.6): incluirla en el criterio de desempate no aportaría ninguna señal real. Mismo patrón de deduplicación determinista de S3 (`Window`+`row_number()`), con un criterio de orden distinto: en vez de "la fila con mayor `age`", acá se conserva **la fila con menos nulos** — el mismo minuto medido dos veces, con la versión más completa ganando. En una corrida real, `708 958` filas se redujeron a `527 040` — una por cada minuto distinto.

### 3.4 Integrar las tres fuentes

**Producto del paso:** `df_integrado`, con el campo eléctrico como tabla principal.

`FechaHora` es la clave común para integrar; el campo eléctrico queda como tabla principal (`left join`), porque interesa el periodo que ese sensor cubre, no el de los otros dos.

```python
df_integrado = (
    df_ce
    .join(df_cm, on="FechaHora", how="left")
    .join(df_va_unico, on="FechaHora", how="left")
)

print(f"Integrado: {df_integrado.count():,} registros x {len(df_integrado.columns)} columnas")
df_integrado.printSchema()
```

En una corrida real, el resultado fue `186 664` filas × `11` columnas — el mismo conteo que `campo_electrico.csv` por sí solo, sin duplicar ninguna fila: la deduplicación de 3.3 hizo su trabajo antes de llegar acá.

### 3.5 Explorar nulos en la tabla integrada

**Producto del paso:** conteo de nulos por columna sobre la tabla ya integrada — un `left join` puede introducir nulos nuevos si algún `FechaHora` del campo eléctrico no tiene contraparte en las otras dos fuentes.

```python
df_integrado.select([
    spark_count(when(col(c).isNull(), c)).alias(c) for c in df_integrado.columns
]).show(vertical=True, truncate=False)
```

#### Fase 3 — Data Preparation (3.6-3.13)

Con los datos ya conocidos (Fase 2), acá se aplican las reglas de limpieza, se escribe la salida Gold y se deja lista la tabla para modelar: nada de esto se decide sin la exploración previa.

### 3.6 Tratar una columna sin valores útiles y un código de error de sensor

**Producto del paso:** `df_limpio`, sin `WindDir` y sin el código de error de `Valor_CM` (2.2).

Dos problemas de calidad distintos, dos tratamientos distintos — ninguno de los dos es un nulo común:

**Tabla 5. Dos problemas de calidad, dos tratamientos distintos**

| Problema | Cómo se detecta | Por qué no es lo mismo que un nulo | Tratamiento |
|---|---|---|---|
| `WindDir` sin valores útiles | Conteo de nulos = 100 % de las filas | Sí es un nulo — pero total, no parcial: ninguna fila tiene el dato | Se descarta la columna completa |
| `Valor_CM = 99999` | Filtro por el valor de dominio conocido, no `isNull()` | No es un nulo: Spark ve un `Double` válido, no `NULL` | Se descartan las filas con ese código |

```python
nulos_winddir = df_integrado.filter(col("WindDir").isNull()).count()
total = df_integrado.count()
print(f"WindDir nula: {nulos_winddir:,} de {total:,} ({nulos_winddir/total*100:.1f}%)")

df_sin_winddir = df_integrado.drop("WindDir")
```

En una corrida real, `WindDir` dio `708 958` nulos de `708 958` filas — el 100 % — cero valores útiles en toda la fuente.

```python
errores_cm = df_sin_winddir.filter(col("Valor_CM") == 99999).count()
print(f"Filas con codigo de error Valor_CM=99999: {errores_cm:,}")

df_limpio = df_sin_winddir.filter(col("Valor_CM") != 99999)
print(f"Filas despues de eliminar el codigo de error: {df_limpio.count():,}")
```

En una corrida real, `2 126` filas tenían ese código — se descartan, no se tratan como si `99999` microteslas fuera un dato real.

### 3.7 Confirmar ausencia de duplicados en la tabla final

**Producto del paso:** confirmación de que ni la deduplicación (3.3) ni el `join` (3.4) dejaron `FechaHora` repetida.

```python
total_final = df_limpio.count()
sin_duplicar = df_limpio.dropDuplicates(["FechaHora"]).count()

print(f"Total: {total_final:,}, sin duplicar por FechaHora: {sin_duplicar:,}")
assert total_final == sin_duplicar, "Hay FechaHora duplicada en la tabla integrada final"
```

### 3.8 Nulos finales sobre las variables numéricas y filtrado

**Producto del paso:** `df_valido`, con las nueve variables físicas completas, cacheado para reutilizar en los pasos siguientes.

```python
VARIABLES_9 = [
    "Valor_CE", "Valor_CM", "TempOut", "OutHum",
    "WindSpeed", "Bar", "Rain", "SolarRad", "UVIndex",
]

antes = df_limpio.count()
df_valido = df_limpio.na.drop(subset=VARIABLES_9)
despues = df_valido.count()

print(f"Filas antes: {antes:,}, despues de na.drop(subset=VARIABLES_9): {despues:,}")
print(f"Filas eliminadas por nulos en variables criticas: {antes - despues:,}")

df_valido = df_valido.cache()
```

### 3.9 Escritura particionada en Parquet, por mes (Gold)

**Producto del paso:** `campo_electrico_particionado/`, la capa Gold de este Data Lake (2.2).

```python
from pyspark.sql.functions import date_format

ARTIFACTS = "/opt/s04-ml-distribuido-regresion/artifacts"

df_particionable = df_valido.withColumn("AnioMes", date_format(col("FechaHora"), "yyyy-MM"))

(
    df_particionable
    .repartition(4)
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("AnioMes")
    .save(f"{ARTIFACTS}/campo_electrico_particionado")
)

import os
for carpeta in sorted(os.listdir(f"{ARTIFACTS}/campo_electrico_particionado")):
    print(carpeta)
```

**Tabla 6. Filas reales por `AnioMes`**

| AnioMes | Filas |
|---|---:|
| 2025-05 | 28 107 |
| 2025-06 | 41 502 |
| 2025-07 | 1 409 |
| 2025-08 | 20 582 |
| 2025-09 | 27 397 |
| 2025-10 | 32 877 |
| 2025-11 | 32 652 |
| 2025-12 | 12 |

Julio y diciembre tienen muchas menos filas que el resto — un vacío real de medición, no un error de esta guía.

### 3.10 Leer de vuelta y verificar el particionamiento

**Producto del paso:** `df_verificacion`, confirmado sin pérdida de filas, con `PartitionFilters` en el plan de ejecución al filtrar por `AnioMes` (S3, 2.5).

```python
df_verificacion = spark.read.parquet(f"{ARTIFACTS}/campo_electrico_particionado")
df_verificacion.printSchema()

assert df_verificacion.count() == df_particionable.count()
print(f"Verificado: {df_verificacion.count():,} filas, ida y vuelta sin perdida.")

df_verificacion.filter(col("AnioMes") == "2025-09").explain(True)
```

Para ver cuántas filas quedaron guardadas en cada partición, sin salir de Spark ni contar archivos a mano:

```python
df_verificacion.groupBy("AnioMes").count().orderBy("AnioMes").show(truncate=False)
```

En una corrida real, coincide exactamente con la Tabla 6.

```python
df_valido.unpersist()
```

### 3.11 Reutilizar el Data Lake Gold para el modelado

**Producto del paso:** `df`, listo para ensamblar el vector de predictores.

`df_verificacion` (3.10) ya es la salida Gold, leída y verificada — no hace falta volver a leer el Parquet desde disco para empezar la parte de modelado:

```python
df = df_verificacion

df.printSchema()
print(f"Filas: {df.count():,}")
df.describe(VARIABLES_9).show()
```

En una corrida real, este conteo dio **184 538** filas.

### 3.12 Ensamblar el vector de predictores (`VectorAssembler`)

**Producto del paso:** `df_ml`, con una sola columna `features` (vector de 8 predictores) y la columna objetivo `Valor_CE` (2.3).

```python
from pyspark.ml.feature import VectorAssembler

PREDICTORES = [v for v in VARIABLES_9 if v != "Valor_CE"]
print(f"Predictores ({len(PREDICTORES)}): {PREDICTORES}")

ensamblador = VectorAssembler(inputCols=PREDICTORES, outputCol="features")
df_ml = ensamblador.transform(df).select("features", "Valor_CE")

df_ml.show(5, truncate=False)
```

### 3.13 Dividir en entrenamiento y prueba

**Producto del paso:** `df_train`/`df_test`, división aleatoria 80/20 (2.3, Tabla 2).

```python
df_train, df_test = df_ml.randomSplit([0.8, 0.2], seed=42)

print(f"Entrenamiento: {df_train.count():,} filas")
print(f"Prueba: {df_test.count():,} filas")
```

En una corrida real, sobre las 184 538 filas: **147 943** para entrenamiento y **36 595** para prueba — el 80/20 esperado, con `seed=42` garantizando que sea la misma división en cada nueva corrida (necesario para que las comparaciones de 3.16-3.17 sean justas entre sí).

#### Fase 4 y 5 — Modeling y Evaluation (3.14-3.17, cierre en 3.19)

En un pipeline de investigación completo, Modeling (ajustar y comparar candidatos con validación interna) y Evaluation (medir el resultado final, una sola vez, sobre una prueba reservada) son fases separadas. Esta sesión no reserva una tercera partición para validación interna — cada configuración se entrena y se mide directamente contra `df_test` en el mismo paso — así que aquí ambas fases avanzan juntas, paso por paso, hasta cerrar con la reflexión de 3.19.

### 3.14 Entrenar un modelo base de regresión lineal

**Producto del paso:** `modelo_base` entrenado, con coeficientes e intercepto visibles (2.4).

```python
from pyspark.ml.regression import LinearRegression

lr_base = LinearRegression(featuresCol="features", labelCol="Valor_CE")
modelo_base = lr_base.fit(df_train)

print("Coeficientes:", modelo_base.coefficients)
print("Intercepto:", modelo_base.intercept)
```

**Advertencias esperadas, no errores**: al correr esta celda pueden aparecer dos líneas en la consola —

- `regParam is zero, which might cause numerical instability and overfitting`: Spark avisa que este modelo base no tiene regularización (2.6) — es exactamente lo que se está probando a propósito como línea base en 3.16, no un problema a corregir.
- `netlib-blas: JNI_OnLoad: dlopen(libblas.so.3) failed...`: el contenedor no tiene una librería de álgebra lineal nativa instalada, así que Spark cae a una implementación en JVM pura — más lenta, pero igual de correcta. No afecta ningún resultado de esta sesión.

En una corrida real, `modelo_base.coefficients` dio `[-0.0028, 0.1454, 0.0069, -0.0779, -0.0429, 1.2399, -0.0007, 0.0416]` (en el orden de `PREDICTORES`) y el intercepto `104.7116`. El coeficiente de `Rain` (`1.2399`) es, por lejos, el más grande en magnitud — vale la pena que confirmes si ese signo y esa magnitud tienen sentido físico con lo que ya sabes del dominio, antes de asumir que el modelo "aprendió algo real".

### 3.15 Evaluar el modelo base (RMSE, R², MAE)

**Producto del paso:** las tres métricas del modelo base, sobre el conjunto de prueba — nunca sobre el de entrenamiento (2.5).

```python
from pyspark.ml.evaluation import RegressionEvaluator

predicciones_base = modelo_base.transform(df_test)
predicciones_base.select("Valor_CE", "prediction").show(5)

def evaluar(predicciones, nombre):
    resultados = {}
    for metrica in ["rmse", "r2", "mae"]:
        evaluador = RegressionEvaluator(labelCol="Valor_CE", predictionCol="prediction", metricName=metrica)
        resultados[metrica.upper()] = evaluador.evaluate(predicciones)
    print(f"{nombre}: RMSE={resultados['RMSE']:.4f}  R2={resultados['R2']:.4f}  MAE={resultados['MAE']:.4f}")
    return resultados

resultados_base = evaluar(predicciones_base, "LinearRegression base")
```

**Tabla 7. Resultado real del modelo base**

| Métrica | Valor |
|---|---|
| RMSE | 0.7909 |
| R² | 0.2271 |
| MAE | 0.6087 |

Un R² de `0.23` significa que el modelo lineal explica poco menos de un cuarto de la variabilidad real de `Valor_CE` — no es un resultado inútil, pero deja bastante margen sin explicar. Si esa cifra sorprende, guárdala: 3.17 compara este mismo resultado contra un algoritmo sin el supuesto de linealidad.

### 3.16 Comparar tres configuraciones básicas de regularización

**Producto del paso:** tabla comparativa de las tres configuraciones de la Tabla 4 (2.6).

```python
configuraciones = [
    {"nombre": "Sin regularizacion", "regParam": 0.0, "elasticNetParam": 0.0},
    {"nombre": "Ridge (L2)", "regParam": 0.1, "elasticNetParam": 0.0},
    {"nombre": "Elastic Net (L1+L2)", "regParam": 0.1, "elasticNetParam": 0.5},
]

comparacion_configs = []
for config in configuraciones:
    lr = LinearRegression(
        featuresCol="features", labelCol="Valor_CE",
        regParam=config["regParam"], elasticNetParam=config["elasticNetParam"],
    )
    modelo = lr.fit(df_train)
    predicciones = modelo.transform(df_test)
    resultado = evaluar(predicciones, config["nombre"])
    resultado["Configuracion"] = config["nombre"]
    comparacion_configs.append(resultado)

import pandas as pd
pd.DataFrame(comparacion_configs)[["Configuracion", "RMSE", "R2", "MAE"]]
```

### 3.17 Comparar con un segundo algoritmo (Random Forest) e importancia de variables

**Producto del paso:** métricas de `RandomForestRegressor`, comparables directamente contra la Tabla 7 y el paso 3.16 (2.6), más la importancia de cada variable.

```python
from pyspark.ml.regression import RandomForestRegressor

rf = RandomForestRegressor(featuresCol="features", labelCol="Valor_CE", numTrees=50, maxDepth=8, seed=42)
modelo_rf = rf.fit(df_train)
predicciones_rf = modelo_rf.transform(df_test)

resultados_rf = evaluar(predicciones_rf, "Random Forest")
```

**Tabla 8. Comparación final**

| Configuración | RMSE | R² | MAE |
|---|---|---|---|
| Sin regularización | 0.7909 | 0.2271 | 0.6087 |
| Ridge (L2) | 0.7962 | 0.2166 | 0.6174 |
| Elastic Net (L1+L2) | 0.8091 | 0.1910 | 0.6372 |
| **Random Forest** | **0.6732** | **0.4399** | **0.5001** |

En una corrida real, **Random Forest ganó en las tres métricas a la vez** — no solo en RMSE, también en R² (casi el doble que el mejor modelo lineal) y en MAE. Entre las tres configuraciones lineales, agregar regularización **empeoró** el resultado en vez de mejorarlo: una señal de que este modelo base no estaba sobreajustando — no había ningún problema de *overfitting* que la regularización tuviera que corregir. La ganancia real vino de otro lado: `RandomForestRegressor` no asume una relación lineal entre las 8 variables ambientales y `Valor_CE`, y esa relación, en los datos reales, no lo es — coherente con que `Rain` (3.14) tuviera un coeficiente lineal desproporcionadamente grande frente a las demás variables.

**¿Las 8 variables aportan por igual?** Los coeficientes de `LinearRegression` (3.14) no responden esto de forma confiable — no son comparables entre sí porque cada variable tiene una escala distinta (`Valor_CM` en miles, `Rain` entre 0 y 0.2). `RandomForestRegressor` sí calcula algo directamente comparable, sin costo adicional: `featureImportances`, una proporción de cuánto reduce cada variable el error del modelo en promedio, a lo largo de todos sus árboles — las proporciones de las 8 variables suman `1.0`.

```python
importancias = list(zip(PREDICTORES, modelo_rf.featureImportances.toArray()))
importancias.sort(key=lambda x: x[1], reverse=True)

for variable, importancia in importancias:
    print(f"{variable:12s} {importancia:.4f}")
```

**Tabla 9. Importancia de cada variable — completar con tu corrida real**

| Variable | Importancia |
|---|---|
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |
| ____ | ____ |

Si una o dos variables concentran la mayor parte de la importancia y el resto aporta casi nada, es una señal real para decidir con datos —no por intuición— si conviene simplificar el modelo a menos predictores en una futura iteración (fuera del alcance evaluado de esta sesión).

#### Fase 6 — Deployment (3.18)

Aquí "Deployment" significa exactamente lo que hace este paso — persistir el artefacto ganador para poder reutilizarlo sin reentrenar — y no más que eso: no hay empaquetado productivo, servicio de inferencia ni monitoreo; ese alcance queda fuera de esta sesión.

### 3.18 Guardar el modelo seleccionado

**Producto del paso:** el modelo con mejor RMSE en la Tabla 8, guardado como artefacto reutilizable.

```python
modelo_ganador = modelo_rf  # Random Forest gano las tres metricas (Tabla 8)

modelo_ganador.write().overwrite().save(f"{ARTIFACTS}/modelo_ce_regresion")
print(f"Modelo guardado en {ARTIFACTS}/modelo_ce_regresion")
```

**Error frecuente**: guardar el primer modelo entrenado (3.14) en vez del que realmente ganó la comparación de la Tabla 8. La sección 4.6 evalúa explícitamente que el modelo guardado coincida con el mejor resultado reportado — no con el más rápido de entrenar. En esta corrida coinciden (`modelo_rf` ya era el modelo de referencia del notebook), pero no des por sentado que el ganador siempre va a ser el mismo que el ejemplo — vuelve a comparar cada vez que cambien los datos.

### 3.19 Documentar hallazgos y responder preguntas de reflexión

**Producto del paso:** notebook documentado con celdas markdown explicando cada resultado.

Agrega celdas markdown breves debajo de cada bloque de código (3.3-3.17) explicando qué hiciste y qué observaste — es la base directa de la evidencia técnica que armarás en 4.3.1.

**Reflexión técnica breve** (5 a 8 líneas): ¿por qué resolver los duplicados de variables ambientales antes del `join` evita un problema que, si se dejara para después, sería más difícil de rastrear? ¿qué configuración de la Tabla 8 tuvo el mejor RMSE, y por cuánto margen superó a la línea base sin regularización? ¿cuál fue la variable con mayor `featureImportances` en tu Tabla 9, y tiene sentido físico que sea la más relevante para explicar `Valor_CE`? ¿por qué `VectorAssembler` es un paso obligatorio en Spark MLlib y no en scikit-learn?

**Evidencia de aprendizaje:**

- Notebook `04_ml_distribuido_regresion_practica.ipynb` con las tres fuentes integradas, calidad de datos aplicada, y salida particionada (Gold) verificada.
- Vector de predictores, modelo base, evaluación y comparación de configuraciones documentados.
- Comparación con un segundo algoritmo (`RandomForestRegressor`), incluida la importancia de cada variable (`featureImportances`).
- Modelo seleccionado guardado, coincidente con el mejor resultado de la comparación.
- Reflexión técnica documentada.

## 4. Crea: actividad autónoma

Tiempo: 3h fuera del aula.

### 4.1 Actividad

Replicación autónoma de la integración de datos y del entrenamiento/comparación de modelos de regresión construidos en clase, sobre datos del Proyecto Sello del equipo — reales si el equipo ya los tiene, o una muestra representativa si todavía no hay datos reales disponibles.

Completa y evidencia estas tareas:

1. Integrar al menos dos fuentes de datos del Proyecto Sello (o preparar una sola fuente ya integrada, si el caso del equipo no tiene múltiples orígenes), con esquema explícito y calidad de datos aplicada (equivalente a 3.2-3.8).
2. Escribir una salida particionada en Parquet (Gold) e identificar sus capas Bronze/Silver/Gold (equivalente a 3.9-3.10, 2.2).
3. Ensamblar el vector de predictores sobre esa salida, con una variable objetivo numérica relevante al caso del equipo (equivalente a 3.12).
4. Entrenar un modelo base de regresión y evaluarlo con las tres métricas (RMSE, R², MAE) (equivalente a 3.14-3.15).
5. Comparar al menos tres configuraciones básicas de regularización, documentando el resultado de cada una (equivalente a 3.16).
6. Comparar con un segundo algoritmo de MLlib, distinto de `LinearRegression`, incluida la importancia de variables (equivalente a 3.17).
7. Guardar el modelo con mejor desempeño, coincidente con la comparación (equivalente a 3.18).
8. Explicar, con tus propias palabras, por qué ninguna métrica sola basta para decidir cuál modelo es mejor.

### 4.2 Propósito

Que cada estudiante demuestre, de forma individual y fuera del aula, que puede integrar datos con calidad, y entrenar, evaluar y comparar modelos de regresión distribuida sin el acompañamiento del docente — aplicándolo al caso real del Proyecto Sello de su equipo, no a un dataset desconectado.

### 4.3 Indicaciones

Entrega un PDF con el siguiente nombre:

```text
S04_Equipo##_ApellidoNombre.pdf
```

Cada captura de pantalla del informe debe mostrar, sin recortar, el reloj del sistema (fecha y hora) y tu usuario o foto de perfil (Windows, VS Code o navegador) visibles en pantalla — es lo que permite verificar que la evidencia es tuya y que corresponde al momento real de tu trabajo.

#### 4.3.1 Estructura del informe

**Datos del estudiante**

- Nombre:
- Equipo:
- Sesión: S04 - ML Distribuido con Spark MLlib (Regresión)
- Rol o aporte realizado:
- Link de GitHub:

**Evidencia técnica**

Incluye capturas o extractos con una breve explicación debajo de cada uno, organizados en los mismos 5 bloques de la rúbrica (4.6):

1. *Integración y calidad de datos*
    - Fuentes integradas con esquema explícito, calidad de datos aplicada (equivalente a 3.2-3.8).
2. *Salida particionada (Gold)*
    - Salida en Parquet particionada, verificada, con sus capas Bronze/Silver/Gold identificadas (equivalente a 3.9-3.10).
3. *Preparación y modelo base*
    - Vector de predictores ensamblado; modelo base entrenado y evaluado con las tres métricas (equivalente a 3.12, 3.14-3.15).
4. *Comparación de configuraciones y algoritmos*
    - Tabla comparativa de al menos tres configuraciones de regularización y un segundo algoritmo, con importancia de variables (equivalente a 3.16-3.17).
5. *Modelo seleccionado y reflexión*
    - Modelo guardado, coincidente con la mejor comparación, más la reflexión técnica (equivalente a 3.18-3.19).

**Error o hallazgo**

Describe al menos un error o comportamiento inesperado que encontraste al integrar tus datos o al entrenar/comparar tus propios modelos:

- Qué ocurrió o qué limitación encontraste.
- Cómo lo identificaste.
- Cómo lo resolviste o qué decisión tomaste.

**Reflexión técnica breve**

Responde en 5 a 8 líneas:

```text
¿Por qué confiar en un único modelo, sin comparar configuraciones ni
algoritmos alternativos, es una decisión de riesgo cuando ese modelo
va a usarse para tomar decisiones reales?
```

**Anexo: Feedback de la sesión**

Pega esta página como la última hoja del PDF, con tus respuestas.

1. ¿Cuál es el aprendizaje más importante que te llevas de la clase de hoy?
2. ¿Qué punto de la clase te resultó más confuso o te dejó con dudas?
3. ¿Tienes alguna pregunta que te gustaría que sea respondida la siguiente clase?
4. Sobre tu nivel de comprensión de la clase de hoy, marca una opción:
    - ¡Entendido! - Lo domino y podría explicarlo.
    - Más o menos. - Entendí la idea general, pero tengo dudas.
    - Necesito ayuda. - Me siento perdido/a con este tema.
5. ¿Cómo puedo ayudarte a comprender mejor el tema?
6. Pensando en tu participación y esfuerzo en la clase de hoy, ¿cómo te autoevaluarías? Marca una opción:
    - Muy Comprometido/a: Me esforcé al máximo.
    - Comprometido/a: Sé que podría haberme esforzado un poco más.
    - Poco Comprometido/a: Hoy no di mi mejor esfuerzo.
7. Mi satisfacción con la clase fue... (califica del 1 al 10, donde 1 es insatisfecho y 10 es muy satisfecho).

### 4.4 Criterios mínimos de aceptación

La evidencia individual se considera completa si:

- El archivo respeta el nombre solicitado.
- Al menos una fuente de datos del Proyecto Sello fue cargada con esquema explícito y calidad de datos aplicada.
- Una salida particionada (Gold) fue escrita y verificada.
- Un vector de predictores fue ensamblado sobre esa salida.
- El modelo base fue entrenado y evaluado con RMSE, R² y MAE.
- Al menos tres configuraciones de regularización fueron comparadas con las mismas métricas.
- Un segundo algoritmo de MLlib fue comparado contra el modelo lineal, con importancia de variables.
- El modelo guardado coincide con el de mejor desempeño en la comparación.
- Cada captura de la evidencia técnica muestra el reloj del sistema y el usuario/perfil visible, sin recortar.
- Las fechas y horas de las capturas son coherentes con el historial de commits de su repositorio en GitHub.
- Incluye un error o hallazgo técnico diagnosticado.
- Incluye la reflexión técnica breve solicitada.
- Incluye el Anexo de feedback de la sesión respondido, como última página del PDF.

### 4.5 Preguntas de defensa

1. ¿Por qué resolver los duplicados de una fuente antes de integrarla evita un problema más difícil de rastrear después?
2. ¿Cuál es la diferencia entre un nulo y un código de error de sensor, y por qué `isNull()` no detecta el segundo?
3. En tu propio pipeline, ¿qué artefacto real corresponde a Bronze, cuál a Silver y cuál a Gold?
4. ¿Por qué `VectorAssembler` es un paso obligatorio en Spark MLlib, y qué pasaría si intentaras entrenar un modelo sin ese paso?
5. ¿Qué diferencia hay entre `regParam=0.0` y `regParam` con un valor alto, y qué riesgo controla cada extremo?
6. ¿Por qué RMSE penaliza más los errores grandes que MAE, y cuándo esa diferencia importa más para una decisión real?
7. ¿Por qué esta sesión usa una división aleatoria (`randomSplit`) en vez de una división cronológica?
8. Si `RandomForestRegressor` hubiera dado un RMSE mucho peor que `LinearRegression`, ¿qué te diría eso sobre la relación entre tus variables?

### 4.6 Rúbrica de evaluación

**Tabla 10. Rúbrica de evaluación**

| Criterio | Peso (%) | A (20 pts) | B (15 pts) | C (10 pts) | D (5 pts) | Nivel obtenido |
|---|---:|---|---|---|---|---:|
| 1. Integración y calidad de datos* | 20 | Fuentes integradas con esquema explícito y calidad de datos aplicada (nulos, duplicados, casos especiales) con criterio documentado. | Integración y calidad correctas, con algún criterio menos justificado. | Integración o calidad incompleta. | No integra fuentes ni aplica calidad de datos. | |
| 2. Salida particionada (Gold)* | 20 | Salida particionada, verificada, con las tres capas (Bronze/Silver/Gold) identificadas sobre el propio pipeline. | Salida particionada y verificada, sin identificar las capas con claridad. | Escritura particionada incompleta o sin verificación. | No escribe salida particionada. | |
| 3. Preparación y modelo base* | 20 | Vector de predictores correcto, modelo base entrenado y evaluado con las tres métricas. | Preparación y modelo base correctos, con alguna métrica incompleta. | Preparación o modelo base incompletos. | No presenta un modelo entrenado ni evaluado. | |
| 4. Comparación de configuraciones y algoritmos* | 20 | Al menos tres configuraciones y un segundo algoritmo comparados con las mismas métricas, con importancia de variables y análisis claro. | Comparación completa, con análisis superficial. | Comparación incompleta. | No compara configuraciones ni algoritmos. | |
| 5. Modelo guardado y comprensión* | 20 | Modelo guardado coincide con la mejor comparación; explicación clara de por qué ninguna métrica sola basta. | Modelo guardado correcto, explicación con detalles menores. | Modelo guardado no coincide con la mejor comparación, o explicación superficial. | No guarda modelo ni explica el criterio de selección. | |

\* Agregado manual.

Nota final = suma de (`Peso` / 100 × `Puntos del nivel obtenido`) = ____ / 20.

Para usar la rúbrica con IA, solicita:

```text
Evalúa el PDF usando la rúbrica de la sesión.
Para cada criterio selecciona el nivel obtenido usando la escala A=20, B=15, C=10, D=5 puntos.
Justifica brevemente cada nivel asignado.
Verifica que cada captura muestre reloj del sistema y usuario/perfil visible, y que las fechas sean coherentes con el historial de commits de GitHub. Si falta esta evidencia o hay inconsistencias, indícalo explícitamente antes de calificar.
Calcula la nota final con la fórmula: suma de (Peso/100 × Puntos del nivel obtenido), directamente sobre 20.
Indica 2 fortalezas y 2 recomendaciones.
```

## 5. Cierre

Tiempo: 5 min.

**Resumen breve:** hoy el pipeline batch ganó un Data Lake propio y su primer modelo — tres fuentes reales integradas y validadas (esquema, duplicados, un código de error distinto de un nulo), escritas como una capa Gold particionada; sobre esa salida, un vector de predictores ensamblado con `VectorAssembler`, un modelo base de regresión lineal entrenado y evaluado con tres métricas distintas (RMSE, R², MAE), comparado sistemáticamente contra otras configuraciones de regularización y contra un segundo algoritmo (Random Forest), antes de guardar el que realmente tuvo mejor desempeño — no el primero que se entrenó.

**Dinámica participativa:** en una ronda rápida, cada estudiante comparte qué configuración de la Tabla 8 le dio el mejor RMSE, y si le sorprendió o no.

**Metacognición:** ¿qué parte de la sesión te costó más entender: integrar tres fuentes con calidad de datos, por qué Spark MLlib necesita `VectorAssembler`, la diferencia entre RMSE y MAE, o por qué comparar contra Random Forest importa aunque el modelo lineal ya "funcione"?

**Proyección:** S5 evalúa el pipeline batch completo de la Unidad I — arquitectura, PySpark, calidad de datos, particionamiento y el Data Lake con el modelo de regresión de hoy — como un sistema integrado. El componente de series de tiempo (con historial temporal e inferencia en streaming) queda para S10, sobre la misma familia de datos, una vez que exista la infraestructura de streaming (S6-S9).

## Bibliografía

1. incidentdatabase.ai. (2021). *Incident 149: Zillow Shut Down Zillow Offers Division Allegedly Due to Predictive Pricing Tool's Insufficient Accuracy*. https://incidentdatabase.ai/cite/149/
2. Apache Software Foundation. (2024). *Apache Spark documentation*. https://spark.apache.org/docs/latest/
3. Apache Software Foundation. (2024). *PySpark API reference: pyspark.ml*. https://spark.apache.org/docs/latest/api/python/reference/pyspark.ml.html
4. Apache Software Foundation. (2024). *PySpark API reference: pyspark.sql.functions*. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
5. Apache Software Foundation. (2024). *Parquet files*. Spark SQL Guide. https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
6. Apache Software Foundation. (2024). *ML Tuning: model selection and hyperparameter tuning*. Spark MLlib Guide. https://spark.apache.org/docs/latest/ml-tuning.html
