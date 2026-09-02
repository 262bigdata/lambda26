# S3b - Calidad de datos y particionamiento: un segundo caso de uso (sensores ambientales)

*Material de referencia, no una sesión del sílabo.* Aplica las mismas técnicas de calidad de datos y particionamiento de [S3](S03_Procesamiento_Calidad_Datos_Particionamiento.md) sobre un dataset real distinto — tres fuentes de sensores ambientales que hay que integrar antes de poder tratarlas. Es la entrada real de [S4](S04_ML_Distribuido_Regresion_Spark_MLlib.md) (ML distribuido con Spark MLlib).

## 1. Propósito

S3 (H&M) parte de una sola fuente ya lista. Este segundo caso de uso agrega el problema que casi cualquier proyecto real tiene desde el principio: los datos no llegan integrados, llegan repartidos en varios sistemas de origen, cada uno con su propia estructura y sus propios problemas de calidad. Resolver eso — antes de aplicar esquema, nulos y duplicados — es el contenido nuevo de esta guía.

Al finalizar podrás construir un flujo reproducible que:

1. lea tres fuentes con esquema explícito, cada una con su propia estructura;
2. resuelva duplicados en una fuente **antes** de integrarla, no después;
3. integre las tres por una clave temporal común;
4. distinga un nulo de un código de error de sensor (`99999`), que no es lo mismo;
5. descarte una columna sin valores útiles, en vez de rellenarla;
6. escriba una salida Parquet particionada por periodo (mes), no por categoría;
7. verifique la lectura y el *partition pruning* sobre esa partición temporal.

## 2. Preparación del laboratorio

### 2.1 Estructura esperada

```text
s03b-calidad-campo-electrico/
├── data/
│   ├── campo_electrico.csv
│   ├── campo_magnetico.csv
│   └── variables_ambientales.csv
└── artifacts/
```

Las tres fuentes ya vienen extraídas a CSV (fecha y hora combinadas en una sola columna `FechaHora`) — la extracción desde el formato original de cada sensor no es el contenido de esta guía, igual que S3 no reconstruye de dónde salió `customers.csv` originalmente.

### 2.2 Sesión de Spark

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("sesion3b-calidad-campo-electrico")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

ORIGEN_DATOS = "/opt/s03b-calidad-campo-electrico/data"
ARTIFACTS = "/opt/s03b-calidad-campo-electrico/artifacts"
```

### 2.3 Lectura con esquema explícito

Tres fuentes, tres esquemas — cada una con su propia estructura, unidas después por `FechaHora`:

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
    StructField("SolarRad.", DoubleType(), nullable=True),
    StructField("UVIndex", DoubleType(), nullable=True),
    StructField("FechaHora", TimestampType(), nullable=False),
])
df_va = spark.read.csv(f"{ORIGEN_DATOS}/variables_ambientales.csv", header=True, schema=schema_va)
```

**Tabla 1. Las tres fuentes, tal como llegan**

| Fuente | Filas | `FechaHora` duplicada |
|---|---:|---:|
| `campo_electrico.csv` | 186 664 | 0 |
| `campo_magnetico.csv` | 525 600 | 0 |
| `variables_ambientales.csv` | 708 958 | **181 918** |

Solo una de las tres trae duplicados — y es justo la que hay que resolver antes de integrar (sección 3), porque un `join` contra una clave duplicada multiplica filas del lado que no lo está, sin ningún error visible.

## 3. Resolver duplicados de una fuente antes de integrar

Mismo patrón de deduplicación determinista de S3 (`Window`+`row_number()`), con un criterio de orden distinto: en vez de "la fila con mayor `age`", acá se conserva **la fila con menos nulos** — el mismo minuto medido dos veces, con la versión más completa ganando.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when

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

`WindDir` queda fuera del conteo de nulos a propósito — como se confirma en la sección 4, esa columna es 100 % nula en las tres fuentes: incluirla en el criterio de desempate no aportaría ninguna señal real. En una corrida real, `708 958` filas se redujeron a `527 040` — una por cada minuto distinto.

## 4. Integrar las tres fuentes

`FechaHora` es la clave común. El campo eléctrico queda como tabla principal (`left join`): interesa el periodo que ese sensor cubre, no el de los otros dos.

```python
df_integrado = (
    df_ce
    .join(df_cm, on="FechaHora", how="left")
    .join(df_va_unico, on="FechaHora", how="left")
)
```

En una corrida real, el resultado fue `186 664` filas × `11` columnas — el mismo conteo que `campo_electrico.csv` por sí solo, sin duplicar ninguna fila: la deduplicación de la sección 3 hizo su trabajo antes de llegar acá.

## 5. Dos tipos de dato sucio, no uno

### 5.1 `WindDir`: una columna sin un solo valor útil

A diferencia de H&M (donde `FN`/`Active` tenían ~65 % de nulos, pero no 100 %), acá el conteo real fue **`708 958` nulos de `708 958` filas** — cero valores útiles en toda la fuente. No es un caso de "rellenar con un valor razonable" (Tabla 5 de S3) — es una columna que no aporta nada, y se descarta directamente:

```python
df_sin_winddir = df_integrado.drop("WindDir")
```

### 5.2 `Valor_CM = 99999`: un código de error, no un nulo

`isNull()` nunca encuentra este problema — el sensor de campo magnético usa `99999` como código de error del equipo, un valor numérico perfectamente válido para Spark, pero físicamente imposible. Detectarlo exige conocer el dominio del dato, no solo su tipo:

```python
errores_cm = df_sin_winddir.filter(col("Valor_CM") == 99999).count()
df_limpio = df_sin_winddir.filter(col("Valor_CM") != 99999)
```

En una corrida real, `2 126` filas tenían ese código — se descartan, no se tratan como si `99999` microteslas fuera un dato real.

**Tabla 2. Dos problemas de calidad, dos tratamientos distintos**

| Problema | Cómo se detecta | Por qué no es lo mismo que un nulo | Tratamiento |
|---|---|---|---|
| `WindDir` sin valores útiles | Conteo de nulos = 100 % de las filas | Sí es un nulo — pero total, no parcial: ninguna fila tiene el dato | Se descarta la columna completa |
| `Valor_CM = 99999` | Filtro por el valor de dominio conocido, no `isNull()` | No es un nulo: Spark ve un `Double` válido, no `NULL` | Se descartan las filas con ese código |

## 6. Confirmar ausencia de duplicados en la tabla final

```python
total_final = df_limpio.count()
sin_duplicar = df_limpio.dropDuplicates(["FechaHora"]).count()

assert total_final == sin_duplicar, "Hay FechaHora duplicada en la tabla integrada final"
```

En una corrida real, ambos conteos coincidieron — la deduplicación de la sección 3 y el `left join` de la sección 4 no dejaron ninguna `FechaHora` repetida en la tabla final.

## 7. Nulos finales sobre las variables críticas

Las nueve variables (`Valor_CE`, `Valor_CM` y las siete ambientales restantes, sin `WindDir`) son todas necesarias como entrada del modelo de regresión de S4 — una fila con cualquiera de ellas en nulo no sirve. A diferencia de H&M (donde `FN`/`Active` admitían un `0` razonable), acá ninguna de las nueve variables físicas admite un relleno con sentido: un `0` en `TempOut` no es "temperatura ausente", es una temperatura falsa que se vería como un dato real. Se descartan las filas incompletas, no se rellenan:

```python
VARIABLES_9 = [
    "Valor_CE", "Valor_CM", "TempOut", "OutHum",
    "WindSpeed", "Bar", "Rain", "SolarRad.", "UVIndex",
]

df_valido = df_limpio.na.drop(subset=VARIABLES_9).cache()
```

En una corrida real, el conteo antes y después de este paso fue el mismo (`184 538`) — las nueve variables ya llegaban completas después de los pasos 5 y 6; el filtro queda como control explícito, no como corrección de un problema que ya apareció.

## 8. Escritura particionada en Parquet, por mes

H&M particionó por `club_member_status` — una columna categórica ya presente en los datos. Acá no existe una columna así, pero `FechaHora` permite **derivar** una: el mes. Particionar series de tiempo por periodo (mes, día) es el patrón más común en almacenamiento analítico real, distinto de "particionar por categoría", pero basado en el mismo criterio: pocos valores distintos, usados seguido en filtros.

```python
from pyspark.sql.functions import date_format

df_particionable = df_valido.withColumn("AnioMes", date_format(col("FechaHora"), "yyyy-MM"))

(
    df_particionable
    .repartition(4)
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("AnioMes")
    .save(f"{ARTIFACTS}/campo_electrico_particionado")
)
```

**Tabla 3. Filas reales por `AnioMes`**

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

Julio y diciembre tienen muchas menos filas que el resto — un vacío real de medición, no un error de esta guía. Vale la pena confirmarlo antes de usar estos meses como ejemplo en S4: una partición con 12 filas no es representativa para nada que dependa de volumen.

## 9. Lectura y verificación

```python
df_verificacion = spark.read.parquet(f"{ARTIFACTS}/campo_electrico_particionado")

assert df_verificacion.count() == df_particionable.count()

df_verificacion.filter(col("AnioMes") == "2025-09").explain(True)
```

El plan de ejecución debe mostrar `PartitionFilters` sobre `AnioMes` (S3, sección 2.5) — filtrar por mes descarta carpetas enteras antes de abrir ningún archivo, exactamente igual que filtrar por `club_member_status` en H&M, solo que la columna de partición esta vez viene derivada de una fecha, no copiada tal cual de la fuente.

```python
df_valido.unpersist()
```

## 10. Pipeline integrado

```python
df_va_unico = (
    df_va
    .withColumn(
        "CantidadNulos",
        sum(when(col(c).isNull(), 1).otherwise(0)
            for c in df_va.columns if c not in ("FechaHora", "WindDir")),
    )
    .withColumn("row_num", row_number().over(
        Window.partitionBy("FechaHora").orderBy(col("CantidadNulos").asc())
    ))
    .filter(col("row_num") == 1)
    .drop("row_num", "CantidadNulos")
)

df_valido = (
    df_ce
    .join(df_cm, on="FechaHora", how="left")
    .join(df_va_unico, on="FechaHora", how="left")
    .drop("WindDir")
    .filter(col("Valor_CM") != 99999)
    .na.drop(subset=VARIABLES_9)
    .withColumn("AnioMes", date_format(col("FechaHora"), "yyyy-MM"))
)

(
    df_valido
    .repartition(4)
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("AnioMes")
    .save(f"{ARTIFACTS}/campo_electrico_particionado")
)
```

## 11. Evidencia mínima del laboratorio

El notebook o informe debe mostrar:

1. esquema y conteo inicial de cada una de las tres fuentes;
2. duplicados de `FechaHora` detectados en variables ambientales, antes de integrar;
3. deduplicación determinista con `Window`+`row_number()`, con su criterio de orden;
4. conteo de filas de la tabla integrada;
5. `WindDir` identificada como 100 % nula, y descartada;
6. `Valor_CM = 99999` identificado y filtrado, distinto de un nulo;
7. confirmación de ausencia de duplicados en la tabla final;
8. escritura Parquet particionada por `AnioMes`;
9. lectura con conteo reconciliado y plan con `PartitionFilters`;
10. liberación de la caché con `unpersist()`.

## 12. Preguntas de reflexión

1. ¿Por qué resolver los duplicados de variables ambientales **antes** del `join` evita un problema que, si se dejara para después, sería mucho más difícil de rastrear hasta su causa?
2. ¿Qué hubiera pasado si `Valor_CM = 99999` no se filtrara, y ese valor entrara directo a un modelo de regresión en S4?
3. ¿Por qué `WindDir` se descartó por completo, en vez de rellenarla con algún valor por defecto?
4. ¿Por qué particionar por `AnioMes` tiene sentido acá, y por qué particionar por `FechaHora` completa (minuto a minuto) no serviría de nada?
5. ¿Qué relación hay entre esta salida particionada y el dataset que usa S4?

## 13. Lista de verificación técnica

- [ ] Las tres fuentes se leyeron con esquema explícito, cada una con su propia estructura.
- [ ] Los duplicados de la fuente ambiental se resolvieron antes del `join`, con un criterio de orden documentado.
- [ ] La integración usó `FechaHora` como clave, con el campo eléctrico como tabla principal.
- [ ] `WindDir` se identificó como 100 % nula y se descartó como columna, no como filas.
- [ ] `Valor_CM = 99999` se identificó como código de error y se filtró, distinto de un nulo.
- [ ] La tabla final se confirmó sin `FechaHora` duplicada.
- [ ] Las nueve variables críticas se validaron sin nulos antes de escribir la salida.
- [ ] La escritura particionó por una columna derivada de tiempo (`AnioMes`), no copiada de la fuente.
- [ ] La lectura de vuelta reconcilió el conteo de filas.
- [ ] El plan de ejecución confirmó `PartitionFilters` al filtrar por `AnioMes`.

## Bibliografía

1. Apache Software Foundation. (2024). *Apache Spark documentation*. https://spark.apache.org/docs/latest/
2. Apache Software Foundation. (2024). *PySpark API reference: pyspark.sql.functions*. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
3. Apache Software Foundation. (2024). *Parquet files*. Spark SQL Guide. https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
