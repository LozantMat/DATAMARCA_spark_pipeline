# DATAMARCA_spark_pipeline

Repositorio para el desarrollo de la Unidad 3: implementación de un pipeline distribuido con Apache Spark para **DATAMARCA S.A.**, cadena de supermercados con operación en Colombia, México y Perú.

---

## 1. Generación de datos sintéticos (`data_generator.py`)

DATAMARCA recibe información transaccional desde tres canales heterogéneos y sin un esquema común entre países. Como no se cuenta con datos reales, el script `data_generator.py` simula esos flujos de forma **reproducible**, en modalidad **batch** o **streaming**, para alimentar el pipeline de Spark.

El generador usa únicamente la librería estándar de Python (no requiere instalar dependencias).

### 1.1 Requisitos y ejecución

- Python 3.10 o superior.

```bash
# Batch: un archivo por canal por cada minuto simulado (120 minutos = 2 horas)
python data_generator.py batch --clean

# Streaming: micro-lotes cada 10 s simulados, a 60x la velocidad real (~2 min reales)
python data_generator.py stream --clean --speed 60
```

| Parámetro | Valor por defecto | Descripción |
|---|---|---|
| `--seed` | `42` | Semilla aleatoria. La misma semilla produce exactamente los mismos datos. |
| `--start` | `2026-10-26T13:00:00Z` | Inicio de la simulación en UTC (08:00 en Bogotá y Lima, 07:00 en Ciudad de México). |
| `--minutes` | `120` | Minutos simulados (2 horas). |
| `--tx-per-minute` | `2000` | Transacciones promedio por minuto. |
| `--dirty-rate` | `0.02` | Proporción de transacciones con un defecto de calidad inyectado. |
| `--late-scale` | `1.0` | Multiplicador de la probabilidad de llegada tardía. |
| `--out` | `data/batch` o `data/stream` | Carpeta de salida. |
| `--clean` | — | Borra la carpeta de salida antes de escribir. |
| `--quiet` | — | No imprime el resumen de cada ventana. |
| `--interval` (solo stream) | `10` | Segundos simulados por micro-lote (debe dividir 60). |
| `--speed` (solo stream) | `60` | Aceleración: `1` = tiempo real (2 h), `60` = 1 minuto simulado por segundo real, `0` = sin esperas. |

También puede usarse como módulo desde otro archivo de Python:

```python
from data_generator import DataGenerator

gen = DataGenerator(seed=42)

for ventana in gen.iter_batches():          # una ventana por minuto (~2000 tx)
    ventana.records["POS"]                  # lista de dict (filas)
    ventana.records["ECOMMERCE"]            # lista de dict (eventos anidados)
    ventana.records["LOYALTY_APP"]

for ventana in gen.iter_stream(10):         # micro-lotes de 10 s (~333 tx)
    ...

for canal, registro in gen.iter_events():   # evento a evento, p. ej. para Kafka
    ...
```

### 1.2 Fuentes simuladas

| Canal | Formato | Granularidad | Sistema origen |
|---|---|---|---|
| **POS** (tiendas físicas) | CSV con encabezado | Una fila por línea de producto | `POS-NCR-CO`, `POS-SAP-MX`, `POS-ORACLE-PE` |
| **E-commerce** | JSON Lines | Un evento `ORDER_PLACED` anidado por pedido | `ECOM-VTEX` |
| **App de fidelización** | JSON Lines | Un evento `SCAN_AND_GO_PURCHASE` por compra | `LOYALTY-APP-ANDROID` / `LOYALTY-APP-IOS` |

Esto cubre los formatos que pide la guía: registros tabulares CSV y logs semiestructurados JSON. El POS es plano (una fila por producto); los canales digitales traen los productos anidados en un arreglo `items`, por lo que el pipeline debe aplanarlos (`explode`) para unificarlos con el POS.

### 1.3 Campos relevantes para el pipeline

Todos los registros incluyen:

| Campo | Propósito |
|---|---|
| `source_channel`, `source_system` | Origen del dato (canal y sistema que lo produjo). |
| `country_code` | País (`CO`, `MX`, `PE`). Columna candidata de partición. |
| `event_ts_utc` | Momento en que ocurrió la transacción (tiempo de evento, en UTC). Base para ventanas y *watermarks*. |
| `event_ts_local` | El mismo instante en la hora local del país, con su desfase (`-05:00` / `-06:00`). |
| `emitted_ts_utc` | Momento simulado en que la fuente envió el registro. La diferencia con `event_ts_utc` es la latencia. |
| `event_date`, `event_hour` | Fecha y hora UTC del evento, precalculadas para particionar. |
| `batch_id` | Lote o micro-lote en el que llegó el registro (`B-AAAAMMDD-HHMM` o `S-AAAAMMDD-HHMMSS`). |
| `schema_version` | Versión del esquema de la fuente (`1.0`). |

**Estrategia de partición sugerida:** `country_code` / `event_date` (y opcionalmente `source_channel` o `event_hour`). Así las consultas de inventario por país y día aprovechan el *partition pruning* de Spark.

Otros campos de negocio: `transaction_id` / `order_id`, `store_id`, `city`, `warehouse_id` (CEDI que abastece la tienda), `customer_id`, `loyalty_id`, `sku`, `product_name`, `category`, `unit_of_measure`, `quantity`, `unit_price`, `discount_amount`, `tax_amount`, `line_total`, `currency`, `payment_method`, y por canal: `terminal_id` y `cashier_id` (POS); `session_id`, `device`, `fulfillment.delivery_type` (e-commerce); `member.tier` y `points` (fidelización).

### 1.4 Estructura de salida

```
data/
├── batch/
│   ├── _manifest.jsonl
│   ├── pos/ingest_date=2026-10-26/pos_20261026_1300.csv
│   ├── ecommerce/ingest_date=2026-10-26/ecommerce_20261026_1300.jsonl
│   └── loyalty/ingest_date=2026-10-26/loyalty_20261026_1300.jsonl
└── stream/
    ├── _manifest.jsonl
    ├── pos/part-000000-20261026T130000.csv
    ├── ecommerce/part-000000-20261026T130000.jsonl
    └── loyalty/part-000000-20261026T130000.jsonl
```

- En batch, las carpetas `ingest_date=...` usan el formato Hive, por lo que Spark agrega `ingest_date` como columna al leerlas.
- En streaming, cada archivo se escribe primero con un nombre oculto temporal y luego se renombra de forma atómica. Así Spark Structured Streaming (fuente de archivos) nunca lee un archivo a medio escribir.
- `_manifest.jsonl` registra por ventana: número de transacciones por canal, número de registros, llegadas tardías y defectos inyectados por tipo. Sirve como "respuesta esperada" para verificar las validaciones de calidad del pipeline.
- Una corrida completa ocupa unos 300 MB por modalidad. La carpeta `data/` está en `.gitignore` (solo se versiona `data/.gitkeep`); los datos se regeneran con el script.

### 1.5 Cómo funciona

1. **Generación por minuto.** Para cada minuto simulado se crea un generador aleatorio propio con semilla derivada de `(seed, minuto)`. Se generan ~2000 transacciones (distribución normal con desviación del 5 %), con tiempos de evento uniformes dentro del minuto.
2. **Construcción de la transacción.** Para cada una se elige canal, país, tienda, cliente, canasta de productos, precios, descuentos, impuestos y medio de pago, y luego se da el formato propio de su canal (fila CSV o evento JSON).
3. **Latencia de envío.** A cada transacción se le asigna un momento de emisión: normalmente unos segundos después del evento, o varios minutos después si es una llegada tardía.
4. **Entrega por ventanas.** Las transacciones se ordenan por momento de emisión y se entregan en ventanas: de 60 s en batch y de `--interval` segundos en streaming. Un registro tardío aparece en la ventana en la que *llega*, no en la que *ocurrió*, igual que en un sistema real.
5. **Reproducibilidad.** Como cada minuto tiene su propia semilla, batch y streaming generan **exactamente las mismas transacciones** (240 056 con la semilla 42); solo cambia el tamaño de la ventana de entrega. Dos ejecuciones con la misma semilla producen archivos idénticos.

Después de los 120 minutos aparecen algunas ventanas adicionales casi vacías: corresponden a registros tardíos que siguen llegando (en batch resultan 134 ventanas en lugar de 120).

---

## 2. Supuestos de la simulación

### 2.1 Volumen

- ~**2000 transacciones por minuto** en total (todos los canales y países), equivalente a ~33 por segundo.
- En streaming el volumen es proporcional: ~333 transacciones por micro-lote de 10 s.
- El volumen es constante durante las 2 horas (no se modelan horas pico).
- Una transacción del POS genera tantas filas como productos tenga la canasta (en promedio ~3), por lo que hay más filas que transacciones.

### 2.2 Distribución por canal y país

| Canal | Participación | | País | Participación |
|---|---|---|---|---|
| POS | 65 % | | México | 45 % |
| E-commerce | 25 % | | Colombia | 35 % |
| App de fidelización | 10 % | | Perú | 20 % |

### 2.3 Parámetros por país

| País | Moneda | Zona horaria | Impuesto | Tasa de cambio (por USD) | Ciudades (n.º de tiendas) | Medios de pago locales |
|---|---|---|---|---|---|---|
| Colombia | COP | UTC-5 | IVA 19 % | 4000 | Bogotá (12), Medellín (7), Cali (5), Barranquilla (4) | Nequi, Daviplata |
| México | MXN | UTC-6 | IVA 16 % | 18 | CDMX (14), Guadalajara (7), Monterrey (7), Puebla (4) | SPEI, CoDi |
| Perú | PEN | UTC-5 | IGV 18 % | 3.7 | Lima (12), Arequipa (4), Trujillo (3) | Yape, Plin |

- Cada ciudad tiene un centro de distribución (`CEDI-<país>-<ciudad>`) que abastece sus tiendas.
- Las zonas horarias son fijas (ninguno de los tres países aplica horario de verano).
- Todos los países usan las mismas columnas dentro de cada canal. La heterogeneidad entre países está en moneda, impuestos, hora local, sistema POS de origen y medios de pago.

### 2.4 Catálogo y precios

- 39 productos en 9 categorías: abarrotes, lácteos, frutas y verduras, carnes, bebidas, aseo, panadería, cuidado personal y mascotas.
- Cada producto tiene un precio base en USD que se convierte a moneda local con una variación aleatoria de ±5 % y se redondea a un múltiplo realista (50 COP, 0.50 MXN, 0.10 PEN).
- Frutas, verduras y carnes se venden por kilogramo (cantidades decimales); el resto por unidad.
- Los precios **incluyen impuestos**; `tax_amount` es el impuesto contenido en el total de la línea.
- El 15 % de las líneas tiene un descuento promocional del 5 % al 30 %.
- Tamaño de canasta: POS 1–8 productos, e-commerce 2–12, app 1–5.

### 2.5 Clientes

- 50 000 clientes por país; el 60 % está inscrito en el programa de fidelización.
- En el POS, el 55 % de las compras identifica al cliente; el resto son anónimas (`customer_id` vacío, lo cual es válido y **no** es un error).
- En la app de fidelización todas las compras son de clientes inscritos, con nivel (`BRONZE`, `SILVER`, `GOLD`, `PLATINUM`) y puntos ganados (1 punto por cada 1000 COP, 10 MXN o 3 PEN).

### 2.6 Latencia y datos tardíos

| Canal | Latencia normal (promedio) | Probabilidad de llegada tardía | Retraso tardío | Causa simulada |
|---|---|---|---|---|
| POS | 3 s | 3 % | 2 – 15 min | Tiendas que sincronizan un buffer local tras perder conexión |
| E-commerce | 0.8 s | 0.5 % | 1 – 5 min | Reintentos del servidor |
| App de fidelización | 2 s | 2 % | 1 – 15 min | Celulares sin señal que sincronizan después |

En total ~2.3 % de las transacciones llega tarde. Esto permite evidenciar el manejo de *late data* con *watermarks* en el modo streaming.

### 2.7 Defectos de calidad inyectados

Aproximadamente el **2 %** de las transacciones tiene un defecto, para que las validaciones de ingestión tengan algo que detectar:

| Defecto | Peso | Qué ocurre |
|---|---|---|
| `missing_required` | 25 % | Falta un campo obligatorio: `store_id` (POS), `warehouse_id` (e-commerce) o `loyalty_id` (app). |
| `duplicate` | 20 % | La misma transacción se reenvía entre 1 y 90 s después (mismo ID). |
| `negative_quantity` | 15 % | La primera línea tiene cantidad negativa. |
| `amount_mismatch` | 15 % | El `line_total` de la primera línea está multiplicado por 10 y no cuadra con cantidad × precio − descuento. |
| `invalid_country` | 10 % | Código de país en minúscula (`co`), de 3 letras (`COL`) o vacío. |
| `future_event_ts` | 8 % | El tiempo del evento está entre 1 y 30 días en el futuro. |
| `malformed` | 7 % | La línea del archivo está truncada (CSV con columnas faltantes o JSON inválido). Solo ocurre al escribir archivos. |

Los defectos no se marcan en los datos (el pipeline debe detectarlos por su cuenta); el conteo esperado por ventana está en `_manifest.jsonl`.

---

## 3. Ejecución en Databricks (serverless)

`pipeline.py`, `ingest.py` y `transform.py` detectan Databricks (variable `DATABRICKS_RUNTIME_VERSION`) y se adaptan solos; en local y en Dataproc el comportamiento no cambia. El cómputo serverless de Databricks usa Spark Connect, por lo que:

| Restricción de serverless | Cómo la resuelve el pipeline |
|---|---|
| No existe `spark.sparkContext` (ni acceso a la JVM) | Los archivos `_contract.json` / `_truth.jsonl` se leen con Python (los Volumes son rutas locales) |
| No se permite `cache()` / `unpersist()` | `materialize()` y `release()` no hacen nada en Databricks |
| Solo se pueden fijar algunas configuraciones de Spark | Se usa la sesión existente y solo se fijan `spark.sql.session.timeZone` y `spark.sql.shuffle.partitions` |
| No se puede desactivar `spark.sql.csv.parser.columnPruning.enabled` | DQ01 también marca como corrupta toda fila CSV cuya última columna (`schema_version`) llega vacía (una fila truncada siempre la pierde) |
| Unity Catalog no admite tablas sobre Volumes | Las capas se escriben como tablas Delta administradas `<catálogo>.<esquema>.<capa>_<nombre>` (`--tables`); los reportes JSON van a `<lake>/_reports` |

### 3.1 Preparación (una sola vez)

```sql
CREATE SCHEMA IF NOT EXISTS workspace.datamarca;
CREATE VOLUME IF NOT EXISTS workspace.datamarca.raw;
CREATE VOLUME IF NOT EXISTS workspace.datamarca.lake;
```

Luego: *Workspace → Create → Git folder* con la URL de este repositorio.

### 3.2 Job

*Jobs & Pipelines → Create job*, una tarea **Python script** (fuente *Workspace*, archivo `pipeline.py` de la Git folder), cómputo **Serverless** y parámetros:

```json
["--input", "/Volumes/workspace/datamarca/raw/batch",
 "--dims", "/Volumes/workspace/datamarca/raw/dims",
 "--lake", "/Volumes/workspace/datamarca/lake",
 "--tables", "workspace.datamarca",
 "--overwrite-generated"]
```

La etapa `generate` genera los datos en disco local y los copia al Volume (el generador renombra archivos de forma atómica). Para reutilizar datos ya generados se agrega `"--stages", "ingest", "transform"`; para pruebas de volumen, `"--scale", "2"` con otro `--input`.

Tablas resultantes en `workspace.datamarca`: `bronze_sales_lines`, `silver_sales_lines`, `quarantine_sales_lines`, `silver_fact_sales_lines`, `gold_daily_sales`, `gold_hourly_sales`, `gold_store_daily_kpis`, `gold_warehouse_daily_demand` y `gold_dq_daily_rejections`.

### 3.3 Evidencia

```sql
SELECT * FROM workspace.datamarca.gold_daily_sales ORDER BY sales_usd DESC;
SELECT dq_rule, dq_rule_name, SUM(lines) FROM workspace.datamarca.gold_dq_daily_rejections GROUP BY ALL;
DESCRIBE HISTORY workspace.datamarca.silver_fact_sales_lines;
EXPLAIN SELECT SUM(line_total_usd) FROM workspace.datamarca.silver_fact_sales_lines
WHERE country_code = 'CO';   -- PartitionFilters: partition pruning
```

El Spark UI no está disponible en serverless; el plan y los tiempos de cada consulta se ven en el *query profile*. Los tiempos por etapa quedan en la salida del job y en `/Volumes/workspace/datamarca/lake/_reports/`.

### 3.4 Exportar resultados para el equipo

`export_results.py` toma la última ejecución exitosa (según `<lake>/_reports`) y escribe en `<lake>/exports/<run_id>/` los CSV de resumen, tiempos, calidad (DQ01–DQ07), reconciliación y las cinco tablas gold, más un reporte HTML autocontenido y un `.zip` con todo. En Databricks se agrega como segunda tarea del job (*depends on* la tarea del pipeline), también de tipo **Python script** sobre cómputo **Serverless**:

```json
["--lake", "/Volumes/workspace/datamarca/lake",
 "--tables", "workspace.datamarca",
 "--sep", ";"]
```

`--sep ";"` hace que Excel en español abra los CSV en columnas; con `","` quedan listos para pandas o Power BI. Para exportar una ejecución anterior se agrega `"--run-id", "<run_id>"`. Los archivos se descargan desde *Catalog → workspace → datamarca → lake → exports*. En local: `python export_results.py --lake lake`.
