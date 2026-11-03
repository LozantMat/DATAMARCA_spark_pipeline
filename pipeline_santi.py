"""
DATAMARCA S.A. - Batch pipeline with Apache Spark (Unidad 3).

Stages
    1. Ingestion      : read POS (CSV) + e-commerce / loyalty (JSON Lines) with
                        explicit schemas, flatten nested items, apply data
                        quality rules and send rejected rows to quarantine.
    2. Transformation : deduplicate, enrich (country reference data + loyalty
                        tier join between sources) and build business
                        aggregates with the DataFrame API and SparkSQL.
    3. Persistence    : write Parquet (or Delta) partitioned by
                        country_code / event_date, plus quarantine and metrics.

Only native Spark functions are used (no Python UDFs) so Catalyst can apply
predicate pushdown, column pruning and whole-stage code generation.

Run locally:
    python pipeline.py --input data/batch --output output

Run on Databricks (serverless job, Python script task):
    parameters: --input /Volumes/workspace/datamarca/raw/batch
                --output /Volumes/workspace/datamarca/lake
                --tables workspace.datamarca

Run on Dataproc Serverless:
    gcloud dataproc batches submit pyspark pipeline.py --region=$REGION \
        --version=2.3 --deps-bucket=gs://$BUCKET \
        -- --input gs://$BUCKET/raw/batch --output gs://$BUCKET/lake
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# --------------------------------------------------------------------------- #
# Reference data and schemas
# --------------------------------------------------------------------------- #

VALID_COUNTRIES = ["CO", "MX", "PE"]
# country_code, currency, tax_rate, local units per USD (same as the generator)
COUNTRY_REF = [("CO", "COP", 0.19, 4000.0),
               ("MX", "MXN", 0.16, 18.0),
               ("PE", "PEN", 0.18, 3.7)]
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
LATE_THRESHOLD_S = 60          # arrives after its 1-minute batch window
AMOUNT_TOLERANCE = 0.05        # rounding tolerance for qty*price-discount

S, D, I = StringType(), DoubleType(), IntegerType()

# Databricks serverless runs on Spark Connect: no sparkContext, no DataFrame
# cache and only a few settable Spark configs. The pipeline adapts itself.
ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


def materialize(df):
    """Cache where allowed (local / Dataproc); no-op on Databricks serverless."""
    return df if ON_DATABRICKS else df.cache()


def _struct(*fields):
    return StructType([StructField(n, t, True) for n, t in fields])


TIME_FIELDS = [("event_ts_utc", S), ("event_ts_local", S),
               ("emitted_ts_utc", S), ("event_date", S), ("event_hour", I),
               ("batch_id", S), ("schema_version", S)]

POS_SCHEMA = _struct(
    ("transaction_id", S), ("line_number", I), ("source_channel", S),
    ("source_system", S), ("country_code", S), ("city", S), ("store_id", S),
    ("warehouse_id", S), ("terminal_id", S), ("cashier_id", S),
    ("customer_id", S), ("loyalty_id", S), ("sku", S), ("product_name", S),
    ("category", S), ("unit_of_measure", S), ("quantity", D),
    ("unit_price", D), ("discount_amount", D), ("tax_amount", D),
    ("line_total", D), ("currency", S), ("payment_method", S),
    *TIME_FIELDS, ("_corrupt_record", S))

ITEM = _struct(("sku", S), ("product_name", S), ("category", S),
               ("unit_of_measure", S), ("quantity", D), ("unit_price", D),
               ("discount_amount", D), ("line_total", D))

ECOM_SCHEMA = _struct(
    ("event_id", S), ("event_type", S), ("order_id", S),
    ("source_channel", S), ("source_system", S), ("country_code", S),
    ("session_id", S),
    ("customer", _struct(("customer_id", S), ("loyalty_id", S))),
    ("device", _struct(("platform", S), ("app_version", S))),
    ("fulfillment", _struct(("delivery_type", S), ("store_id", S),
                            ("warehouse_id", S), ("city", S))),
    ("items", ArrayType(ITEM)),
    ("payment", _struct(("method", S), ("currency", S),
                        ("discount_total", D), ("tax_total", D),
                        ("total", D))),
    *TIME_FIELDS, ("_corrupt_record", S))

LOYALTY_SCHEMA = _struct(
    ("event_id", S), ("event_type", S), ("transaction_id", S),
    ("source_channel", S), ("source_system", S), ("country_code", S),
    ("member", _struct(("loyalty_id", S), ("customer_id", S), ("tier", S))),
    ("store_id", S), ("city", S), ("app_version", S),
    ("items", ArrayType(ITEM)),
    ("amount", _struct(("currency", S), ("total", D), ("tax_total", D),
                       ("discount_total", D))),
    ("points", _struct(("earned", I), ("redeemed", I))),
    ("payment_method", S),
    *TIME_FIELDS, ("_corrupt_record", S))

# Canonical line-level columns shared by the three channels
CANONICAL = ["transaction_id", "line_number", "source_channel",
             "source_system", "country_code", "city", "store_id",
             "warehouse_id", "customer_id", "loyalty_id", "sku",
             "product_name", "category", "unit_of_measure", "quantity",
             "unit_price", "discount_amount", "tax_amount", "line_total",
             "currency", "payment_method", "event_ts_utc", "emitted_ts_utc",
             "batch_id", "schema_version", "required_ok", "delivery_type",
             "device_platform", "member_tier", "points_earned",
             "_corrupt_record"]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

class StageTimer:
    """Collects wall-clock time per pipeline stage."""

    def __init__(self):
        self.stages: dict[str, float] = {}
        self._t0 = time.perf_counter()

    def run(self, name, fn, *args):
        t = time.perf_counter()
        result = fn(*args)
        self.stages[name] = round(time.perf_counter() - t, 2)
        print(f"[timing] {name}: {self.stages[name]} s", flush=True)
        return result

    @property
    def total(self):
        return round(time.perf_counter() - self._t0, 2)


def build_spark(fmt: str, shuffle_partitions: int | None) -> SparkSession:
    if ON_DATABRICKS:                     # session already exists, Delta built in
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        if shuffle_partitions:
            spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        return spark
    b = (SparkSession.builder.appName("datamarca-batch-pipeline")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if shuffle_partitions:
        b = b.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    if fmt == "delta":
        b = (b.config("spark.sql.extensions",
                      "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return b.getOrCreate()


def _ts(col):
    return F.to_timestamp(col, TS_FMT)


# --------------------------------------------------------------------------- #
# 1. Ingestion
# --------------------------------------------------------------------------- #

def read_pos(spark, base) -> DataFrame:
    raw = (spark.read.schema(POS_SCHEMA)
           .option("header", True)
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt_record")
           .csv(f"{base}/pos"))
    return (raw
            .withColumn("required_ok", F.col("store_id").isNotNull())
            .withColumn("delivery_type", F.lit(None).cast(S))
            .withColumn("device_platform", F.lit(None).cast(S))
            .withColumn("member_tier", F.lit(None).cast(S))
            .withColumn("points_earned", F.lit(None).cast(I))
            .select(*CANONICAL))


def _read_json(spark, path, schema) -> DataFrame:
    return (spark.read.schema(schema)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(path))


def read_ecommerce(spark, base) -> DataFrame:
    raw = _read_json(spark, f"{base}/ecommerce", ECOM_SCHEMA)
    # posexplode_outer keeps malformed rows (items = null) for quarantine
    flat = raw.select("*", F.posexplode_outer("items").alias("pos", "it"))
    return flat.select(
        F.col("order_id").alias("transaction_id"),
        (F.col("pos") + 1).alias("line_number"),
        "source_channel", "source_system", "country_code",
        F.col("fulfillment.city").alias("city"),
        F.col("fulfillment.store_id").alias("store_id"),
        F.col("fulfillment.warehouse_id").alias("warehouse_id"),
        F.col("customer.customer_id").alias("customer_id"),
        F.col("customer.loyalty_id").alias("loyalty_id"),
        F.col("it.sku").alias("sku"),
        F.col("it.product_name").alias("product_name"),
        F.col("it.category").alias("category"),
        F.col("it.unit_of_measure").alias("unit_of_measure"),
        F.col("it.quantity").alias("quantity"),
        F.col("it.unit_price").alias("unit_price"),
        F.col("it.discount_amount").alias("discount_amount"),
        F.lit(None).cast(D).alias("tax_amount"),
        F.col("it.line_total").alias("line_total"),
        F.col("payment.currency").alias("currency"),
        F.col("payment.method").alias("payment_method"),
        "event_ts_utc", "emitted_ts_utc", "batch_id", "schema_version",
        F.col("fulfillment.warehouse_id").isNotNull().alias("required_ok"),
        F.col("fulfillment.delivery_type").alias("delivery_type"),
        F.col("device.platform").alias("device_platform"),
        F.lit(None).cast(S).alias("member_tier"),
        F.lit(None).cast(I).alias("points_earned"),
        "_corrupt_record")


def read_loyalty(spark, base) -> DataFrame:
    raw = _read_json(spark, f"{base}/loyalty", LOYALTY_SCHEMA)
    flat = raw.select("*", F.posexplode_outer("items").alias("pos", "it"))
    # The app does not send the CEDI: derive it from the store (CO-BOG-001 -> CEDI-CO-BOG)
    warehouse = F.concat(F.lit("CEDI-"), F.substring_index("store_id", "-", 2))
    return flat.select(
        "transaction_id", (F.col("pos") + 1).alias("line_number"),
        "source_channel", "source_system", "country_code", "city",
        "store_id", warehouse.alias("warehouse_id"),
        F.col("member.customer_id").alias("customer_id"),
        F.col("member.loyalty_id").alias("loyalty_id"),
        F.col("it.sku").alias("sku"),
        F.col("it.product_name").alias("product_name"),
        F.col("it.category").alias("category"),
        F.col("it.unit_of_measure").alias("unit_of_measure"),
        F.col("it.quantity").alias("quantity"),
        F.col("it.unit_price").alias("unit_price"),
        F.col("it.discount_amount").alias("discount_amount"),
        F.lit(None).cast(D).alias("tax_amount"),
        F.col("it.line_total").alias("line_total"),
        F.col("amount.currency").alias("currency"),
        "payment_method", "event_ts_utc", "emitted_ts_utc", "batch_id",
        "schema_version",
        F.col("member.loyalty_id").isNotNull().alias("required_ok"),
        F.lit(None).cast(S).alias("delivery_type"),
        F.lit(None).cast(S).alias("device_platform"),
        F.col("member.tier").alias("member_tier"),
        F.col("points.earned").alias("points_earned"),
        "_corrupt_record")


def validate(df: DataFrame) -> DataFrame:
    """Adds parsed timestamps and a `dq_reason` column (null = valid row).
    Rules are evaluated in priority order; the first failing rule wins."""
    df = (df.withColumn("event_ts", _ts("event_ts_utc"))
            .withColumn("emitted_ts", _ts("emitted_ts_utc")))

    malformed = (F.col("_corrupt_record").isNotNull()
                 | F.col("schema_version").isNull()
                 | F.col("transaction_id").isNull()
                 | F.col("sku").isNull()
                 | F.col("quantity").isNull()
                 | F.col("unit_price").isNull()
                 | F.col("line_total").isNull()
                 | F.col("event_ts").isNull()
                 | F.col("emitted_ts").isNull())
    bad_country = ~F.coalesce(F.col("country_code").isin(VALID_COUNTRIES),
                              F.lit(False))
    expected_total = (F.col("quantity") * F.col("unit_price")
                      - F.coalesce(F.col("discount_amount"), F.lit(0.0)))
    mismatch = (F.abs(expected_total - F.col("line_total"))
                > F.lit(AMOUNT_TOLERANCE) + F.abs(expected_total) * 0.001)

    reason = (F.when(malformed, "malformed")
               .when(~F.col("required_ok"), "missing_required")
               .when(bad_country, "invalid_country")
               .when(F.col("quantity") <= 0, "negative_quantity")
               .when(mismatch, "amount_mismatch")
               .when(F.col("event_ts") > F.col("emitted_ts"), "future_event_ts"))
    return df.withColumn("dq_reason", reason)


def deduplicate(valid: DataFrame):
    """Client/POS retries resend the same transaction: keep the first arrival."""
    w = Window.partitionBy("source_channel", "transaction_id", "line_number") \
              .orderBy("emitted_ts", "batch_id")
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    clean = ranked.filter("_rn = 1").drop("_rn")
    dups = (ranked.filter("_rn > 1").drop("_rn")
            .withColumn("dq_reason", F.lit("duplicate")))
    return clean, dups


def ingest(spark, base):
    raw = (read_pos(spark, base)
           .unionByName(read_ecommerce(spark, base))
           .unionByName(read_loyalty(spark, base)))
    checked = materialize(validate(raw))
    total = checked.count()                      # materialize the stage
    valid = checked.filter(F.col("dq_reason").isNull())
    rejected = checked.filter(F.col("dq_reason").isNotNull())
    clean, dups = deduplicate(valid)
    clean = materialize(clean)
    n_clean = clean.count()
    quarantine = rejected.unionByName(dups)
    return checked, clean, quarantine, {"records_read": total,
                                        "records_valid": n_clean}


# --------------------------------------------------------------------------- #
# 2. Transformation
# --------------------------------------------------------------------------- #

def transform(spark, clean: DataFrame):
    country = F.broadcast(spark.createDataFrame(
        COUNTRY_REF, "country_code string, ref_currency string, "
                     "tax_rate double, fx_per_usd double"))

    # Join between sources: latest loyalty tier per member, taken from the
    # loyalty app, applied to POS and e-commerce purchases of the same member.
    members = (clean.filter(F.col("member_tier").isNotNull())
               .groupBy("loyalty_id")
               .agg(F.max_by("member_tier", "emitted_ts").alias("tier_from_app")))

    fact = materialize(clean.join(country, "country_code", "left")
            .join(members, "loyalty_id", "left")
            .withColumn("tax_amount", F.coalesce(
                "tax_amount",
                F.round(F.col("line_total")
                        - F.col("line_total") / (1 + F.col("tax_rate")), 2)))
            .withColumn("line_total_usd",
                        F.round(F.col("line_total") / F.col("fx_per_usd"), 4))
            .withColumn("discount_usd",
                        F.round(F.col("discount_amount") / F.col("fx_per_usd"), 4))
            .withColumn("customer_tier", F.coalesce(
                "member_tier", "tier_from_app",
                F.when(F.col("customer_id").isNull(), "ANONYMOUS")
                 .otherwise("NON_MEMBER")))
            .withColumn("latency_s", F.col("emitted_ts").cast("double")
                        - F.col("event_ts").cast("double"))
            .withColumn("is_late", F.col("latency_s") > LATE_THRESHOLD_S)
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("event_hour", F.hour("event_ts"))
            .select("transaction_id", "line_number", "source_channel",
                    "source_system", "country_code", "city", "store_id",
                    "warehouse_id", "customer_id", "loyalty_id",
                    "customer_tier", "sku", "product_name", "category",
                    "unit_of_measure", "quantity", "unit_price",
                    "discount_amount", "tax_amount", "line_total", "currency",
                    "line_total_usd", "discount_usd", "payment_method",
                    "delivery_type", "device_platform", "points_earned",
                    "event_ts", "emitted_ts", "latency_s", "is_late",
                    "batch_id", "event_date", "event_hour"))

    # DataFrame API: daily sales by country / channel / category
    sales_daily = (fact.groupBy("event_date", "country_code",
                                "source_channel", "category")
                   .agg(F.countDistinct("transaction_id").alias("transactions"),
                        F.count("*").alias("lines"),
                        F.round(F.sum("line_total"), 2).alias("revenue_local"),
                        F.round(F.sum("line_total_usd"), 2).alias("revenue_usd"),
                        F.round(F.sum("discount_usd"), 2).alias("discount_usd")))

    # DataFrame API: hourly demand per CEDI and SKU (replenishment input)
    inventory = (fact.groupBy("event_date", "event_hour", "country_code",
                              "warehouse_id", "sku", "product_name",
                              "unit_of_measure")
                 .agg(F.round(F.sum("quantity"), 3).alias("quantity_sold"),
                      F.count("*").alias("lines")))

    # SparkSQL: ticket and revenue by loyalty tier
    fact.createOrReplaceTempView("fact_sales_lines")
    tiers = spark.sql("""
        SELECT country_code, event_date, source_channel, customer_tier,
               COUNT(DISTINCT transaction_id)                     AS transactions,
               COUNT(DISTINCT customer_id)                        AS customers,
               ROUND(SUM(line_total_usd), 2)                      AS revenue_usd,
               ROUND(SUM(line_total_usd)
                     / COUNT(DISTINCT transaction_id), 2)         AS avg_ticket_usd
        FROM fact_sales_lines
        GROUP BY country_code, event_date, source_channel, customer_tier
    """)

    outputs = {"fact_sales_lines": fact,
               "agg_sales_daily": materialize(sales_daily),
               "agg_inventory_demand_hourly": materialize(inventory),
               "agg_customer_tier": materialize(tiers)}
    counts = {name: df.count() for name, df in outputs.items()}
    counts["late_lines"] = fact.filter("is_late").count()
    return outputs, counts


# --------------------------------------------------------------------------- #
# 3. Persistence
# --------------------------------------------------------------------------- #

PARTITIONS = {"fact_sales_lines": ["country_code", "event_date"],
              "agg_sales_daily": ["country_code", "event_date"],
              "agg_inventory_demand_hourly": ["country_code", "event_date"],
              "agg_customer_tier": ["country_code"]}


def _save(df, cols, fmt, path, table):
    w = (df.repartition(*cols)            # one file per partition, no tiny files
           .write.format(fmt).mode("overwrite").partitionBy(*cols))
    if table:                             # Unity Catalog / metastore table
        w.option("overwriteSchema", "true").saveAsTable(table)
    else:
        w.save(path)


def persist(outputs, quarantine, out, fmt, tables=None):
    for name, df in outputs.items():
        _save(df, PARTITIONS[name], fmt, f"{out}/curated/{name}",
              f"{tables}.{name}" if tables else None)
    q = (quarantine.select("dq_reason", "source_channel", "transaction_id",
                       "line_number", "country_code", "store_id",
                       "warehouse_id", "loyalty_id", "sku", "quantity",
                       "unit_price", "discount_amount", "line_total",
                       "event_ts_utc", "emitted_ts_utc", "batch_id",
                       "_corrupt_record"))
    _save(q, ["dq_reason"], fmt, f"{out}/quarantine/rejected_records",
          f"{tables}.quarantine_rejected_records" if tables else None)


# --------------------------------------------------------------------------- #
# Quality report (compared against the generator manifest)
# --------------------------------------------------------------------------- #

def _read_manifest_lines(spark, base):
    """Spark's file sources skip names starting with "_", so the small manifest
    is read directly: plain Python for local disk and Databricks /Volumes,
    Hadoop FileSystem API (through the JVM) for gs:// and other URIs."""
    if "://" not in base or base.startswith("file:"):
        with open(f"{base.replace('file:', '')}/_manifest.jsonl",
                  encoding="utf-8") as fh:
            return fh.readlines()
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{base}/_manifest.jsonl")
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    reader = jvm.java.io.BufferedReader(
        jvm.java.io.InputStreamReader(fs.open(path), "UTF-8"))
    lines, line = [], reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return lines


def quality_report(spark, quarantine, base, use_manifest):
    rows = (quarantine.groupBy("dq_reason")
            .agg(F.count("*").alias("records"),
                 F.countDistinct("source_channel", "transaction_id")
                  .alias("transactions"))
            .collect())
    detected = {r["dq_reason"]: {"records": r["records"],
                                 "transactions": r["transactions"]}
                for r in rows}
    expected = {}
    if use_manifest:
        try:
            for line in _read_manifest_lines(spark, base):
                for k, v in json.loads(line).get("injected_defects", {}).items():
                    expected[k] = expected.get(k, 0) + int(v)
        except Exception as exc:          # manifest is optional
            print(f"[warn] manifest not read: {exc}")
    return {"detected": detected, "expected_from_manifest": expected}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    p = argparse.ArgumentParser(description="DATAMARCA batch pipeline")
    p.add_argument("--input", default="data/batch",
                   help="Folder with pos/, ecommerce/, loyalty/ (local or gs://)")
    p.add_argument("--output", default="output",
                   help="Base output folder (local or gs://)")
    p.add_argument("--format", choices=["parquet", "delta"], default=None,
                   help="Default: delta on Databricks, parquet elsewhere")
    p.add_argument("--tables", default=None,
                   help="Also register outputs as tables, e.g. workspace.datamarca")
    p.add_argument("--run-id", default=None)
    p.add_argument("--shuffle-partitions", type=int, default=None)
    p.add_argument("--no-manifest", action="store_true")
    a = p.parse_args()
    a.format = a.format or ("delta" if ON_DATABRICKS else "parquet")

    run_id = a.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") \
        + "-" + uuid.uuid4().hex[:6]
    spark = build_spark(a.format, a.shuffle_partitions)
    if not ON_DATABRICKS:
        spark.sparkContext.setLogLevel("WARN")
    timer = StageTimer()

    checked, clean, quarantine, ing = timer.run("ingestion", ingest, spark, a.input)
    outputs, counts = timer.run("transformation", transform, spark, clean)
    timer.run("persistence", persist, outputs, quarantine, a.output, a.format,
              a.tables)
    dq = quality_report(spark, quarantine, a.input, not a.no_manifest)

    metrics = {
        "run_id": run_id, "input": a.input, "output": a.output,
        "format": a.format, "spark_version": spark.version,
        "platform": "databricks" if ON_DATABRICKS else "spark",
        "stage_seconds": timer.stages, "total_seconds": timer.total,
        "counts": {**ing, **counts}, "data_quality": dq,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
    }
    print("[metrics] " + json.dumps(metrics, indent=2), flush=True)
    (spark.createDataFrame([(json.dumps(metrics),)], "value string")
     .coalesce(1).write.mode("overwrite").text(f"{a.output}/_metrics/run={run_id}"))
    if not ON_DATABRICKS:                 # never stop the shared session
        spark.stop()


if __name__ == "__main__":
    main()
