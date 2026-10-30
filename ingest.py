"""
Ingestion + data-quality stage of the DATAMARCA Spark pipeline (Unidad 3).

Reads the three raw feeds produced by ``data_generator.py`` (POS CSV,
e-commerce and loyalty JSON Lines), unifies them at line-item level, applies
the quality rules DQ01-DQ09 declared in ``_contract.json`` and writes a
medallion layout:

    <lake>/bronze/sales_lines      every raw line + ingestion metadata
    <lake>/silver/sales_lines      valid, deduplicated lines
                                   (partitioned by country_code / event_date)
    <lake>/quarantine/sales_lines  rejected lines + the rule they failed
    <lake>/_reports/ingest_<run_id>.json
                                   DQ report vs ground truth + stage timings

Quality rules are evaluated in contract order and a record is tagged with the
FIRST rule it fails. Header-level defects (DQ01-DQ06) are propagated to every
line of the same transaction, so a transaction is either fully accepted or
fully quarantined (totals never get half a basket).

Usage:
    python ingest.py --input data/batch --lake lake
    python ingest.py --input data/batch --lake lake --format delta
    spark-submit ingest.py --input gs://<bucket>/raw/batch --lake gs://<bucket>/lake

Databricks (serverless job / notebook): outputs become Unity Catalog Delta
tables named <layer>_<name> (e.g. workspace.datamarca.silver_sales_lines);
reports still go to <lake>/_reports (use a /Volumes/... path as --lake):
    python ingest.py --input /Volumes/workspace/datamarca/raw/batch \
        --dims /Volumes/workspace/datamarca/raw/dims \
        --lake /Volumes/workspace/datamarca/lake --tables workspace.datamarca
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

CHANNEL_DIRS = {"POS": "pos", "ECOMMERCE": "ecommerce", "LOYALTY_APP": "loyalty"}
VALID_COUNTRIES = ("CO", "MX", "PE")
AMOUNT_TOLERANCE = 0.02           # DQ05, same value as the contract
FUTURE_MARGIN = "INTERVAL 5 MINUTES"  # DQ06 clock-skew margin
LATE_THRESHOLD_S = 60             # late-data definition in the contract

ITEM_COLS = ["sku", "product_name", "category", "unit_of_measure", "quantity",
             "unit_price", "discount_amount", "tax_amount", "line_total"]

# --------------------------------------------------------------------------- #
# Runtime: Databricks serverless vs classic Spark (local / Dataproc)
# --------------------------------------------------------------------------- #

# Databricks serverless runs on Spark Connect: no sparkContext / JVM access,
# no DataFrame cache and only a few settable Spark configs. Everything else in
# the pipeline is identical; these helpers hide the differences.
ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
DEFAULT_DATABRICKS_TABLES = "workspace.datamarca"
# Unity Catalog managed tables are Delta; elsewhere tables follow --format
TABLE_FORMAT: str | None = "delta" if ON_DATABRICKS else None
_TABLES: str | None = None   # "<catalog>.<schema>" -> write/read tables, not paths


def configure_tables(tables: str | None) -> str | None:
    """Register lake datasets as tables in ``<catalog>.<schema>``.
    Mandatory on Databricks (Unity Catalog cannot use Volumes as table
    locations), optional elsewhere."""
    global _TABLES
    _TABLES = tables or (DEFAULT_DATABRICKS_TABLES if ON_DATABRICKS else None)
    return _TABLES


def table_name(path: str) -> str:
    """<lake>/silver/sales_lines -> <catalog>.<schema>.silver_sales_lines"""
    layer, name = path.rstrip("/").split("/")[-2:]
    return f"{_TABLES}.{layer}_{name}"


def materialize(df: DataFrame) -> DataFrame:
    """cache() where supported; no-op on Databricks serverless."""
    return df if ON_DATABRICKS else df.cache()


def release(*dfs: DataFrame) -> None:
    if not ON_DATABRICKS:
        for df in dfs:
            df.unpersist()


def default_parallelism(spark: SparkSession):
    return None if ON_DATABRICKS else spark.sparkContext.defaultParallelism


# Unified line-level schema shared by the three channels
UNIFIED_COLS = [
    "source_channel", "source_system", "transaction_id", "line_number",
    "country_code", "city", "store_id", "warehouse_id", "customer_id",
    "loyalty_id", *ITEM_COLS, "currency", "payment_method",
    "event_ts_utc", "emitted_ts_utc", "batch_id",
    "_corrupt_record", "_source_file",
]

SILVER_COLS = [
    "source_channel", "source_system", "transaction_id", "line_number",
    "country_code", "city", "store_id", "warehouse_id", "customer_id",
    "loyalty_id", *ITEM_COLS, "currency", "payment_method",
    "event_ts_utc", "emitted_ts_utc", "event_date", "event_hour",
    "arrival_delay_s", "is_late", "batch_id", "dq_flags",
    "_source_file", "_ingest_run_id", "_ingested_at",
]

# DQ02 required fields, expressed on the unified columns
# (contract paths such as fulfillment.warehouse_id are flattened here)
REQUIRED = {
    "POS": ["transaction_id", "store_id", "warehouse_id", "sku", "quantity",
            "unit_price", "line_total", "event_ts_utc"],
    "ECOMMERCE": ["transaction_id", "store_id", "warehouse_id", "sku",
                  "quantity", "unit_price", "line_total", "event_ts_utc"],
    "LOYALTY_APP": ["transaction_id", "store_id", "loyalty_id", "sku",
                    "quantity", "unit_price", "line_total", "event_ts_utc"],
}


# --------------------------------------------------------------------------- #
# Spark session
# --------------------------------------------------------------------------- #

def build_spark(fmt: str, master: str | None = None,
                shuffle_partitions: int = 16) -> SparkSession:
    if ON_DATABRICKS:
        # The session already exists (Delta built in). Only a few configs can
        # be set on serverless; an unsupported one would fail the job.
        spark = SparkSession.builder.getOrCreate()
        for key, value in (("spark.sql.session.timeZone", "UTC"),
                           ("spark.sql.shuffle.partitions", str(shuffle_partitions))):
            try:
                spark.conf.set(key, value)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: {key} not settable here ({exc})")
        return spark
    builder = (SparkSession.builder.appName("datamarca-ingest")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               # Without this, a CSV row truncated in a column we do not select
               # is NOT reported in _corrupt_record (DQ01 would miss it)
               .config("spark.sql.csv.parser.columnPruning.enabled", "false")
               .config("spark.ui.showConsoleProgress", "false"))
    if master:
        builder = builder.master(master)
    if fmt == "delta":
        builder = (builder
                   .config("spark.sql.extensions",
                           "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        try:  # local runs: pip install delta-spark; on Dataproc use --packages
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# Read + normalise (raw feeds -> unified line-level DataFrame)
# --------------------------------------------------------------------------- #

def read_text_file(spark: SparkSession, path: str) -> str | None:
    """Read a small text file through the Hadoop FileSystem API.

    Spark's DataFrame readers skip files whose name starts with ``_`` (such as
    ``_contract.json``), so metadata files are opened directly. Works for
    local paths and for gs:// / s3a:// / abfss:// alike.
    """
    if "://" not in path:  # local disk or Databricks /Volumes (FUSE mount)
        local = path[len("dbfs:"):] if path.startswith("dbfs:") else path
        if not Path(local).exists():
            return None
        return Path(local).read_text(encoding="utf-8")
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        return None
    stream = fs.open(hpath)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def load_contract(spark: SparkSession, input_dir: str) -> dict:
    """The contract travels with the data (``<input>/_contract.json``)."""
    text = read_text_file(spark, f"{input_dir}/_contract.json")
    if text is None:
        raise FileNotFoundError(f"{input_dir}/_contract.json not found: "
                                "regenerate the data with data_generator.py")
    return json.loads(text)


def load_truth(spark: SparkSession, input_dir: str) -> DataFrame | None:
    """Ground truth written by the generator (``_truth.jsonl``), if present."""
    text = read_text_file(spark, f"{input_dir}/_truth.jsonl")
    if not text:
        return None
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    return spark.createDataFrame(
        [(r["source_channel"], r["transaction_id"], r["defect"], r["is_late"])
         for r in rows],
        "source_channel STRING, transaction_id STRING, defect STRING, is_late BOOLEAN")


def _reader(spark: SparkSession, ddl: str):
    return (spark.read.schema(f"{ddl}, _corrupt_record STRING")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record"))


def read_pos(spark: SparkSession, path: str, ddl: str) -> DataFrame:
    raw = _reader(spark, ddl).option("header", True).csv(path)
    # A truncated CSV row always loses its last column. Checking it keeps DQ01
    # exact even where spark.sql.csv.parser.columnPruning.enabled cannot be
    # turned off (Databricks serverless).
    last_col = ddl.split(",")[-1].split()[0]
    corrupt = F.coalesce(F.col("_corrupt_record"),
                         F.when(F.col(last_col).isNull(), F.lit("TRUNCATED_ROW")))
    return raw.withColumn("_corrupt_record", corrupt).select(
        F.lit("POS").alias("source_channel"), "source_system",
        "transaction_id", "line_number", "country_code", "city", "store_id",
        "warehouse_id", "customer_id", "loyalty_id", *ITEM_COLS,
        "currency", "payment_method", "event_ts_utc", "emitted_ts_utc",
        "batch_id", "_corrupt_record",
        F.col("_metadata.file_path").alias("_source_file"))


def read_ecommerce(spark: SparkSession, path: str, ddl: str) -> DataFrame:
    raw = _reader(spark, ddl).json(path)
    return (raw.select("*", F.col("_metadata.file_path").alias("_source_file"))
            .select(
                F.lit("ECOMMERCE").alias("source_channel"), "source_system",
                F.col("order_id").alias("transaction_id"), "country_code",
                F.col("fulfillment.city").alias("city"),
                F.col("fulfillment.store_id").alias("store_id"),
                F.col("fulfillment.warehouse_id").alias("warehouse_id"),
                F.col("customer.customer_id").alias("customer_id"),
                F.col("customer.loyalty_id").alias("loyalty_id"),
                F.posexplode_outer("items").alias("pos", "item"),
                F.col("payment.currency").alias("currency"),
                F.col("payment.method").alias("payment_method"),
                "event_ts_utc", "emitted_ts_utc", "batch_id",
                "_corrupt_record", "_source_file")
            .select("*", *[F.col(f"item.{c}").alias(c) for c in ITEM_COLS],
                    (F.col("pos") + 1).alias("line_number"))
            .select(*UNIFIED_COLS))


def read_loyalty(spark: SparkSession, path: str, ddl: str) -> DataFrame:
    raw = _reader(spark, ddl).json(path)
    return (raw.select("*", F.col("_metadata.file_path").alias("_source_file"))
            .select(
                F.lit("LOYALTY_APP").alias("source_channel"), "source_system",
                "transaction_id", "country_code", "city", "store_id",
                F.lit(None).cast("string").alias("warehouse_id"),  # from dim_stores
                F.col("member.customer_id").alias("customer_id"),
                F.col("member.loyalty_id").alias("loyalty_id"),
                F.posexplode_outer("items").alias("pos", "item"),
                F.col("amount.currency").alias("currency"), "payment_method",
                "event_ts_utc", "emitted_ts_utc", "batch_id",
                "_corrupt_record", "_source_file")
            .select("*", *[F.col(f"item.{c}").alias(c) for c in ITEM_COLS],
                    (F.col("pos") + 1).alias("line_number"))
            .select(*UNIFIED_COLS))


def read_raw(spark: SparkSession, input_dir: str, contract: dict,
             run_id: str) -> DataFrame:
    ddl = {ch: contract["channels"][ch]["spark_ddl"] for ch in CHANNEL_DIRS}
    parts = [
        read_pos(spark, f"{input_dir}/{CHANNEL_DIRS['POS']}", ddl["POS"]),
        read_ecommerce(spark, f"{input_dir}/{CHANNEL_DIRS['ECOMMERCE']}",
                       ddl["ECOMMERCE"]),
        read_loyalty(spark, f"{input_dir}/{CHANNEL_DIRS['LOYALTY_APP']}",
                     ddl["LOYALTY_APP"]),
    ]
    lines = reduce(lambda a, b: a.unionByName(b), [p.select(*UNIFIED_COLS) for p in parts])
    return (lines
            .withColumn("_ingest_run_id", F.lit(run_id))
            .withColumn("_ingested_at", F.current_timestamp()))


# --------------------------------------------------------------------------- #
# Quality rules
# --------------------------------------------------------------------------- #

def _any_missing(cols: list[str]):
    return reduce(lambda a, b: a | b,
                  [F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
                   for c in cols])


def apply_quality_rules(lines: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Tag every line with ``dq_rule`` (first failed rule, null = valid) and
    ``dq_flags`` (non-blocking rules DQ08/DQ09)."""
    ch = F.col("source_channel")
    missing = (F.when(ch == "POS", _any_missing(REQUIRED["POS"]))
                .when(ch == "ECOMMERCE", _any_missing(REQUIRED["ECOMMERCE"]))
                .otherwise(_any_missing(REQUIRED["LOYALTY_APP"])))
    bad_country = ~F.coalesce(F.col("country_code").isin(*VALID_COUNTRIES),
                              F.lit(False))
    expected_total = F.col("quantity") * F.col("unit_price") - F.col("discount_amount")

    line_rule = (
        F.when(F.col("_corrupt_record").isNotNull(), "DQ01")
         .when(missing, "DQ02")
         .when(bad_country, "DQ03")
         .when(F.col("quantity") <= 0, "DQ04")
         .when(F.abs(F.col("line_total") - expected_total) > AMOUNT_TOLERANCE, "DQ05")
         .when(F.col("event_ts_utc") > F.col("emitted_ts_utc") + F.expr(FUTURE_MARGIN),
               "DQ06"))

    # Propagate to the whole transaction: "DQ0x" strings sort in rule order,
    # so the minimum is the first rule failed by any of its lines.
    tx = Window.partitionBy("source_channel", "transaction_id")
    tagged = (lines.withColumn("_line_rule", line_rule)
              .withColumn("dq_rule", F.coalesce(F.min("_line_rule").over(tx),
                                                F.col("_line_rule"))))

    # DQ07: keep the earliest emission of each business key among valid lines
    key = (Window.partitionBy("source_channel", "transaction_id", "line_number")
           .orderBy(F.col("dq_rule").isNotNull(), "emitted_ts_utc", "batch_id",
                    "_source_file"))
    tagged = (tagged.withColumn("_rn", F.row_number().over(key))
              .withColumn("dq_rule", F.when(F.col("dq_rule").isNull()
                                            & (F.col("_rn") > 1), "DQ07")
                                      .otherwise(F.col("dq_rule"))))

    # DQ08 / DQ09: non-blocking checks against the dimension tables
    countries = dims["dim_countries"].select(
        "country_code", F.col("currency").alias("_dim_currency"))
    stores = dims["dim_stores"].select(
        "store_id", F.col("warehouse_id").alias("_dim_warehouse"))
    products = dims["dim_products"].select("sku", F.lit(True).alias("_dim_sku"))
    tagged = (tagged
              .join(F.broadcast(countries), "country_code", "left")
              .join(F.broadcast(stores), "store_id", "left")
              .join(F.broadcast(products), "sku", "left"))

    flags = F.array(
        F.when(F.col("currency") != F.col("_dim_currency"), F.lit("DQ08")),
        F.when(F.col("_dim_warehouse").isNull() | F.col("_dim_sku").isNull(),
               F.lit("DQ09")))
    return (tagged
            .withColumn("dq_flags", F.filter(flags, lambda x: x.isNotNull()))
            # loyalty events do not carry the CEDI: enrich it from dim_stores
            .withColumn("warehouse_id", F.coalesce("warehouse_id", "_dim_warehouse"))
            .drop("_line_rule", "_rn", "_dim_currency", "_dim_warehouse", "_dim_sku"))


def build_silver(tagged: DataFrame) -> DataFrame:
    delay = (F.col("emitted_ts_utc").cast("double")
             - F.col("event_ts_utc").cast("double"))
    return (tagged.filter(F.col("dq_rule").isNull())
            .drop("dq_rule", "_corrupt_record")
            .withColumn("event_date", F.to_date("event_ts_utc"))
            .withColumn("event_hour", F.hour("event_ts_utc"))
            .withColumn("arrival_delay_s", F.round(delay, 3))
            .withColumn("is_late", delay > LATE_THRESHOLD_S)
            .select(*SILVER_COLS))


def build_quarantine(tagged: DataFrame, rules: dict[str, dict]) -> DataFrame:
    names = F.create_map(*[x for r in rules.values()
                           for x in (F.lit(r["id"]), F.lit(r["name"]))])
    return (tagged.filter(F.col("dq_rule").isNotNull())
            .withColumn("dq_rule_name", names[F.col("dq_rule")]))


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #

def write_table(df: DataFrame, path: str, fmt: str, partition_by: list[str]) -> None:
    writer = (df.write.mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy(*partition_by))
    if _TABLES:  # managed table (Delta in Unity Catalog on Databricks)
        writer.format(TABLE_FORMAT or fmt).saveAsTable(table_name(path))
    else:
        writer.format(fmt).save(path)


def read_table(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if _TABLES:
        return spark.table(table_name(path))
    return spark.read.format(fmt).load(path)


def write_json(spark: SparkSession, path: str, obj: dict) -> None:
    text = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    if "://" in path:  # gs://, s3a://, abfss:// ...
        (spark.createDataFrame([(text,)], ["value"]).coalesce(1)
         .write.mode("overwrite").text(path))
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text, encoding="utf-8")


# --------------------------------------------------------------------------- #
# Data-quality report (evidence: detected vs injected defects)
# --------------------------------------------------------------------------- #

def dq_report(spark: SparkSession, input_dir: str, quarantine: DataFrame,
              silver: DataFrame, rules: dict[str, dict]) -> dict:
    by_rule = {r["dq_rule"]: r.asDict() for r in quarantine.groupBy("dq_rule").agg(
        F.count("*").alias("lines"),
        F.countDistinct("source_channel", "transaction_id").alias("transactions"),
        F.sum(F.col("transaction_id").isNull().cast("int")).alias("unidentified"),
    ).collect()}

    truth = load_truth(spark, input_dir)

    rows = []
    for rule_id, rule in rules.items():
        if rule["action"] == "flag":
            continue
        got = by_rule.get(rule_id, {"lines": 0, "transactions": 0, "unidentified": 0})
        row = {"rule": rule_id, "name": rule["name"], "detects": rule["detects"],
               "lines": got["lines"],
               # an unparseable JSON line has no id: count each one as a tx
               "transactions": got["transactions"] + got["unidentified"]}
        if truth is not None:
            expected = (truth.filter(F.col("defect") == rule["detects"])
                        .select("source_channel", "transaction_id").distinct())
            detected = (quarantine.filter(F.col("dq_rule") == rule_id)
                        .select("source_channel", "transaction_id").distinct())
            n_exp = expected.count()
            tp = expected.join(detected, ["source_channel", "transaction_id"]).count()
            if rule_id == "DQ01":  # ids of corrupt JSON lines are unreadable
                tp += got["unidentified"]
            row.update({"injected": n_exp, "true_positives": tp,
                        "recall": round(tp / n_exp, 4) if n_exp else None,
                        "precision": round(tp / row["transactions"], 4)
                        if row["transactions"] else None})
        rows.append(row)

    flags = {r["flag"]: r["n"] for r in silver.select(F.explode("dq_flags").alias("flag"))
             .groupBy("flag").agg(F.count("*").alias("n")).collect()}
    late = silver.agg(F.sum(F.col("is_late").cast("int")).alias("n")).first()["n"] or 0
    report = {"rules": rows,
              "non_blocking_flags": {"DQ08": flags.get("DQ08", 0),
                                     "DQ09": flags.get("DQ09", 0)},
              "late_lines_in_silver": late}
    if truth is not None:
        report["late_transactions_injected"] = truth.filter("is_late").count()
    return report


def _print_report(report: dict, timings: dict, counts: dict) -> None:
    print("\nData-quality report")
    print(f"{'rule':5} {'name':25} {'lines':>7} {'tx':>6} {'injected':>9} "
          f"{'recall':>7} {'precision':>9}")
    for r in report["rules"]:
        print(f"{r['rule']:5} {r['name']:25} {r['lines']:>7} {r['transactions']:>6} "
              f"{r.get('injected', '-'):>9} {r.get('recall', '-')!s:>7} "
              f"{r.get('precision', '-')!s:>9}")
    print(f"non-blocking flags: {report['non_blocking_flags']}  "
          f"late lines in silver: {report['late_lines_in_silver']}")
    print("\nRows:", counts)
    print("Timings (s):", timings)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run(input_dir: str, dims_dir: str, lake: str, fmt: str = "parquet",
        master: str | None = None, shuffle_partitions: int = 16,
        tables: str | None = None) -> dict:
    configure_tables(tables)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    spark = build_spark(fmt, master, shuffle_partitions)
    timings: dict[str, float] = {}
    t_total = time.perf_counter()

    def timed(name, fn):
        t0 = time.perf_counter()
        result = fn()
        timings[name] = round(time.perf_counter() - t0, 2)
        return result

    contract = load_contract(spark, input_dir)
    rules = {r["id"]: r for r in contract["quality_rules"]}
    dims = {name: spark.read.option("header", True).option("inferSchema", True)
            .csv(f"{dims_dir}/{name}.csv")
            for name in ("dim_countries", "dim_stores", "dim_products")}

    # 1) read + normalise, materialised once (also avoids Spark's restriction
    #    on queries that only reference the corrupt-record column)
    bronze = materialize(read_raw(spark, input_dir, contract, run_id))
    n_bronze = timed("read_normalize", bronze.count)
    timed("write_bronze", lambda: write_table(
        bronze, f"{lake}/bronze/sales_lines", fmt, ["source_channel"]))

    # 2) quality rules
    tagged = materialize(apply_quality_rules(bronze, dims))
    timed("validate", tagged.count)
    silver = build_silver(tagged)
    quarantine = build_quarantine(tagged, rules)

    # 3) write silver + quarantine
    timed("write_silver", lambda: write_table(
        silver, f"{lake}/silver/sales_lines", fmt, ["country_code", "event_date"]))
    timed("write_quarantine", lambda: write_table(
        quarantine, f"{lake}/quarantine/sales_lines", fmt, ["source_channel", "dq_rule"]))

    # 4) evidence
    report = timed("dq_report", lambda: dq_report(spark, input_dir, quarantine,
                                                  silver, rules))
    counts = {"bronze_lines": n_bronze,
              "silver_lines": silver.count(),
              "quarantine_lines": quarantine.count()}
    timings["total"] = round(time.perf_counter() - t_total, 2)

    summary = {"run_id": run_id, "input": input_dir, "lake": lake, "format": fmt,
               "spark_version": spark.version,
               "default_parallelism": default_parallelism(spark),
               "tables": _TABLES,
               "counts": counts, "timings_s": timings, "dq": report}
    write_json(spark, f"{lake}/_reports/ingest_{run_id}.json", summary)
    _print_report(report, timings, counts)
    print(f"\nReport -> {lake}/_reports/ingest_{run_id}.json")
    release(tagged, bronze)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="DATAMARCA ingestion + data quality")
    parser.add_argument("--input", default="data/batch",
                        help="folder written by data_generator.py (local or gs://)")
    parser.add_argument("--dims", default=None,
                        help="dimension tables folder (default: <input>/../dims)")
    parser.add_argument("--lake", default="lake", help="output root of the lake")
    parser.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    parser.add_argument("--master", default=None,
                        help="e.g. local[4]; leave empty under spark-submit")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--tables", default=None,
                        help="<catalog>.<schema> to write tables instead of paths "
                             f"(default on Databricks: {DEFAULT_DATABRICKS_TABLES})")
    args = parser.parse_args()

    input_dir = args.input.rstrip("/")
    dims_dir = args.dims or input_dir.rsplit("/", 1)[0] + "/dims"
    run(input_dir, dims_dir, args.lake.rstrip("/"), args.format, args.master,
        args.shuffle_partitions, args.tables)


if __name__ == "__main__":
    main()
