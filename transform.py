"""
Transformation stage of the DATAMARCA Spark pipeline (Unidad 3).

Reads the validated lines written by ``ingest.py`` (silver), joins them with
the dimension tables, normalises every amount to USD, converts event times to
each country's local time and builds the gold (analytics-ready) tables:

    <lake>/silver/fact_sales_lines        silver + dimensions + USD + local time
    <lake>/gold/daily_sales               country / local day / channel / category
    <lake>/gold/hourly_sales              country / local day / local hour / channel
    <lake>/gold/store_daily_kpis          store / local day: tickets, avg ticket ...
    <lake>/gold/warehouse_daily_demand    CEDI / local day / SKU: units and kg sold
    <lake>/gold/dq_daily_rejections       quarantined lines per rule / channel / day
    <lake>/_reports/transform_<run_id>.json   reconciliation + stage timings

Dimension tables are tiny (3 / 79 / 39 rows), so every join is a broadcast
join: the large fact table is never shuffled to join.

Usage:
    python transform.py --lake lake
    python transform.py --lake lake --format delta
    spark-submit --py-files ingest.py transform.py --lake gs://<bucket>/lake --dims gs://<bucket>/raw/dims
"""

from __future__ import annotations

import argparse
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingest import build_spark, write_json, write_table

USD_DECIMALS = 4


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #

def read_table(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    return spark.read.format(fmt).load(path)


def read_dims(spark: SparkSession, dims_dir: str) -> dict[str, DataFrame]:
    return {name: spark.read.option("header", True).option("inferSchema", True)
            .csv(f"{dims_dir}/{name}.csv")
            for name in ("dim_countries", "dim_stores", "dim_products")}


# --------------------------------------------------------------------------- #
# Enrichment: silver lines -> fact_sales_lines
# --------------------------------------------------------------------------- #

def enrich(silver: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Join the facts with the three dimensions and derive USD amounts,
    net sales (without tax) and the local date/hour of every sale."""
    countries = dims["dim_countries"].select(
        "country_code", "country_name", "fx_per_usd", "utc_offset")
    stores = dims["dim_stores"].select("store_id", "city_code")
    products = dims["dim_products"].select("sku", "is_perishable", "base_price_usd")

    fact = (silver
            .join(F.broadcast(countries), "country_code", "left")
            .join(F.broadcast(stores), "store_id", "left")
            .join(F.broadcast(products), "sku", "left"))

    usd = lambda c: F.round(F.col(c) / F.col("fx_per_usd"), USD_DECIMALS)  # noqa: E731
    # "-05:00" / "-06:00": fixed offsets (none of the 3 countries uses DST)
    tz = F.format_string("%+03d:00", F.col("utc_offset"))
    local_ts = F.from_utc_timestamp("event_ts_utc", tz)

    return (fact
            .withColumn("net_amount", F.round(F.col("line_total") - F.col("tax_amount"), 2))
            .withColumn("gross_amount", F.round(F.col("line_total") + F.col("discount_amount"), 2))
            .withColumn("line_total_usd", usd("line_total"))
            .withColumn("net_amount_usd", usd("net_amount"))
            .withColumn("tax_amount_usd", usd("tax_amount"))
            .withColumn("discount_amount_usd", usd("discount_amount"))
            .withColumn("gross_amount_usd", usd("gross_amount"))
            .withColumn("units", F.when(F.col("unit_of_measure") == "UN", F.col("quantity"))
                                  .otherwise(F.lit(0.0)))
            .withColumn("kg", F.when(F.col("unit_of_measure") == "KG", F.col("quantity"))
                               .otherwise(F.lit(0.0)))
            .withColumn("event_ts_local", local_ts)
            .withColumn("event_date_local", F.to_date(local_ts))
            .withColumn("event_hour_local", F.hour(local_ts))
            .withColumn("is_identified", F.col("customer_id").isNotNull())
            .drop("utc_offset"))


# --------------------------------------------------------------------------- #
# Gold tables
# --------------------------------------------------------------------------- #

def _sales_metrics():
    """Common aggregations over fact lines."""
    return [
        F.countDistinct("source_channel", "transaction_id").alias("transactions"),
        F.count("*").alias("lines"),
        F.round(F.sum("units"), 0).cast("long").alias("units_sold"),
        F.round(F.sum("kg"), 3).alias("kg_sold"),
        F.round(F.sum("line_total"), 2).alias("sales_local"),
        F.round(F.sum("line_total_usd"), 2).alias("sales_usd"),
        F.round(F.sum("net_amount_usd"), 2).alias("net_sales_usd"),
        F.round(F.sum("tax_amount_usd"), 2).alias("tax_usd"),
        F.round(F.sum("discount_amount_usd"), 2).alias("discount_usd"),
    ]


def daily_sales(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("country_code", "country_name", "currency",
                         "event_date_local", "source_channel", "category")
            .agg(*_sales_metrics(),
                 F.round(F.sum(F.col("late_line").cast("int")) / F.count("*"), 4)
                  .alias("late_share"))
            .withColumn("avg_line_usd", F.round(F.col("sales_usd") / F.col("lines"), 2)))


def hourly_sales(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("country_code", "event_date_local", "event_hour_local",
                         "source_channel")
            .agg(*_sales_metrics()))


def transactions(fact: DataFrame) -> DataFrame:
    """One row per ticket (basket), needed for ticket-level KPIs."""
    return (fact.groupBy("source_channel", "transaction_id", "country_code",
                         "store_id", "city", "warehouse_id", "event_date_local",
                         "customer_id", "loyalty_id", "payment_method")
            .agg(F.count("*").alias("lines"),
                 F.sum("line_total_usd").alias("ticket_usd"),
                 F.sum("discount_amount_usd").alias("discount_usd")))


def store_daily_kpis(tickets: DataFrame) -> DataFrame:
    return (tickets.groupBy("country_code", "store_id", "city", "warehouse_id",
                            "event_date_local")
            .agg(F.count("*").alias("tickets"),
                 F.round(F.sum("ticket_usd"), 2).alias("sales_usd"),
                 F.round(F.avg("ticket_usd"), 2).alias("avg_ticket_usd"),
                 F.round(F.avg("lines"), 2).alias("avg_lines_per_ticket"),
                 F.round(F.avg(F.col("customer_id").isNotNull().cast("int")), 4)
                  .alias("identified_share"),
                 F.round(F.avg(F.col("loyalty_id").isNotNull().cast("int")), 4)
                  .alias("loyalty_share"),
                 *[F.sum((F.col("source_channel") == ch).cast("int")).alias(f"tickets_{alias}")
                   for ch, alias in (("POS", "pos"), ("ECOMMERCE", "ecommerce"),
                                     ("LOYALTY_APP", "app"))]))


def warehouse_daily_demand(fact: DataFrame) -> DataFrame:
    """Demand per CEDI and SKU: what each distribution center must replenish."""
    return (fact.groupBy("country_code", "warehouse_id", "event_date_local",
                         "sku", "product_name", "category", "unit_of_measure",
                         "is_perishable")
            .agg(F.round(F.sum("quantity"), 3).alias("quantity_sold"),
                 F.countDistinct("store_id").alias("stores"),
                 F.countDistinct("source_channel", "transaction_id").alias("transactions"),
                 F.round(F.sum("line_total_usd"), 2).alias("sales_usd")))


def dq_daily_rejections(quarantine: DataFrame) -> DataFrame:
    """Governance view: rejected lines per rule, channel and UTC day."""
    return (quarantine
            .withColumn("event_date", F.to_date("event_ts_utc"))
            .groupBy("source_channel", "dq_rule", "dq_rule_name", "event_date")
            .agg(F.count("*").alias("lines"),
                 F.countDistinct("transaction_id").alias("transactions")))


# --------------------------------------------------------------------------- #
# Reconciliation (evidence that no money is lost or duplicated in gold)
# --------------------------------------------------------------------------- #

def reconcile(silver: DataFrame, fact: DataFrame, gold: dict[str, DataFrame]) -> dict:
    s = silver.agg(F.count("*").alias("lines"),
                   F.countDistinct("source_channel", "transaction_id").alias("tx")).first()
    f = fact.agg(F.count("*").alias("lines"),
                 F.round(F.sum("line_total_usd"), 2).alias("usd"),
                 F.sum(F.col("fx_per_usd").isNull().cast("int")).alias("no_fx"),
                 F.sum(F.col("is_perishable").isNull().cast("int")).alias("no_product"),
                 F.sum(F.col("city_code").isNull().cast("int")).alias("no_store")).first()
    totals = {name: df.agg(F.round(F.sum("sales_usd"), 2)).first()[0]
              for name, df in gold.items() if "sales_usd" in df.columns}
    tx_daily = gold["daily_sales"].groupBy().sum("transactions").first()[0]
    tickets = gold["store_daily_kpis"].groupBy().sum("tickets").first()[0]
    checks = {
        "silver_lines == fact_lines (joins did not drop/duplicate rows)": s["lines"] == f["lines"],
        "every line matched country, store and product dims":
            f["no_fx"] == 0 and f["no_product"] == 0 and f["no_store"] == 0,
        "gold sales_usd totals match fact (+/- 1 USD rounding)":
            all(abs(v - f["usd"]) <= 1 for v in totals.values()),
        "tickets in store_daily_kpis == silver transactions": tickets == s["tx"],
    }
    return {"silver_lines": s["lines"], "silver_transactions": s["tx"],
            "fact_lines": f["lines"], "fact_sales_usd": f["usd"],
            "gold_sales_usd": totals,
            "daily_sales_transactions_sum": tx_daily,
            "checks": checks, "all_ok": all(checks.values())}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run(lake: str, dims_dir: str, fmt: str = "parquet", master: str | None = None,
        shuffle_partitions: int = 16) -> dict:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    spark = build_spark(fmt, master, shuffle_partitions)
    spark.sparkContext.setJobGroup("transform", f"datamarca transform {run_id}")
    timings: dict[str, float] = {}
    t_total = time.perf_counter()

    def timed(name, fn):
        t0 = time.perf_counter()
        result = fn()
        timings[name] = round(time.perf_counter() - t0, 2)
        return result

    silver = read_table(spark, f"{lake}/silver/sales_lines", fmt)
    quarantine = read_table(spark, f"{lake}/quarantine/sales_lines", fmt)
    dims = read_dims(spark, dims_dir)

    # 1) enrichment, materialised once and reused by every gold table
    fact = enrich(silver, dims).withColumnRenamed("is_late", "late_line").cache()
    n_fact = timed("enrich", fact.count)
    timed("write_fact", lambda: write_table(
        fact, f"{lake}/silver/fact_sales_lines", fmt,
        ["country_code", "event_date_local"]))

    # 2) gold tables
    tickets = transactions(fact).cache()
    gold = {
        "daily_sales": daily_sales(fact),
        "hourly_sales": hourly_sales(fact),
        "store_daily_kpis": store_daily_kpis(tickets),
        "warehouse_daily_demand": warehouse_daily_demand(fact),
        "dq_daily_rejections": dq_daily_rejections(quarantine),
    }
    partitions = {"dq_daily_rejections": ["source_channel"]}
    rows = {}
    for name, df in gold.items():
        df = df.cache()
        gold[name] = df
        rows[name] = timed(f"build_{name}", df.count)
        # gold tables are small: one file per partition avoids "small files"
        part = partitions.get(name, ["country_code"])
        timed(f"write_{name}", lambda df=df, name=name, part=part: write_table(
            df.repartition(*part), f"{lake}/gold/{name}", fmt, part))

    # 3) evidence
    recon = timed("reconcile", lambda: reconcile(silver, fact, gold))
    timings["total"] = round(time.perf_counter() - t_total, 2)
    summary = {"run_id": run_id, "lake": lake, "format": fmt,
               "fact_lines": n_fact, "gold_rows": rows,
               "reconciliation": recon, "timings_s": timings}
    write_json(spark, f"{lake}/_reports/transform_{run_id}.json", summary)

    print("\nGold tables (rows):", rows)
    print("\nReconciliation:")
    for check, ok in recon["checks"].items():
        print(f"  [{'OK' if ok else 'FAIL'}] {check}")
    print(f"  fact sales: {recon['fact_sales_usd']:,.2f} USD")
    print("\nTimings (s):", timings)
    print(f"\nReport -> {lake}/_reports/transform_{run_id}.json")
    for df in [fact, tickets, *gold.values()]:
        df.unpersist()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="DATAMARCA transformations (gold layer)")
    parser.add_argument("--lake", default="lake", help="lake root written by ingest.py")
    parser.add_argument("--dims", default="data/dims", help="dimension tables folder")
    parser.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    parser.add_argument("--master", default=None,
                        help="e.g. local[4]; leave empty under spark-submit")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    args = parser.parse_args()
    run(args.lake.rstrip("/"), args.dims.rstrip("/"), args.format, args.master,
        args.shuffle_partitions)


if __name__ == "__main__":
    main()
