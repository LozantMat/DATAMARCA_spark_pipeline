"""
Synthetic data generator for DATAMARCA S.A.

Simulates the transactional feeds of a supermarket chain operating in
Colombia (CO), Mexico (MX) and Peru (PE) through three heterogeneous channels:

    POS          -> tabular CSV, one row per line item (physical stores)
    ECOMMERCE    -> JSON Lines event log, one nested event per order
    LOYALTY_APP  -> JSON Lines event log, scan-and-go purchases with points

Every record carries:
    * its source (``source_channel``, ``source_system``, ``country_code``)
    * the simulated business time (``event_ts_utc`` / ``event_ts_local``)
    * the simulated generation/emission time (``emitted_ts_utc``)
    * Spark partitioning helpers (``event_date``, ``event_hour``,
      ``country_code``, ``source_channel``) and a ``batch_id``

Realistic imperfections are injected so the pipeline has something to validate:
late arrivals (offline POS / mobile sync), duplicates (client retries),
missing required fields, negative quantities, amount mismatches, invalid
country codes, future timestamps and malformed lines (file output only).

Both modes are driven by the same deterministic engine: for a given seed,
batch and streaming produce exactly the same transactions; only the size of
the delivery window changes (60 s for batch, ``interval_seconds`` for stream).

Usage (CLI):
    python data_generator.py batch  --minutes 120 --seed 42
    python data_generator.py stream --minutes 120 --interval 10 --speed 60

Usage (Python):
    from data_generator import DataGenerator
    gen = DataGenerator(seed=42)
    for window in gen.iter_batches():
        window.records["POS"]  # list[dict]
"""

from __future__ import annotations

import argparse
import copy
import csv
import heapq
import json
import os
import random
import shutil
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DATA_DIR = Path(__file__).resolve().parent / "data"
SCHEMA_VERSION = "1.0"

DEFAULT_SEED = 42
DEFAULT_START = "2026-10-26T13:00:00Z"  # 08:00 Bogotá/Lima, 07:00 CDMX
DEFAULT_MINUTES = 120
DEFAULT_TX_PER_MINUTE = 2000
DEFAULT_DIRTY_RATE = 0.02

CHANNELS = ("POS", "ECOMMERCE", "LOYALTY_APP")
CHANNEL_WEIGHTS = (0.65, 0.25, 0.10)
CHANNEL_DIRS = {"POS": "pos", "ECOMMERCE": "ecommerce", "LOYALTY_APP": "loyalty"}
CHANNEL_EXT = {"POS": "csv", "ECOMMERCE": "jsonl", "LOYALTY_APP": "jsonl"}

COUNTRIES = {
    "CO": {
        "weight": 0.35, "currency": "COP", "utc_offset": -5, "tax_rate": 0.19,
        "fx_per_usd": 4000.0, "price_step": 50.0, "points_unit": 1000.0,
        "pos_system": "POS-NCR-CO",
        "cities": {"BOG": ("Bogotá", 12), "MED": ("Medellín", 7),
                   "CAL": ("Cali", 5), "BAQ": ("Barranquilla", 4)},
        "payments": {"CASH": 30, "DEBIT_CARD": 30, "CREDIT_CARD": 20,
                     "NEQUI": 12, "DAVIPLATA": 8},
    },
    "MX": {
        "weight": 0.45, "currency": "MXN", "utc_offset": -6, "tax_rate": 0.16,
        "fx_per_usd": 18.0, "price_step": 0.5, "points_unit": 10.0,
        "pos_system": "POS-SAP-MX",
        "cities": {"CMX": ("Ciudad de México", 14), "GDL": ("Guadalajara", 7),
                   "MTY": ("Monterrey", 7), "PUE": ("Puebla", 4)},
        "payments": {"CASH": 35, "DEBIT_CARD": 28, "CREDIT_CARD": 22,
                     "SPEI": 8, "CODI": 7},
    },
    "PE": {
        "weight": 0.20, "currency": "PEN", "utc_offset": -5, "tax_rate": 0.18,
        "fx_per_usd": 3.7, "price_step": 0.1, "points_unit": 3.0,
        "pos_system": "POS-ORACLE-PE",
        "cities": {"LIM": ("Lima", 12), "AQP": ("Arequipa", 4),
                   "TRU": ("Trujillo", 3)},
        "payments": {"CASH": 32, "DEBIT_CARD": 25, "CREDIT_CARD": 18,
                     "YAPE": 17, "PLIN": 8},
    },
}

# (sku, name, category, unit_of_measure, base_price_usd)
CATALOG = [
    ("SKU-ABA-0001", "Arroz blanco 1kg", "ABARROTES", "UN", 1.50),
    ("SKU-ABA-0002", "Aceite de girasol 1L", "ABARROTES", "UN", 3.20),
    ("SKU-ABA-0003", "Azúcar refinada 1kg", "ABARROTES", "UN", 1.20),
    ("SKU-ABA-0004", "Pasta spaghetti 500g", "ABARROTES", "UN", 0.90),
    ("SKU-ABA-0005", "Frijol rojo 500g", "ABARROTES", "UN", 1.40),
    ("SKU-ABA-0006", "Atún en lata 170g", "ABARROTES", "UN", 1.60),
    ("SKU-ABA-0007", "Harina de maíz 1kg", "ABARROTES", "UN", 1.30),
    ("SKU-ABA-0008", "Café molido 500g", "ABARROTES", "UN", 5.50),
    ("SKU-LAC-0001", "Leche entera 1L", "LACTEOS", "UN", 1.10),
    ("SKU-LAC-0002", "Yogurt natural 1L", "LACTEOS", "UN", 2.40),
    ("SKU-LAC-0003", "Queso fresco 500g", "LACTEOS", "UN", 4.00),
    ("SKU-LAC-0004", "Huevos AA x30", "LACTEOS", "UN", 4.80),
    ("SKU-LAC-0005", "Mantequilla 250g", "LACTEOS", "UN", 2.60),
    ("SKU-FRU-0001", "Banano", "FRUVER", "KG", 0.90),
    ("SKU-FRU-0002", "Tomate chonto", "FRUVER", "KG", 1.30),
    ("SKU-FRU-0003", "Papa blanca", "FRUVER", "KG", 0.80),
    ("SKU-FRU-0004", "Aguacate", "FRUVER", "KG", 3.00),
    ("SKU-FRU-0005", "Manzana roja", "FRUVER", "KG", 2.50),
    ("SKU-FRU-0006", "Cebolla cabezona", "FRUVER", "KG", 1.10),
    ("SKU-FRU-0007", "Limón", "FRUVER", "KG", 1.60),
    ("SKU-CAR-0001", "Pechuga de pollo", "CARNES", "KG", 5.50),
    ("SKU-CAR-0002", "Carne molida de res", "CARNES", "KG", 7.00),
    ("SKU-CAR-0003", "Lomo de cerdo", "CARNES", "KG", 6.80),
    ("SKU-CAR-0004", "Filete de tilapia", "CARNES", "KG", 7.50),
    ("SKU-BEB-0001", "Gaseosa cola 1.5L", "BEBIDAS", "UN", 1.60),
    ("SKU-BEB-0002", "Agua sin gas 600ml", "BEBIDAS", "UN", 0.60),
    ("SKU-BEB-0003", "Jugo de naranja 1L", "BEBIDAS", "UN", 2.00),
    ("SKU-BEB-0004", "Cerveza lager six-pack", "BEBIDAS", "UN", 6.50),
    ("SKU-ASE-0001", "Detergente en polvo 1kg", "ASEO", "UN", 4.20),
    ("SKU-ASE-0002", "Jabón de baño x3", "ASEO", "UN", 2.80),
    ("SKU-ASE-0003", "Papel higiénico x12", "ASEO", "UN", 6.00),
    ("SKU-ASE-0004", "Lavaloza líquido 500ml", "ASEO", "UN", 1.90),
    ("SKU-PAN-0001", "Pan tajado 500g", "PANADERIA", "UN", 1.80),
    ("SKU-PAN-0002", "Arepas x5", "PANADERIA", "UN", 1.50),
    ("SKU-PAN-0003", "Galletas de soda", "PANADERIA", "UN", 1.20),
    ("SKU-CPE-0001", "Shampoo 400ml", "CUIDADO_PERSONAL", "UN", 4.50),
    ("SKU-CPE-0002", "Crema dental 100ml", "CUIDADO_PERSONAL", "UN", 2.00),
    ("SKU-CPE-0003", "Desodorante en barra", "CUIDADO_PERSONAL", "UN", 3.50),
    ("SKU-MAS-0001", "Concentrado para perro 2kg", "MASCOTAS", "UN", 8.00),
]

# Basket size (number of distinct line items) per channel: (values, weights)
BASKET_SIZES = {
    "POS": ((1, 2, 3, 4, 5, 6, 8), (22, 22, 18, 14, 10, 8, 6)),
    "ECOMMERCE": ((2, 4, 6, 8, 10, 12), (15, 25, 25, 18, 10, 7)),
    "LOYALTY_APP": ((1, 2, 3, 4, 5), (30, 28, 20, 14, 8)),
}

# Delivery latency per channel:
# (mean on-time latency s, late probability, late delay min s, late delay max s)
# POS stores sync offline buffers late; mobile apps sync when they regain signal.
LATENCY = {
    "POS": (3.0, 0.03, 120, 900),
    "ECOMMERCE": (0.8, 0.005, 60, 300),
    "LOYALTY_APP": (2.0, 0.02, 60, 900),
}

DEFECT_TYPES = {
    "missing_required": 25,
    "negative_quantity": 15,
    "amount_mismatch": 15,
    "invalid_country": 10,
    "future_event_ts": 8,
    "duplicate": 20,
    "malformed": 7,
}

CUSTOMER_POOL = 50_000      # customers per country
LOYALTY_SHARE = 0.60        # share of the pool enrolled in the loyalty program
POS_IDENTIFIED_SHARE = 0.55  # POS purchases where the customer is identified

POS_FIELDS = [
    "transaction_id", "line_number", "source_channel", "source_system",
    "country_code", "city", "store_id", "warehouse_id", "terminal_id",
    "cashier_id", "customer_id", "loyalty_id", "sku", "product_name",
    "category", "unit_of_measure", "quantity", "unit_price", "discount_amount",
    "tax_amount", "line_total", "currency", "payment_method",
    "event_ts_utc", "event_ts_local", "emitted_ts_utc", "event_date",
    "event_hour", "batch_id", "schema_version",
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _fmt_local(dt: datetime, utc_offset: int) -> str:
    local = dt.astimezone(timezone(timedelta(hours=utc_offset)))
    return local.isoformat(timespec="milliseconds")


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _round_to(value: float, step: float) -> float:
    return round(round(value / step) * step, 2)


def _build_stores() -> dict[str, list[dict]]:
    stores: dict[str, list[dict]] = {}
    for cc, cfg in COUNTRIES.items():
        stores[cc] = [
            {"store_id": f"{cc}-{code}-{i:03d}", "city": name,
             "warehouse_id": f"CEDI-{cc}-{code}"}
            for code, (name, n) in cfg["cities"].items()
            for i in range(1, n + 1)
        ]
    return stores


STORES = _build_stores()

COUNTRY_NAMES = {"CO": "Colombia", "MX": "México", "PE": "Perú"}
PERISHABLE_CATEGORIES = {"FRUVER", "CARNES", "LACTEOS", "PANADERIA"}


# --------------------------------------------------------------------------- #
# Dimension tables (reference data for the pipeline joins)
# --------------------------------------------------------------------------- #

DIM_FIELDS = {
    "dim_countries": ["country_code", "country_name", "currency", "fx_per_usd",
                      "tax_rate", "utc_offset", "pos_system"],
    "dim_stores": ["store_id", "country_code", "city_code", "city",
                   "warehouse_id"],
    "dim_products": ["sku", "product_name", "category", "unit_of_measure",
                     "base_price_usd", "is_perishable"],
}


def build_dimensions() -> dict[str, list[dict]]:
    """Static master data the generator already uses internally, exposed as
    tables so the pipeline can join facts with stores, products and FX rates
    (e.g. to normalise COP/MXN/PEN amounts to USD)."""
    countries = [{
        "country_code": cc, "country_name": COUNTRY_NAMES[cc],
        "currency": cfg["currency"], "fx_per_usd": cfg["fx_per_usd"],
        "tax_rate": cfg["tax_rate"], "utc_offset": cfg["utc_offset"],
        "pos_system": cfg["pos_system"],
    } for cc, cfg in COUNTRIES.items()]

    stores = [{
        "store_id": s["store_id"], "country_code": cc,
        "city_code": s["store_id"].split("-")[1], "city": s["city"],
        "warehouse_id": s["warehouse_id"],
    } for cc, rows in STORES.items() for s in rows]

    products = [{
        "sku": sku, "product_name": name, "category": cat,
        "unit_of_measure": uom, "base_price_usd": usd,
        "is_perishable": cat in PERISHABLE_CATEGORIES,
    } for sku, name, cat, uom, usd in CATALOG]

    return {"dim_countries": countries, "dim_stores": stores,
            "dim_products": products}


def write_dimensions(out_dir: Path | str = DATA_DIR / "dims") -> Path:
    """Write one CSV per dimension table into ``out_dir``.

    The tables are static (they do not depend on the seed), so they are
    rewritten on every run and always match the facts.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in build_dimensions().items():
        target = out_dir / f"{name}.csv"
        tmp = target.with_name(f".{target.name}.tmp")
        with tmp.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=DIM_FIELDS[name])
            writer.writeheader()
            for row in rows:  # lowercase booleans so Spark infers BooleanType
                writer.writerow({k: str(v).lower() if isinstance(v, bool) else v
                                 for k, v in row.items()})
        os.replace(tmp, target)
    return out_dir


# --------------------------------------------------------------------------- #
# Data contract (expected schemas + quality rules the pipeline must enforce)
# --------------------------------------------------------------------------- #

_TIME_SCHEMA = {
    "event_ts_utc": "TIMESTAMP", "event_ts_local": "STRING",
    "emitted_ts_utc": "TIMESTAMP", "event_date": "DATE", "event_hour": "INT",
    "batch_id": "STRING", "schema_version": "STRING",
}

_ITEM_SCHEMA = {
    "sku": "STRING", "product_name": "STRING", "category": "STRING",
    "unit_of_measure": "STRING", "quantity": "DOUBLE", "unit_price": "DOUBLE",
    "discount_amount": "DOUBLE", "tax_amount": "DOUBLE", "line_total": "DOUBLE",
}

# Nested dict = STRUCT, one-element list = ARRAY of that element.
SCHEMAS: dict[str, dict] = {
    "POS": {
        "transaction_id": "STRING", "line_number": "INT",
        "source_channel": "STRING", "source_system": "STRING",
        "country_code": "STRING", "city": "STRING", "store_id": "STRING",
        "warehouse_id": "STRING", "terminal_id": "STRING",
        "cashier_id": "STRING", "customer_id": "STRING", "loyalty_id": "STRING",
        **_ITEM_SCHEMA,
        "currency": "STRING", "payment_method": "STRING",
        **_TIME_SCHEMA,
    },
    "ECOMMERCE": {
        "event_id": "STRING", "event_type": "STRING", "order_id": "STRING",
        "source_channel": "STRING", "source_system": "STRING",
        "country_code": "STRING", "session_id": "STRING",
        "customer": {"customer_id": "STRING", "loyalty_id": "STRING"},
        "device": {"platform": "STRING", "app_version": "STRING"},
        "fulfillment": {"delivery_type": "STRING", "store_id": "STRING",
                        "warehouse_id": "STRING", "city": "STRING"},
        "items": [_ITEM_SCHEMA],
        "payment": {"method": "STRING", "currency": "STRING",
                    "discount_total": "DOUBLE", "tax_total": "DOUBLE",
                    "total": "DOUBLE"},
        **_TIME_SCHEMA,
    },
    "LOYALTY_APP": {
        "event_id": "STRING", "event_type": "STRING",
        "transaction_id": "STRING", "source_channel": "STRING",
        "source_system": "STRING", "country_code": "STRING",
        "member": {"loyalty_id": "STRING", "customer_id": "STRING",
                   "tier": "STRING"},
        "store_id": "STRING", "city": "STRING", "app_version": "STRING",
        "items": [_ITEM_SCHEMA],
        "amount": {"currency": "STRING", "total": "DOUBLE",
                   "tax_total": "DOUBLE", "discount_total": "DOUBLE"},
        "points": {"earned": "INT", "redeemed": "INT"},
        "payment_method": "STRING",
        **_TIME_SCHEMA,
    },
}
# POS columns must follow the CSV header order
SCHEMAS["POS"] = {f: SCHEMAS["POS"][f] for f in POS_FIELDS}

# Business key per channel (used for deduplication)
DEDUP_KEYS = {
    "POS": ["transaction_id", "line_number"],
    "ECOMMERCE": ["order_id"],
    "LOYALTY_APP": ["transaction_id"],
}

# Each rule names the injected defect it is meant to catch ("detects") and
# what the pipeline should do with the offending record ("action").
# Rules are evaluated in order and a record is tagged with the FIRST rule it
# fails (e.g. a negative quantity also breaks DQ05, but it is reported as DQ04).
QUALITY_RULES = [
    {"id": "DQ01", "name": "parseable_record", "channels": list(CHANNELS),
     "check": "record parses with the expected schema "
              "(CSV: all columns present; JSON: valid object)",
     "detects": "malformed", "action": "quarantine"},
    {"id": "DQ02", "name": "required_fields", "channels": list(CHANNELS),
     "check": "not null / not empty",
     "fields": {
         "POS": ["transaction_id", "store_id", "warehouse_id", "sku",
                 "quantity", "unit_price", "line_total", "event_ts_utc"],
         "ECOMMERCE": ["order_id", "fulfillment.store_id",
                       "fulfillment.warehouse_id", "items", "event_ts_utc"],
         "LOYALTY_APP": ["transaction_id", "store_id", "member.loyalty_id",
                         "items", "event_ts_utc"],
     },
     "note": "country_code is validated by DQ03 (an empty code is invalid)",
     "detects": "missing_required", "action": "quarantine"},
    {"id": "DQ03", "name": "valid_country", "channels": list(CHANNELS),
     "check": "country_code IN ('CO', 'MX', 'PE')",
     "detects": "invalid_country", "action": "quarantine"},
    {"id": "DQ04", "name": "positive_quantity", "channels": list(CHANNELS),
     "check": "quantity > 0 (every line / every item)",
     "detects": "negative_quantity", "action": "quarantine"},
    {"id": "DQ05", "name": "line_amount_consistency", "channels": list(CHANNELS),
     "check": "abs(line_total - (quantity * unit_price - discount_amount)) <= 0.02",
     "tolerance": 0.02,
     "detects": "amount_mismatch", "action": "quarantine"},
    {"id": "DQ06", "name": "event_not_in_future", "channels": list(CHANNELS),
     "check": "event_ts_utc <= emitted_ts_utc + 5 minutes (clock-skew margin)",
     "detects": "future_event_ts", "action": "quarantine"},
    {"id": "DQ07", "name": "unique_business_key", "channels": list(CHANNELS),
     "check": "one record per business key; keep the earliest emitted_ts_utc",
     "keys": DEDUP_KEYS,
     "detects": "duplicate", "action": "drop_duplicate"},
    {"id": "DQ08", "name": "currency_matches_country", "channels": list(CHANNELS),
     "check": "currency = dim_countries.currency for the country_code",
     "detects": None, "action": "flag",
     "note": "no defect is injected for this rule; 0 violations expected"},
    {"id": "DQ09", "name": "referential_integrity", "channels": list(CHANNELS),
     "check": "store_id IN dim_stores AND sku IN dim_products",
     "detects": None, "action": "flag",
     "note": "no defect is injected for this rule; 0 violations expected"},
]

# Late data is not an error: it is handled with watermarks, not quarantined.
LATE_DATA_POLICY = {
    "definition": "emitted_ts_utc - event_ts_utc > 60 seconds",
    "max_expected_delay_minutes": 15,
    "suggested_watermark": "20 minutes on event_ts_utc",
}


def _ddl_type(t) -> str:
    if isinstance(t, dict):
        return "STRUCT<" + ", ".join(f"{k}: {_ddl_type(v)}" for k, v in t.items()) + ">"
    if isinstance(t, list):
        return f"ARRAY<{_ddl_type(t[0])}>"
    return t


def spark_ddl(channel: str) -> str:
    """Spark DDL schema string for a channel, ready for
    ``spark.read.schema(spark_ddl("POS")).csv(...)``."""
    return ", ".join(f"{k} {_ddl_type(v)}" for k, v in SCHEMAS[channel].items())


def build_contract() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "channels": {ch: {"format": CHANNEL_EXT[ch],
                          "folder": CHANNEL_DIRS[ch],
                          "dedup_key": DEDUP_KEYS[ch],
                          "spark_ddl": spark_ddl(ch),
                          "schema": SCHEMAS[ch]} for ch in CHANNELS},
        "quality_rules_evaluation": "in order; a record is tagged with the "
                                    "first rule it fails",
        "quality_rules": QUALITY_RULES,
        "late_data_policy": LATE_DATA_POLICY,
        "partitioning": ["country_code", "event_date"],
    }


def write_contract(target: Path | str) -> Path:
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build_contract(), indent=2, ensure_ascii=False),
                      encoding="utf-8")
    return target


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #

@dataclass
class Window:
    """A delivery window: one batch (60 s) or one streaming micro-batch."""
    index: int
    batch_id: str
    start: datetime
    end: datetime
    records: dict[str, list[dict]] = field(
        default_factory=lambda: {ch: [] for ch in CHANNELS})
    transactions: Counter = field(default_factory=Counter)
    defects: Counter = field(default_factory=Counter)
    late: int = 0
    # Records (by channel/position) that must be corrupted when written to disk
    malformed: dict[str, set[int]] = field(
        default_factory=lambda: {ch: set() for ch in CHANNELS})
    # Ground truth: one entry per late or defective transaction in this window
    truth: list[dict] = field(default_factory=list)

    @property
    def total_transactions(self) -> int:
        return sum(self.transactions.values())

    def manifest(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "window_start_utc": _fmt_utc(self.start),
            "window_end_utc": _fmt_utc(self.end),
            "transactions": self.total_transactions,
            "transactions_by_channel": dict(self.transactions),
            "records_by_channel": {ch: len(r) for ch, r in self.records.items()},
            "late_arrivals": self.late,
            "injected_defects": dict(self.defects),
        }


@dataclass(order=True)
class _Emission:
    emitted: datetime
    order: tuple
    channel: str = field(compare=False)
    payload: list[dict] = field(compare=False)  # POS: n rows; JSON channels: 1
    defect: str | None = field(compare=False)
    late: bool = field(compare=False)
    is_duplicate: bool = field(compare=False, default=False)
    tx_id: str = field(compare=False, default="")


class DataGenerator:
    """Deterministic generator of DATAMARCA transactions.

    The same ``seed`` always yields the same data. Each simulated minute uses
    its own RNG derived from (seed, minute), so results do not depend on how
    the output is windowed or consumed.
    """

    def __init__(self, seed: int = DEFAULT_SEED, start: str = DEFAULT_START,
                 minutes: int = DEFAULT_MINUTES,
                 tx_per_minute: int = DEFAULT_TX_PER_MINUTE,
                 dirty_rate: float = DEFAULT_DIRTY_RATE,
                 late_rate_scale: float = 1.0):
        self.seed = seed
        self.start = _parse_utc(start)
        self.minutes = minutes
        self.tx_per_minute = tx_per_minute
        self.dirty_rate = dirty_rate
        self.late_rate_scale = late_rate_scale

    # ---- public API ------------------------------------------------------ #

    def iter_batches(self) -> Iterator[Window]:
        """Yield one Window per simulated minute (~``tx_per_minute`` tx each)."""
        return self._iter_windows(60, lambda w: f"B-{w:%Y%m%d-%H%M}")

    def iter_stream(self, interval_seconds: int = 10) -> Iterator[Window]:
        """Yield micro-batches of ``interval_seconds`` simulated seconds
        (~``tx_per_minute * interval_seconds / 60`` tx each)."""
        if 60 % interval_seconds:
            raise ValueError("interval_seconds must divide 60")
        return self._iter_windows(interval_seconds,
                                  lambda w: f"S-{w:%Y%m%d-%H%M%S}")

    def iter_events(self, interval_seconds: int = 1) -> Iterator[tuple[str, dict]]:
        """Flat event stream ``(channel, record)`` in arrival order — handy for
        pushing to a socket, Kafka or Event Hubs producer later."""
        for window in self.iter_stream(interval_seconds):
            for ch in CHANNELS:
                for rec in window.records[ch]:
                    yield ch, rec

    def write_batch(self, out_dir: Path | str = DATA_DIR / "batch",
                    clean: bool = False, verbose: bool = True) -> Path:
        """Write one file per channel per simulated minute.

        Layout: ``<out_dir>/<channel>/ingest_date=YYYY-MM-DD/<channel>_YYYYmmdd_HHMM.<ext>``
        plus ``_manifest.jsonl``, ``_truth.jsonl``, ``_contract.json`` and
        ``_run.json`` (files starting with ``_`` are ignored by Spark readers).
        """
        out_dir = Path(out_dir)
        _prepare_dir(out_dir, clean)
        write_contract(out_dir / "_contract.json")
        stats = _RunStats("batch", self, out_dir)
        with (out_dir / "_manifest.jsonl").open("w", encoding="utf-8") as manifest, \
                (out_dir / "_truth.jsonl").open("w", encoding="utf-8") as truth:
            for window in self.iter_batches():
                stamp = f"{window.start:%Y%m%d_%H%M}"
                for ch in CHANNELS:
                    if not window.records[ch]:
                        continue
                    target = (out_dir / CHANNEL_DIRS[ch]
                              / f"ingest_date={window.start:%Y-%m-%d}"
                              / f"{CHANNEL_DIRS[ch]}_{stamp}.{CHANNEL_EXT[ch]}")
                    _write_records(target, ch, window.records[ch],
                                   window.malformed[ch])
                manifest.write(json.dumps(window.manifest()) + "\n")
                truth.writelines(json.dumps(t) + "\n" for t in window.truth)
                stats.add(window)
                if verbose:
                    _log(window)
        stats.finish()
        return out_dir

    def write_stream(self, out_dir: Path | str = DATA_DIR / "stream",
                     interval_seconds: int = 10, speed: float = 60.0,
                     clean: bool = False, verbose: bool = True) -> Path:
        """Drop micro-batch files into ``<out_dir>/<channel>/`` so Spark
        Structured Streaming's file source can pick them up.

        ``speed`` is the simulation speed-up: 1 = real time (2 h for 120 min),
        60 = one simulated minute per real second, 0 = as fast as possible.
        Files are written to a hidden temp name and atomically renamed, so
        Spark never reads a partially written file.
        """
        out_dir = Path(out_dir)
        _prepare_dir(out_dir, clean)
        write_contract(out_dir / "_contract.json")
        stats = _RunStats("stream", self, out_dir,
                          interval_seconds=interval_seconds, speed=speed)
        wall_start = time.monotonic()
        with (out_dir / "_manifest.jsonl").open("w", encoding="utf-8") as manifest, \
                (out_dir / "_truth.jsonl").open("w", encoding="utf-8") as truth:
            for window in self.iter_stream(interval_seconds):
                if speed > 0:
                    due = (window.end - self.start).total_seconds() / speed
                    delay = due - (time.monotonic() - wall_start)
                    if delay > 0:
                        time.sleep(delay)
                for ch in CHANNELS:
                    if not window.records[ch]:
                        continue
                    name = (f"part-{window.index:06d}-{window.start:%Y%m%dT%H%M%S}"
                            f".{CHANNEL_EXT[ch]}")
                    target = out_dir / CHANNEL_DIRS[ch] / name
                    _write_records(target, ch, window.records[ch],
                                   window.malformed[ch])
                manifest.write(json.dumps(window.manifest()) + "\n")
                manifest.flush()
                truth.writelines(json.dumps(t) + "\n" for t in window.truth)
                truth.flush()
                stats.add(window)
                if verbose:
                    _log(window)
        stats.finish()
        return out_dir

    # ---- windowing ------------------------------------------------------- #

    def _iter_windows(self, window_seconds: int, batch_id_fmt) -> Iterator[Window]:
        heap: list[_Emission] = []
        next_minute = 0
        index = 0
        window_start = self.start
        horizon = self.start + timedelta(minutes=self.minutes)
        step = timedelta(seconds=window_seconds)

        while next_minute < self.minutes or heap:
            window_end = window_start + step
            # Emissions are never earlier than their event time, so every
            # minute starting before window_end must be generated first.
            while (next_minute < self.minutes
                   and self.start + timedelta(minutes=next_minute) < window_end):
                for emission in self._generate_minute(next_minute):
                    heapq.heappush(heap, emission)
                next_minute += 1

            window = Window(index=index, batch_id=batch_id_fmt(window_start),
                            start=window_start, end=window_end)
            while heap and heap[0].emitted < window_end:
                self._release(heapq.heappop(heap), window)

            # After the horizon only late stragglers remain; skip empty windows.
            if window_start < horizon or window.total_transactions:
                yield window
                index += 1
            window_start = window_end

    @staticmethod
    def _release(em: _Emission, window: Window) -> None:
        emitted = _fmt_utc(em.emitted)
        bucket = window.records[em.channel]
        if em.defect == "malformed":
            window.malformed[em.channel].add(len(bucket))
        for rec in em.payload:
            rec["emitted_ts_utc"] = emitted
            rec["batch_id"] = window.batch_id
            bucket.append(rec)
        window.transactions[em.channel] += 1
        if em.late:
            window.late += 1
        # A duplicate counts once (on the copy), not on the original
        defect = em.defect if (em.defect != "duplicate" or em.is_duplicate) else None
        if defect:
            window.defects[defect] += 1
        if defect or em.late:
            first = em.payload[0]
            window.truth.append({
                "transaction_id": em.tx_id,
                "source_channel": em.channel,
                "batch_id": window.batch_id,
                "event_ts_utc": first["event_ts_utc"],
                "emitted_ts_utc": emitted,
                "defect": defect,
                "is_late": em.late,
                "is_duplicate_copy": em.is_duplicate,
            })

    # ---- generation ------------------------------------------------------ #

    def _generate_minute(self, minute: int) -> list[_Emission]:
        rng = random.Random(self.seed * 1_000_003 + minute)
        minute_start = self.start + timedelta(minutes=minute)
        n = max(0, round(rng.gauss(self.tx_per_minute, self.tx_per_minute * 0.05)))
        country_codes = list(COUNTRIES)
        country_weights = [COUNTRIES[c]["weight"] for c in country_codes]

        emissions: list[_Emission] = []
        for i in range(n):
            channel = rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0]
            cc = rng.choices(country_codes, country_weights)[0]
            event_dt = minute_start + timedelta(seconds=rng.random() * 60)
            tx = self._build_transaction(rng, channel, cc, event_dt)

            mean_lat, late_p, late_min, late_max = LATENCY[channel]
            late = rng.random() < late_p * self.late_rate_scale
            delay = (rng.uniform(late_min, late_max) if late
                     else rng.expovariate(1 / mean_lat))
            emitted = event_dt + timedelta(seconds=delay)

            defect = None
            if rng.random() < self.dirty_rate:
                defect = rng.choices(list(DEFECT_TYPES),
                                     list(DEFECT_TYPES.values()))[0]
                self._apply_defect(rng, tx, defect)

            payload = _FORMATTERS[channel](tx)
            emissions.append(_Emission(emitted, (minute, i, 0), channel,
                                       payload, defect, late,
                                       tx_id=tx["transaction_id"]))
            if defect == "duplicate":
                # Client/POS retry: same records re-sent seconds later
                retry_at = emitted + timedelta(seconds=rng.uniform(1, 90))
                emissions.append(_Emission(retry_at, (minute, i, 1), channel,
                                           copy.deepcopy(payload), defect,
                                           late, is_duplicate=True,
                                           tx_id=tx["transaction_id"]))
        return emissions

    def _build_transaction(self, rng: random.Random, channel: str, cc: str,
                           event_dt: datetime) -> dict:
        cfg = COUNTRIES[cc]
        store = rng.choice(STORES[cc])

        cust_n = rng.randint(1, CUSTOMER_POOL)
        is_member = cust_n <= CUSTOMER_POOL * LOYALTY_SHARE
        if channel == "LOYALTY_APP" and not is_member:
            cust_n = rng.randint(1, int(CUSTOMER_POOL * LOYALTY_SHARE))
            is_member = True
        identified = channel != "POS" or rng.random() < POS_IDENTIFIED_SHARE
        customer_id = f"CUS-{cc}-{cust_n:07d}" if identified else None
        loyalty_id = f"LYL-{cc}-{cust_n:07d}" if identified and is_member else None
        tier = None
        if loyalty_id:
            members = int(CUSTOMER_POOL * LOYALTY_SHARE)
            tier = ("PLATINUM", "GOLD", "SILVER", "BRONZE")[min(3, cust_n * 4 // members)]

        sizes, weights = BASKET_SIZES[channel]
        n_lines = rng.choices(sizes, weights)[0]
        lines = []
        for ln, (sku, name, cat, uom, usd) in enumerate(
                rng.sample(CATALOG, n_lines), start=1):
            unit_price = _round_to(usd * cfg["fx_per_usd"] * rng.uniform(0.95, 1.05),
                                   cfg["price_step"])
            qty = (round(rng.uniform(0.25, 3.0), 3) if uom == "KG"
                   else rng.choices((1, 2, 3, 4, 6), (60, 22, 10, 5, 3))[0])
            gross = unit_price * qty
            discount = round(gross * rng.choice((0.05, 0.10, 0.15, 0.20, 0.30)), 2) \
                if rng.random() < 0.15 else 0.0
            line_total = round(gross - discount, 2)
            tax = round(line_total - line_total / (1 + cfg["tax_rate"]), 2)
            lines.append({"line_number": ln, "sku": sku, "product_name": name,
                          "category": cat, "unit_of_measure": uom,
                          "quantity": qty, "unit_price": unit_price,
                          "discount_amount": discount, "tax_amount": tax,
                          "line_total": line_total})

        payments = dict(cfg["payments"])
        if channel != "POS":
            payments.pop("CASH")
            if channel == "ECOMMERCE":
                payments["CASH_ON_DELIVERY"] = 5

        total = round(sum(l["line_total"] for l in lines), 2)
        return {
            "transaction_id": _uuid(rng),
            "event_id": _uuid(rng),
            "channel": channel,
            "country_code": cc,
            "source_system": {
                "POS": cfg["pos_system"],
                "ECOMMERCE": "ECOM-VTEX",
                "LOYALTY_APP": "LOYALTY-APP",
            }[channel],
            "store_id": store["store_id"],
            "city": store["city"],
            "warehouse_id": store["warehouse_id"],
            "terminal_id": f"{store['store_id']}-T{rng.randint(1, 12):02d}",
            "cashier_id": f"CJ-{cc}-{rng.randint(1000, 9999)}",
            "session_id": _uuid(rng),
            "platform": rng.choices(("WEB", "ANDROID", "IOS"), (45, 35, 20))[0]
            if channel == "ECOMMERCE" else rng.choice(("ANDROID", "IOS")),
            "app_version": rng.choice(("5.2.0", "5.3.1", "5.4.0")),
            "delivery_type": rng.choices(
                ("HOME_DELIVERY", "CLICK_AND_COLLECT", "EXPRESS"), (60, 25, 15))[0],
            "customer_id": customer_id,
            "loyalty_id": loyalty_id,
            "loyalty_tier": tier,
            "payment_method": rng.choices(list(payments), list(payments.values()))[0],
            "currency": cfg["currency"],
            "utc_offset": cfg["utc_offset"],
            "lines": lines,
            "discount_total": round(sum(l["discount_amount"] for l in lines), 2),
            "tax_total": round(sum(l["tax_amount"] for l in lines), 2),
            "total": total,
            "points_earned": int(total // cfg["points_unit"]),
            "points_redeemed": rng.choice((0, 0, 0, 100, 250, 500)),
            "event_dt": event_dt,
        }

    @staticmethod
    def _apply_defect(rng: random.Random, tx: dict, defect: str) -> None:
        if defect == "missing_required":
            key = {"POS": "store_id", "ECOMMERCE": "warehouse_id",
                   "LOYALTY_APP": "loyalty_id"}[tx["channel"]]
            tx[key] = None
        elif defect == "negative_quantity":
            tx["lines"][0]["quantity"] = -abs(tx["lines"][0]["quantity"])
        elif defect == "amount_mismatch":
            tx["lines"][0]["line_total"] = round(tx["lines"][0]["line_total"] * 10, 2)
        elif defect == "invalid_country":
            cc = tx["country_code"]
            tx["country_code"] = rng.choice(
                (cc.lower(), {"CO": "COL", "MX": "MEX", "PE": "PER"}[cc], ""))
        elif defect == "future_event_ts":
            tx["event_dt"] = tx["event_dt"] + timedelta(days=rng.randint(1, 30))
        # "duplicate" and "malformed" are applied at emission / write time


# --------------------------------------------------------------------------- #
# Channel formatters (canonical transaction -> source-specific records)
# --------------------------------------------------------------------------- #

def _time_fields(tx: dict) -> dict:
    dt = tx["event_dt"]
    return {
        "event_ts_utc": _fmt_utc(dt),
        "event_ts_local": _fmt_local(dt, tx["utc_offset"]),
        "emitted_ts_utc": None,   # filled when the record is released
        "event_date": f"{dt.astimezone(timezone.utc):%Y-%m-%d}",
        "event_hour": dt.astimezone(timezone.utc).hour,
        "batch_id": None,         # filled when the record is released
        "schema_version": SCHEMA_VERSION,
    }


def _to_pos(tx: dict) -> list[dict]:
    times = _time_fields(tx)
    return [{
        "transaction_id": tx["transaction_id"],
        "line_number": line["line_number"],
        "source_channel": "POS",
        "source_system": tx["source_system"],
        "country_code": tx["country_code"],
        "city": tx["city"],
        "store_id": tx["store_id"],
        "warehouse_id": tx["warehouse_id"],
        "terminal_id": tx["terminal_id"],
        "cashier_id": tx["cashier_id"],
        "customer_id": tx["customer_id"],
        "loyalty_id": tx["loyalty_id"],
        "sku": line["sku"],
        "product_name": line["product_name"],
        "category": line["category"],
        "unit_of_measure": line["unit_of_measure"],
        "quantity": line["quantity"],
        "unit_price": line["unit_price"],
        "discount_amount": line["discount_amount"],
        "tax_amount": line["tax_amount"],
        "line_total": line["line_total"],
        "currency": tx["currency"],
        "payment_method": tx["payment_method"],
        **times,
    } for line in tx["lines"]]


def _items(tx: dict) -> list[dict]:
    return [{k: line[k] for k in ("sku", "product_name", "category",
                                  "unit_of_measure", "quantity", "unit_price",
                                  "discount_amount", "tax_amount",
                                  "line_total")}
            for line in tx["lines"]]


def _to_ecommerce(tx: dict) -> list[dict]:
    return [{
        "event_id": tx["event_id"],
        "event_type": "ORDER_PLACED",
        "order_id": tx["transaction_id"],
        "source_channel": "ECOMMERCE",
        "source_system": tx["source_system"],
        "country_code": tx["country_code"],
        "session_id": tx["session_id"],
        "customer": {"customer_id": tx["customer_id"],
                     "loyalty_id": tx["loyalty_id"]},
        "device": {"platform": tx["platform"],
                   "app_version": tx["app_version"]},
        "fulfillment": {"delivery_type": tx["delivery_type"],
                        "store_id": tx["store_id"],
                        "warehouse_id": tx["warehouse_id"],
                        "city": tx["city"]},
        "items": _items(tx),
        "payment": {"method": tx["payment_method"],
                    "currency": tx["currency"],
                    "discount_total": tx["discount_total"],
                    "tax_total": tx["tax_total"],
                    "total": tx["total"]},
        **_time_fields(tx),
    }]


def _to_loyalty(tx: dict) -> list[dict]:
    return [{
        "event_id": tx["event_id"],
        "event_type": "SCAN_AND_GO_PURCHASE",
        "transaction_id": tx["transaction_id"],
        "source_channel": "LOYALTY_APP",
        "source_system": f"{tx['source_system']}-{tx['platform']}",
        "country_code": tx["country_code"],
        "member": {"loyalty_id": tx["loyalty_id"],
                   "customer_id": tx["customer_id"],
                   "tier": tx["loyalty_tier"]},
        "store_id": tx["store_id"],
        "city": tx["city"],
        "app_version": tx["app_version"],
        "items": _items(tx),
        "amount": {"currency": tx["currency"], "total": tx["total"],
                   "tax_total": tx["tax_total"],
                   "discount_total": tx["discount_total"]},
        "points": {"earned": tx["points_earned"],
                   "redeemed": tx["points_redeemed"]},
        "payment_method": tx["payment_method"],
        **_time_fields(tx),
    }]


_FORMATTERS = {"POS": _to_pos, "ECOMMERCE": _to_ecommerce,
               "LOYALTY_APP": _to_loyalty}


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #

def _write_records(target: Path, channel: str, records: list[dict],
                   malformed: set[int]) -> None:
    """Write atomically (hidden temp file + rename). Records whose position is
    in ``malformed`` are truncated to simulate corrupted lines."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        if channel == "POS":
            writer = csv.writer(fh)
            writer.writerow(POS_FIELDS)
            for i, rec in enumerate(records):
                row = ["" if rec[f] is None else rec[f] for f in POS_FIELDS]
                writer.writerow(row[: len(row) // 2] if i in malformed else row)
        else:
            for i, rec in enumerate(records):
                line = json.dumps(rec, ensure_ascii=False)
                fh.write((line[: len(line) // 2] if i in malformed else line) + "\n")
    os.replace(tmp, target)


def _log(window: Window) -> None:
    print(f"[{window.batch_id}] tx={window.total_transactions:>5} "
          f"pos={window.transactions['POS']:>4} "
          f"ecom={window.transactions['ECOMMERCE']:>4} "
          f"loyalty={window.transactions['LOYALTY_APP']:>4} "
          f"late={window.late:>3} defects={sum(window.defects.values()):>3}",
          flush=True)


def _prepare_dir(out_dir: Path, clean: bool) -> None:
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


class _RunStats:
    """Accumulates totals for a run and writes ``_run.json`` at the end.
    Used as evidence for the volume / performance tests."""

    def __init__(self, mode: str, gen: "DataGenerator", out_dir: Path, **extra):
        self.mode, self.out_dir = mode, out_dir
        self.params = {"seed": gen.seed, "start": _fmt_utc(gen.start),
                       "minutes": gen.minutes, "tx_per_minute": gen.tx_per_minute,
                       "dirty_rate": gen.dirty_rate,
                       "late_rate_scale": gen.late_rate_scale, **extra}
        self.windows = 0
        self.transactions: Counter = Counter()
        self.records: Counter = Counter()
        self.defects: Counter = Counter()
        self.late = 0
        self.t0 = time.monotonic()

    def add(self, window: Window) -> None:
        self.windows += 1
        self.transactions.update(window.transactions)
        self.records.update({ch: len(r) for ch, r in window.records.items()})
        self.defects.update(window.defects)
        self.late += window.late

    def finish(self) -> dict:
        elapsed = time.monotonic() - self.t0
        files = [p for p in self.out_dir.rglob("*")
                 if p.is_file() and not p.name.startswith(("_", "."))]
        size = sum(p.stat().st_size for p in files)
        tx = sum(self.transactions.values())
        summary = {
            "mode": self.mode,
            "generated_at_utc": _fmt_utc(datetime.now(timezone.utc)),
            "params": self.params,
            "windows": self.windows,
            "transactions": tx,
            "transactions_by_channel": dict(self.transactions),
            "records_by_channel": dict(self.records),
            "late_arrivals": self.late,
            "injected_defects": dict(self.defects),
            "data_files": len(files),
            "data_size_mb": round(size / 1_048_576, 2),
            "elapsed_seconds": round(elapsed, 2),
            "transactions_per_second": round(tx / elapsed, 1) if elapsed else None,
        }
        (self.out_dir / "_run.json").write_text(json.dumps(summary, indent=2),
                                                 encoding="utf-8")
        print(f"Summary: {tx} tx, {sum(self.records.values())} records, "
              f"{self.late} late, {sum(self.defects.values())} defects, "
              f"{summary['data_files']} files, {summary['data_size_mb']} MB, "
              f"{summary['elapsed_seconds']} s", flush=True)
        return summary



# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="DATAMARCA synthetic data generator")
    sub = parser.add_subparsers(dest="mode", required=True)
    dims = sub.add_parser("dims", help="write only the dimension tables")
    dims.add_argument("--out", default=str(DATA_DIR / "dims"))
    for name in ("batch", "stream"):
        p = sub.add_parser(name)
        p.add_argument("--seed", type=int, default=DEFAULT_SEED)
        p.add_argument("--start", default=DEFAULT_START,
                       help="simulated start time, ISO-8601 UTC")
        p.add_argument("--minutes", type=int, default=DEFAULT_MINUTES,
                       help="simulated minutes to generate (default 120 = 2 h)")
        p.add_argument("--tx-per-minute", type=int, default=DEFAULT_TX_PER_MINUTE)
        p.add_argument("--dirty-rate", type=float, default=DEFAULT_DIRTY_RATE,
                       help="share of transactions with an injected defect")
        p.add_argument("--late-scale", type=float, default=1.0,
                       help="multiplier on late-arrival probabilities")
        p.add_argument("--out", default=str(DATA_DIR / name))
        p.add_argument("--clean", action="store_true",
                       help="delete the output folder before writing")
        p.add_argument("--quiet", action="store_true")
    sub.choices["stream"].add_argument(
        "--interval", type=int, default=10,
        help="simulated seconds per micro-batch file (must divide 60)")
    sub.choices["stream"].add_argument(
        "--speed", type=float, default=60.0,
        help="simulation speed-up: 1 = real time, 60 = 1 min per second, 0 = no wait")
    sub.add_parser("contract", help="print the data contract (schemas + rules)")
    args = parser.parse_args()

    if args.mode == "dims":
        print(f"Done -> {write_dimensions(args.out)}")
        return
    if args.mode == "contract":
        print(json.dumps(build_contract(), indent=2, ensure_ascii=False))
        return

    # Fail fast on invalid parameters instead of producing odd data
    try:
        _parse_utc(args.start)
    except ValueError:
        parser.error(f"--start must be ISO-8601 UTC, got {args.start!r}")
    if args.minutes <= 0:
        parser.error("--minutes must be > 0")
    if args.tx_per_minute <= 0:
        parser.error("--tx-per-minute must be > 0")
    if not 0 <= args.dirty_rate <= 1:
        parser.error("--dirty-rate must be between 0 and 1")
    if args.late_scale < 0:
        parser.error("--late-scale must be >= 0")
    if args.mode == "stream" and (args.interval <= 0 or 60 % args.interval):
        parser.error("--interval must be a positive divisor of 60 (1-6, 10, 12, 15, 20, 30, 60)")

    # Facts and dimensions always travel together: data/dims next to data/batch
    write_dimensions(Path(args.out).parent / "dims")

    gen = DataGenerator(seed=args.seed, start=args.start, minutes=args.minutes,
                        tx_per_minute=args.tx_per_minute,
                        dirty_rate=args.dirty_rate,
                        late_rate_scale=args.late_scale)
    if args.mode == "batch":
        out = gen.write_batch(args.out, clean=args.clean, verbose=not args.quiet)
    else:
        out = gen.write_stream(args.out, interval_seconds=args.interval,
                               speed=args.speed, clean=args.clean,
                               verbose=not args.quiet)
    print(f"Done -> {out}")


if __name__ == "__main__":
    main()
