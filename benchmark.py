"""
Performance tests of the DATAMARCA Spark pipeline (Unidad 3, step 4).

Runs the full pipeline (data_generator -> ingest -> transform) under two
experiments and turns the timings into a cost estimate on Google Cloud:

  A) Volume   : same cores, 1x / 2x / 4x the transactions per minute
               -> does run time grow linearly with volume?
  B) Cores    : same volume, 1 / 2 / 4 ... local cores (Spark executors)
               -> how much does adding parallelism help (speed-up)?

Outputs (``--out``, default ``bench/``):
    results.csv / results.json   one row per run: volume, cores, timings,
                                 throughput, data-quality status
    cost_estimate.json           daily / monthly cost on Dataproc and
                                 Serverless for Apache Spark
    volume.png, cores.png        charts for the report (needs matplotlib)

Usage:
    python benchmark.py                          # defaults: 30 min, x1 x2 x4
    python benchmark.py --minutes 60 --scales 1 2 4 8 --cores 1 2 4 8
    python benchmark.py --skip-cores             # only the volume experiment

Local timings are a proxy: absolute numbers on a laptop differ from a cloud
cluster, but the *shape* (linear growth, speed-up per core) and the measured
throughput per core are what the cost model extrapolates from.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

import ingest
import transform
from data_generator import DEFAULT_TX_PER_MINUTE, DataGenerator, write_dimensions

# --------------------------------------------------------------------------- #
# Cloud prices (us-central1, on-demand, USD). Checked 2026-10-28:
#   Dataproc management fee .... https://cloud.google.com/dataproc/pricing
#   Serverless for Apache Spark  https://cloud.google.com/dataproc-serverless/pricing
#   n2-standard-4 VM ........... https://cloud.google.com/compute/vm-instance-pricing
# Edit these if the region or the prices change.
# --------------------------------------------------------------------------- #

PRICES = {
    "dataproc_fee_per_vcpu_hour": 0.010,
    "n2_standard_4_per_hour": 0.1942,       # 4 vCPU, 16 GB
    "serverless_dcu_hour_standard": 0.060,  # 1 DCU ~ 1 vCPU + 4 GB
    "min_billing_seconds": 60,
}

# Cluster sizes used in the estimate
DATAPROC_CLUSTER = {"master": 1, "workers": 2, "vcpu_per_node": 4}  # n2-standard-4
SERVERLESS_DCU = 12           # default batch: driver 4 + 2 executors x 4
CLUSTER_STARTUP_S = 90        # ephemeral Dataproc cluster create + delete
PARALLEL_EFFICIENCY = 0.70    # share of the ideal speed-up reached on a cluster
MONTHLY_BUDGET_USD = 20_000   # restriction carried over from Unidad 2


# --------------------------------------------------------------------------- #
# One run
# --------------------------------------------------------------------------- #

def _stop_spark() -> None:
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()


def generate(root: Path, scale: float, minutes: int, seed: int) -> dict:
    data = root / f"data_x{scale:g}"
    gen = DataGenerator(seed=seed, minutes=minutes,
                        tx_per_minute=int(DEFAULT_TX_PER_MINUTE * scale))
    gen.write_batch(data / "batch", clean=True, verbose=False)
    write_dimensions(data / "dims")
    return json.loads((data / "batch" / "_run.json").read_text(encoding="utf-8"))


def run_pipeline(root: Path, scale: float, cores: int, fmt: str,
                 shuffle_partitions: int) -> dict:
    data = root / f"data_x{scale:g}"
    lake = root / f"lake_x{scale:g}_c{cores}"
    shutil.rmtree(lake, ignore_errors=True)
    _stop_spark()  # a new master (local[n]) needs a new SparkContext
    master = f"local[{cores}]"
    ing = ingest.run(str(data / "batch"), str(data / "dims"), str(lake), fmt,
                     master, shuffle_partitions)
    trf = transform.run(str(lake), str(data / "dims"), fmt, master,
                        shuffle_partitions)
    shutil.rmtree(lake, ignore_errors=True)  # keep disk usage bounded
    return {"ingest": ing, "transform": trf}


def to_row(experiment: str, scale: float, cores: int, gen: dict, out: dict) -> dict:
    ing, trf = out["ingest"], out["transform"]
    lines = ing["counts"]["bronze_lines"]
    total = ing["timings_s"]["total"] + trf["timings_s"]["total"]
    dq_ok = all(r.get("recall") == 1.0 and r.get("precision") == 1.0
                for r in ing["dq"]["rules"])
    return {
        "experiment": experiment, "scale": scale, "cores": cores,
        "transactions": gen["transactions"], "input_mb": gen["data_size_mb"],
        "bronze_lines": lines, "silver_lines": ing["counts"]["silver_lines"],
        "ingest_s": ing["timings_s"]["total"],
        "transform_s": trf["timings_s"]["total"],
        "total_s": round(total, 2),
        "lines_per_s": round(lines / total, 1),
        "lines_per_s_per_core": round(lines / total / cores, 1),
        "read_normalize_s": ing["timings_s"]["read_normalize"],
        "validate_s": ing["timings_s"]["validate"],
        "write_silver_s": ing["timings_s"]["write_silver"],
        "enrich_s": trf["timings_s"]["enrich"],
        "dq_all_rules_exact": dq_ok,
        "reconciliation_ok": trf["reconciliation"]["all_ok"],
    }


# --------------------------------------------------------------------------- #
# Cost model
# --------------------------------------------------------------------------- #

def _billed_hours(seconds: float) -> float:
    return max(seconds, PRICES["min_billing_seconds"]) / 3600


def fit_scaling(rows: list[dict]) -> dict:
    """Least-squares fit of the volume experiment: total_s = fixed_s + lines * s_per_line.

    ``fixed_s`` is the per-run overhead (job scheduling, small files, reports)
    and ``1 / s_per_line`` the marginal throughput, which is what really
    scales with volume. Falls back to the slowest per-core rate (conservative)
    if there are fewer than two volume points.
    """
    vol = [r for r in rows if r["experiment"] == "volume"]
    cores = vol[0]["cores"] if vol else rows[0]["cores"]
    if len(vol) >= 2:
        xs = [r["bronze_lines"] for r in vol]
        ys = [r["total_s"] for r in vol]
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0
        if slope > 0:
            fixed = max(0.0, my - slope * mx)
            ss_res = sum((y - (fixed + slope * x)) ** 2 for x, y in zip(xs, ys))
            ss_tot = sum((y - my) ** 2 for y in ys)
            return {"method": "linear_fit_volume_experiment", "cores": cores,
                    "fixed_overhead_s": round(fixed, 2),
                    "marginal_lines_per_s": round(1 / slope, 1),
                    "lines_per_s_per_core": round(1 / slope / cores, 1),
                    "r2": round(1 - ss_res / ss_tot, 4) if ss_tot else 1.0}
    slowest = min(rows, key=lambda r: r["lines_per_s_per_core"])
    return {"method": "slowest_measured_run", "cores": slowest["cores"],
            "fixed_overhead_s": 0.0,
            "marginal_lines_per_s": slowest["lines_per_s"],
            "lines_per_s_per_core": slowest["lines_per_s_per_core"], "r2": None}


def cost_estimate(rows: list[dict], minutes: int) -> dict:
    """Extrapolate the measured throughput per core to the real daily volume
    of DATAMARCA (2000 tx/min, 24 h) and price it on GCP."""
    fit = fit_scaling(rows)
    largest = max(rows, key=lambda r: r["bronze_lines"])
    lines_per_tx = largest["bronze_lines"] / largest["transactions"]
    daily_tx = DEFAULT_TX_PER_MINUTE * 60 * 24
    daily_lines = daily_tx * lines_per_tx
    daily_mb = largest["input_mb"] / largest["transactions"] * daily_tx

    worker_vcpu = DATAPROC_CLUSTER["workers"] * DATAPROC_CLUSTER["vcpu_per_node"]
    nodes = DATAPROC_CLUSTER["master"] + DATAPROC_CLUSTER["workers"]
    node_vcpu = nodes * DATAPROC_CLUSTER["vcpu_per_node"]
    eff_rate = fit["lines_per_s_per_core"] * PARALLEL_EFFICIENCY
    overhead = fit["fixed_overhead_s"]
    serverless_exec_vcpu = SERVERLESS_DCU - 4  # the driver does not process data

    def dataproc(runs_per_day: int) -> dict:
        lines = daily_lines / runs_per_day
        processing = overhead + lines / (eff_rate * worker_vcpu)
        seconds = processing + CLUSTER_STARTUP_S
        per_hour = (nodes * PRICES["n2_standard_4_per_hour"]
                    + node_vcpu * PRICES["dataproc_fee_per_vcpu_hour"])
        per_run = _billed_hours(seconds) * per_hour
        return {"runs_per_day": runs_per_day,
                "seconds_per_run": round(seconds, 1),
                "cost_per_run_usd": round(per_run, 4),
                "cost_per_day_usd": round(per_run * runs_per_day, 2),
                "cost_per_month_usd": round(per_run * runs_per_day * 30, 2)}

    def serverless(runs_per_day: int) -> dict:
        lines = daily_lines / runs_per_day
        seconds = overhead + lines / (eff_rate * serverless_exec_vcpu)
        per_run = (_billed_hours(seconds) * SERVERLESS_DCU
                   * PRICES["serverless_dcu_hour_standard"])
        return {"runs_per_day": runs_per_day,
                "seconds_per_run": round(seconds, 1),
                "cost_per_run_usd": round(per_run, 4),
                "cost_per_day_usd": round(per_run * runs_per_day, 2),
                "cost_per_month_usd": round(per_run * runs_per_day * 30, 2)}

    scenarios = {"daily_batch": 1, "hourly_batch": 24, "every_15_min": 96}
    est = {
        "basis": {"throughput_model": fit,
                  "simulated_minutes_per_run": minutes,
                  "daily_transactions": daily_tx,
                  "daily_lines": round(daily_lines),
                  "daily_raw_gb": round(daily_mb / 1024, 2),
                  "parallel_efficiency": PARALLEL_EFFICIENCY},
        "prices_usd": PRICES,
        "dataproc_cluster": {**DATAPROC_CLUSTER, "machine": "n2-standard-4",
                             "startup_s": CLUSTER_STARTUP_S},
        "serverless_dcu": SERVERLESS_DCU,
        "dataproc": {name: dataproc(n) for name, n in scenarios.items()},
        "serverless": {name: serverless(n) for name, n in scenarios.items()},
        "monthly_budget_usd": MONTHLY_BUDGET_USD,
    }
    worst = max(v["cost_per_month_usd"] for k in ("dataproc", "serverless")
                for v in est[k].values())
    est["worst_case_share_of_budget"] = round(worst / MONTHLY_BUDGET_USD, 5)
    return est


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #

def plot(rows: list[dict], out: Path) -> list[Path]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed: skipping charts")
        return []

    ink, muted, grid = "#0b0b0b", "#52514e", "#e4e3df"
    blue, orange = "#2a78d6", "#eb6834"
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": grid,
                         "axes.labelcolor": muted, "xtick.color": muted,
                         "ytick.color": muted, "axes.spines.top": False,
                         "axes.spines.right": False})
    paths = []

    vol = sorted((r for r in rows if r["experiment"] == "volume"),
                 key=lambda r: r["scale"])
    if vol:
        fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=150)
        x = [r["bronze_lines"] / 1e6 for r in vol]
        for key, label, color in (("ingest_s", "Ingestión + calidad", blue),
                                  ("transform_s", "Transformación (gold)", orange)):
            y = [r[key] for r in vol]
            ax.plot(x, y, color=color, lw=2, marker="o", ms=6, label=label)
        ax.set_xlabel("Líneas procesadas (millones)")
        ax.set_ylabel("Tiempo (s)")
        ax.set_title(f"Tiempo vs. volumen ({vol[0]['cores']} núcleos)",
                     loc="left", color=ink, fontsize=11)
        ax.grid(axis="y", color=grid, lw=0.8)
        ax.set_ylim(bottom=0)
        ax.set_xlim(0, max(x) * 1.1)
        ax.legend(frameon=False, loc="upper left")
        fig.tight_layout()
        p = out / "volume.png"
        fig.savefig(p)
        plt.close(fig)
        paths.append(p)

    cor = sorted((r for r in rows if r["experiment"] == "cores"),
                 key=lambda r: r["cores"])
    if len(cor) > 1:
        fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=150)
        labels = [str(r["cores"]) for r in cor]
        y = [r["lines_per_s"] / 1000 for r in cor]
        bars = ax.bar(labels, y, color=blue, width=0.55)
        base = cor[0]["total_s"]
        for b, r in zip(bars, cor):
            ax.annotate(f"{base / r['total_s']:.2f}x", (b.get_x() + b.get_width() / 2,
                        b.get_height()), xytext=(0, 4), textcoords="offset points",
                        ha="center", color=ink, fontsize=9)
        ax.set_xlabel("Núcleos (local[n])")
        ax.set_ylabel("Miles de líneas por segundo")
        ax.set_title(f"Throughput por núcleos (x{cor[0]['scale']:g} volumen) · "
                     "etiqueta = speed-up", loc="left", color=ink, fontsize=11)
        ax.grid(axis="y", color=grid, lw=0.8)
        ax.set_axisbelow(True)
        fig.tight_layout()
        p = out / "cores.png"
        fig.savefig(p)
        plt.close(fig)
        paths.append(p)
    return paths


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def _write_rows(rows: list[dict], out: Path) -> None:
    (out / "results.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    with (out / "results.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _print_table(rows: list[dict]) -> None:
    print(f"\n{'exp':7} {'scale':>5} {'cores':>5} {'lines':>9} {'ingest':>7} "
          f"{'transf':>7} {'total':>7} {'lines/s':>8} {'/core':>7} dq  recon")
    for r in rows:
        print(f"{r['experiment']:7} {r['scale']:>5g} {r['cores']:>5} "
              f"{r['bronze_lines']:>9} {r['ingest_s']:>7} {r['transform_s']:>7} "
              f"{r['total_s']:>7} {r['lines_per_s']:>8} {r['lines_per_s_per_core']:>7} "
              f"{'OK' if r['dq_all_rules_exact'] else '!!'}  "
              f"{'OK' if r['reconciliation_ok'] else '!!'}")


def main() -> None:
    cpu = os.cpu_count() or 1
    parser = argparse.ArgumentParser(description="DATAMARCA pipeline benchmark")
    parser.add_argument("--minutes", type=int, default=30,
                        help="simulated minutes per dataset (default 30)")
    parser.add_argument("--scales", type=float, nargs="+", default=[1, 2, 4],
                        help="volume multipliers over 2000 tx/min")
    parser.add_argument("--cores", type=int, nargs="+", default=[1, 2, 4, 8],
                        help="local cores for the parallelism experiment")
    parser.add_argument("--cores-scale", type=float, default=None,
                        help="volume used in the cores experiment (default: largest scale)")
    parser.add_argument("--skip-cores", action="store_true")
    parser.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="bench")
    parser.add_argument("--keep-data", action="store_true",
                        help="keep the generated datasets under <out>/work")
    args = parser.parse_args()

    out = Path(args.out)
    work = out / "work"
    out.mkdir(parents=True, exist_ok=True)
    cores = sorted({c for c in args.cores if c <= cpu}) or [1]
    vol_cores = max(cores)
    cores_scale = args.cores_scale or max(args.scales)
    if any(c > cpu for c in args.cores):
        print(f"Note: this machine has {cpu} cores; testing {cores}")

    t0 = time.perf_counter()
    print("Warm-up run (JVM/JIT), not recorded ...")
    generate(work, 0.25, 2, args.seed)
    run_pipeline(work, 0.25, vol_cores, args.format, args.shuffle_partitions)

    rows: list[dict] = []
    generated: dict[float, dict] = {}
    for scale in args.scales:
        print(f"\n=== Volume x{scale:g} ({vol_cores} cores) ===")
        generated[scale] = generate(work, scale, args.minutes, args.seed)
        res = run_pipeline(work, scale, vol_cores, args.format, args.shuffle_partitions)
        rows.append(to_row("volume", scale, vol_cores, generated[scale], res))
        _write_rows(rows, out)

    if not args.skip_cores and len(cores) > 1:
        if cores_scale not in generated:
            generated[cores_scale] = generate(work, cores_scale, args.minutes, args.seed)
        for c in cores:
            print(f"\n=== Cores {c} (x{cores_scale:g} volume) ===")
            res = run_pipeline(work, cores_scale, c, args.format, args.shuffle_partitions)
            rows.append(to_row("cores", cores_scale, c, generated[cores_scale], res))
            _write_rows(rows, out)
    _stop_spark()

    est = cost_estimate(rows, args.minutes)
    (out / "cost_estimate.json").write_text(json.dumps(est, indent=2), encoding="utf-8")
    charts = plot(rows, out)
    if not args.keep_data:
        shutil.rmtree(work, ignore_errors=True)

    _print_table(rows)
    fit = est["basis"]["throughput_model"]
    print(f"\nScaling model ({fit['method']}): fixed overhead {fit['fixed_overhead_s']} s, "
          f"{fit['marginal_lines_per_s']:,} lines/s on {fit['cores']} cores "
          f"({fit['lines_per_s_per_core']:,}/core), R2={fit['r2']}")
    print("\nCost estimate for the real volume "
          f"({est['basis']['daily_transactions']:,} tx/day, "
          f"{est['basis']['daily_raw_gb']} GB raw/day):")
    for platform in ("dataproc", "serverless"):
        for name, s in est[platform].items():
            print(f"  {platform:10} {name:13} {s['seconds_per_run']:>8} s/run  "
                  f"{s['cost_per_month_usd']:>8.2f} USD/month")
    print(f"  worst case = {est['worst_case_share_of_budget'] * 100:.3f}% "
          f"of the USD {MONTHLY_BUDGET_USD:,} monthly budget")
    print(f"\nResults -> {out}/results.csv, {out}/cost_estimate.json"
          + "".join(f", {p}" for p in charts))
    print(f"Benchmark wall time: {math.ceil(time.perf_counter() - t0)} s")


if __name__ == "__main__":
    sys.exit(main())
