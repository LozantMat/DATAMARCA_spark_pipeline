"""
DATAMARCA end-to-end pipeline (Unidad 3) - single entry point.

Chains the stages that otherwise run as separate scripts:

    1. generate   data_generator.py  raw feeds + dims + contract   (local only)
    2. ingest     ingest.py          bronze / silver / quarantine + DQ report
       -> quality gate: stop if the quarantine rate is above --max-reject-rate
    3. transform  transform.py       fact + gold tables + reconciliation
       -> quality gate: stop if the reconciliation checks fail

All stages share one run id and one Spark session, and the run ends with a
single consolidated report ``<lake>/_reports/pipeline_<run_id>.json``.
The exit code is 0 only if every stage and gate passed, so the same command
can be scheduled by cron, Airflow / Cloud Composer or a CI job.

Usage:
    python pipeline.py                                  # everything, defaults
    python pipeline.py --minutes 30 --scale 2           # smaller / bigger run
    python pipeline.py --stages ingest transform        # reuse existing data
    python pipeline.py --mode stream                    # ingest the stream files
    python pipeline.py --format delta
    spark-submit --py-files ingest.py,transform.py pipeline.py \\
        --stages ingest transform --input gs://<bucket>/raw/batch \\
        --dims gs://<bucket>/raw/dims --lake gs://<bucket>/lake
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

STAGES = ("generate", "ingest", "transform")


class GateFailed(RuntimeError):
    """A quality gate between stages did not pass."""


def _banner(text: str) -> None:
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}", flush=True)


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #

def stage_generate(args) -> dict:
    from data_generator import (DEFAULT_TX_PER_MINUTE, DataGenerator,
                                write_dimensions)
    if "://" in args.input:
        raise ValueError("generate writes local files only; upload them to the "
                         "bucket and run --stages ingest transform")
    # Generation replaces its target directory (clean=True). Do not silently
    # discard a previous dataset unless the caller explicitly allows it.
    target = Path(args.input)
    if target.exists() and any(target.iterdir()) and not args.overwrite_generated:
        raise FileExistsError(
            f"The generation destination {target} is not empty. "
            "Choose another --input or pass --overwrite-generated to replace it."
        )
    gen = DataGenerator(seed=args.seed, minutes=args.minutes,
                        tx_per_minute=int(DEFAULT_TX_PER_MINUTE * args.scale))
    write_dimensions(args.dims)
    if args.mode == "batch":
        gen.write_batch(args.input, clean=True, verbose=False)
    else:
        gen.write_stream(args.input, interval_seconds=10, speed=0,
                         clean=True, verbose=False)
    run = json.loads((Path(args.input) / "_run.json").read_text(encoding="utf-8"))
    return {"transactions": run["transactions"],
            "data_size_mb": run["data_size_mb"],
            "injected_defects": sum(run["injected_defects"].values()),
            "late_arrivals": run["late_arrivals"]}


def stage_ingest(args) -> dict:
    import ingest
    summary = ingest.run(args.input, args.dims, args.lake, args.format,
                         args.master, args.shuffle_partitions)
    counts = summary["counts"]
    reject_rate = counts["quarantine_lines"] / max(1, counts["bronze_lines"])
    result = {"run_id": summary["run_id"], **counts,
              "reject_rate": round(reject_rate, 4),
              "dq_rules": {r["rule"]: r["transactions"] for r in summary["dq"]["rules"]},
              "timings_s": summary["timings_s"]}
    if reject_rate > args.max_reject_rate:
        raise GateFailed(f"quarantine rate {reject_rate:.2%} > "
                         f"{args.max_reject_rate:.2%}: check the sources before "
                         "publishing gold tables")
    if counts["silver_lines"] == 0:
        raise GateFailed("silver is empty")
    return result


def stage_transform(args) -> dict:
    import transform
    summary = transform.run(args.lake, args.dims, args.format, args.master,
                            args.shuffle_partitions)
    recon = summary["reconciliation"]
    result = {"run_id": summary["run_id"], "gold_rows": summary["gold_rows"],
              "sales_usd": recon["fact_sales_usd"], "checks": recon["checks"],
              "timings_s": summary["timings_s"]}
    if not recon["all_ok"]:
        failed = [c for c, ok in recon["checks"].items() if not ok]
        raise GateFailed(f"reconciliation failed: {failed}")
    return result


RUNNERS = {"generate": stage_generate, "ingest": stage_ingest,
           "transform": stage_transform}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def write_report(args, report: dict) -> str:
    path = f"{args.lake}/_reports/pipeline_{report['run_id']}.json"
    if "://" in path or "ingest" in args.stages or "transform" in args.stages:
        try:  # reuse the writer that also handles gs://
            from ingest import write_json
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is not None:
                write_json(spark, path, report)
                return path
        except ImportError:
            pass
    if "://" in path:
        raise RuntimeError("Cannot write cloud report without an active Spark session: " + path)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return path


def run(args) -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    report = {"run_id": run_id, "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "params": {k: v for k, v in vars(args).items()},
              "stages": {}, "status": "RUNNING"}
    t_total = time.perf_counter()
    exit_code = 0

    for name in STAGES:
        if name not in args.stages:
            report["stages"][name] = {"status": "SKIPPED"}
            continue
        _banner(f"[{run_id}] stage: {name}")
        t0 = time.perf_counter()
        try:
            result = RUNNERS[name](args)
            report["stages"][name] = {"status": "OK", **result}
        except GateFailed as exc:
            report["stages"][name] = {"status": "GATE_FAILED", "error": str(exc)}
            exit_code = 2
        except Exception as exc:  # noqa: BLE001 - record and stop the pipeline
            traceback.print_exc()
            report["stages"][name] = {"status": "FAILED",
                                      "error": f"{type(exc).__name__}: {exc}"}
            exit_code = 1
        report["stages"][name]["duration_s"] = round(time.perf_counter() - t0, 2)
        if exit_code:
            for later in STAGES[STAGES.index(name) + 1:]:
                if later in args.stages:
                    report["stages"][later] = {"status": "NOT_RUN"}
            break

    report["duration_s"] = round(time.perf_counter() - t_total, 2)
    report["status"] = "SUCCESS" if exit_code == 0 else "FAILED"
    try:
        path = write_report(args, report)
    except Exception as exc:
        print(f"ERROR writing consolidated report: {exc}", file=sys.stderr)
        path = "UNAVAILABLE"
        if exit_code == 0:
            exit_code = 1
            report["status"] = "FAILED"

    _banner(f"Pipeline {report['status']}  ({report['duration_s']} s)")
    for name, st in report["stages"].items():
        extra = st.get("error", "")
        dur = f"{st['duration_s']:>7} s" if "duration_s" in st else " " * 9
        print(f"  {name:10} {st['status']:12} {dur}  {extra}")
    print(f"\nReport -> {path}")

    # Releasing Spark must not obscure the original failure or prevent the
    # report from being printed when Spark was never importable.
    try:
        from pyspark.sql import SparkSession
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
    except Exception as exc:
        print(f"WARNING: could not stop Spark cleanly: {exc}", file=sys.stderr)
    return exit_code


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="DATAMARCA end-to-end pipeline")
    p.add_argument("--stages", nargs="+", choices=STAGES, default=list(STAGES),
                   help="stages to run, in order (default: all)")
    p.add_argument("--mode", choices=("batch", "stream"), default="batch",
                   help="which raw layout to generate / ingest")
    p.add_argument("--input", default=None,
                   help="raw data folder (default: data/<mode>)")
    p.add_argument("--dims", default="data/dims")
    p.add_argument("--lake", default="lake")
    p.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    p.add_argument("--master", default=None,
                   help="e.g. local[4]; leave empty under spark-submit")
    p.add_argument("--shuffle-partitions", type=int, default=16)
    p.add_argument("--minutes", type=int, default=120, help="generate: simulated minutes")
    p.add_argument("--scale", type=float, default=1.0,
                   help="generate: volume multiplier over 2000 tx/min")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite-generated", action="store_true",
                   help="allow generate stage to replace existing input files")
    p.add_argument("--max-reject-rate", type=float, default=0.05,
                   help="gate after ingest: max share of quarantined lines")
    args = p.parse_args(argv)
    if args.minutes <= 0:
        p.error("--minutes must be positive")
    if args.scale <= 0:
        p.error("--scale must be positive")
    if args.shuffle_partitions < 1:
        p.error("--shuffle-partitions must be at least 1")
    if not 0 <= args.max_reject_rate <= 1:
        p.error("--max-reject-rate must be between 0 and 1")
    args.input = (args.input or f"data/{args.mode}").rstrip("/")
    args.dims = args.dims.rstrip("/")
    args.lake = args.lake.rstrip("/")
    args.stages = [s for s in STAGES if s in args.stages]  # always in order
    return args


if __name__ == "__main__":
    sys.exit(run(parse_args()))
