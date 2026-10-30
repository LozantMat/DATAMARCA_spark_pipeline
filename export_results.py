"""
Export the results of a DATAMARCA pipeline run for the team (Unidad 3).

Reads the reports of the latest successful run (``<lake>/_reports``) and the
gold tables, and writes to ``<lake>/exports/<run_id>/``:

    01_resumen_ejecucion.csv      run status, durations and row counts
    02_tiempos_por_etapa.csv      seconds per step (ingest + transform)
    03_calidad_reglas.csv         DQ01-DQ07: detected vs injected, recall, precision
    04_reconciliacion.csv         reconciliation checks of the gold layer
    gold_*.csv                    the five gold tables
    reporte_datamarca_<run>.html  self-contained report (KPIs, charts, tables)
    datamarca_resultados_<run>.zip  everything above in one download
    LEEME.txt                     what each file holds

CSV files are UTF-8 with BOM (Excel opens accents correctly); use ``--sep ";"``
when the team's Excel uses comma as decimal separator.

Usage:
    python export_results.py --lake lake
    # Databricks (second task of the job, after pipeline.py):
    export_results.py --lake /Volumes/workspace/datamarca/lake --tables workspace.datamarca
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import shutil
import tempfile
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import ingest

GOLD = ["daily_sales", "hourly_sales", "store_daily_kpis",
        "warehouse_daily_demand", "dq_daily_rejections"]

# Categorical slots 1-3 (validated: CVD-safe, light + dark) + text tokens
CHANNEL_COLORS = {"POS": "var(--s1)", "ECOMMERCE": "var(--s2)", "LOYALTY_APP": "var(--s3)"}
CHANNEL_NAMES = {"POS": "Tiendas (POS)", "ECOMMERCE": "E-commerce", "LOYALTY_APP": "App fidelización"}


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #

def _local(path: str) -> Path:
    if "://" in path:
        raise ValueError("export_results.py reads reports from local disk or "
                         "Databricks /Volumes; copy gs:// reports locally first")
    return Path(path[len("dbfs:"):] if path.startswith("dbfs:") else path)


def load_reports(lake: str, run_id: str | None) -> dict:
    reports = _local(lake) / "_reports"
    if run_id:
        candidates = [reports / f"pipeline_{run_id}.json"]
    else:
        candidates = sorted(reports.glob("pipeline_*.json"), reverse=True)
    for path in candidates:
        if not path.exists():
            continue
        run = json.loads(path.read_text(encoding="utf-8"))
        if run_id or run.get("status") == "SUCCESS":
            break
    else:
        raise FileNotFoundError(f"no successful pipeline report in {reports}")

    def stage_report(stage: str) -> dict:
        sid = run["stages"].get(stage, {}).get("run_id")
        p = reports / f"{stage}_{sid}.json"
        return json.loads(p.read_text(encoding="utf-8")) if sid and p.exists() else {}

    return {"pipeline": run, "ingest": stage_report("ingest"),
            "transform": stage_report("transform")}


def read_gold(spark, lake: str, fmt: str) -> dict[str, tuple[list[str], list[list]]]:
    out = {}
    for name in GOLD:
        df = ingest.read_table(spark, f"{lake}/gold/{name}", fmt)
        cols = df.columns
        out[name] = (cols, [[r[c] for c in cols] for r in df.collect()])
    return out


# --------------------------------------------------------------------------- #
# CSV
# --------------------------------------------------------------------------- #

def _cell(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    return "" if v is None else v


def write_csv(path: Path, header: list[str], rows: list[list], sep: str) -> int:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh, delimiter=sep)
        w.writerow(header)
        w.writerows([[_cell(v) for v in r] for r in rows])
    return len(rows)


def summary_rows(rep: dict) -> list[list]:
    run, ing = rep["pipeline"], rep["ingest"]
    st = run["stages"]
    recon = rep["transform"].get("reconciliation", {})
    rows = [["run_id", run["run_id"]], ["estado", run["status"]],
            ["inicio_utc", run.get("started_at_utc")],
            ["duracion_total_s", run.get("duration_s")]]
    rows += [[f"duracion_{s}_s", v.get("duration_s")] for s, v in st.items()
             if v.get("status") == "OK"]
    g = st.get("generate", {})
    rows += [["transacciones_generadas", g.get("transactions")],
             ["datos_generados_mb", g.get("data_size_mb")],
             ["defectos_inyectados", g.get("injected_defects")]]
    i = st.get("ingest", {})
    rows += [["lineas_bronze", i.get("bronze_lines")],
             ["lineas_silver", i.get("silver_lines")],
             ["lineas_cuarentena", i.get("quarantine_lines")],
             ["tasa_rechazo", i.get("reject_rate")],
             ["lineas_tardias_silver", ing.get("dq", {}).get("late_lines_in_silver")],
             ["transacciones_validas", recon.get("silver_transactions")],
             ["ventas_usd", recon.get("fact_sales_usd")],
             ["reconciliacion_ok", recon.get("all_ok")],
             ["formato", ing.get("format")], ["tablas", ing.get("tables")]]
    return rows


def timing_rows(rep: dict) -> list[list]:
    rows = []
    for stage in ("ingest", "transform"):
        for step, sec in rep[stage].get("timings_s", {}).items():
            if step != "total":
                rows.append([stage, step, sec])
    return rows


# --------------------------------------------------------------------------- #
# HTML report (inline SVG, light + dark, no external assets)
# --------------------------------------------------------------------------- #

def num(v, dec=0) -> str:
    """es-CO number format: 1.234.567,89"""
    if v is None:
        return "–"
    s = f"{float(v):,.{dec}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def esc(v) -> str:
    return html.escape(str(v))


def _bar(x, y, w, h, color, tip) -> str:
    """Horizontal bar, square at the baseline, 4px rounded data end."""
    r = min(4, w / 2, h / 2)
    if w <= 0:
        return ""
    d = (f"M{x:.1f} {y:.1f}h{w - r:.1f}a{r} {r} 0 0 1 {r} {r}"
         f"v{h - 2 * r:.1f}a{r} {r} 0 0 1 {-r} {r}h{-(w - r):.1f}z")
    return f'<path d="{d}" fill="{color}"><title>{esc(tip)}</title></path>'


def hbars(title: str, items: list[tuple[str, float | None]], unit: str,
          dec: int = 0) -> str:
    """Single-series horizontal bars (sorted by the caller), value at bar end.
    An item whose value is None is a group heading row."""
    lab, plot, row = 230, 420, 26
    top = 40
    hgt = top + row * len(items) + 16
    vmax = max((v for _, v in items if v is not None), default=1) or 1
    parts = [f'<svg viewBox="0 0 760 {hgt}" role="img" aria-label="{esc(title)}">',
             f'<text x="0" y="18" class="ct">{esc(title)}</text>']
    for k, (name, v) in enumerate(items):
        y = top + k * row
        if v is None:
            parts.append(f'<text x="0" y="{y + 16}" class="cl b">{esc(name)}</text>')
            continue
        w = v / vmax * plot
        parts.append(f'<text x="{lab - 10}" y="{y + 14}" text-anchor="end" class="cl">{esc(name)}</text>')
        parts.append(_bar(lab, y + 2, w, 16, "var(--s1)", f"{name}: {num(v, dec)} {unit}"))
        parts.append(f'<text x="{lab + w + 6:.1f}" y="{y + 14}" class="cv">{num(v, dec)}</text>')
    parts.append(f'<line x1="{lab}" x2="{lab}" y1="{top - 4}" y2="{hgt - 14}" class="ax"/></svg>')
    return "".join(parts)


def grouped_hbars(title: str, groups: list[str], series: list[str],
                  value: dict[tuple[str, str], float]) -> str:
    """Countries (groups) x channels (series), legend on top, value labels."""
    lab, plot, bh, gap, ggap = 190, 440, 14, 2, 18
    top = 64
    gh = len(series) * (bh + gap) - gap
    hgt = top + len(groups) * (gh + ggap) + 8
    vmax = max(value.values(), default=1) or 1
    parts = [f'<svg viewBox="0 0 760 {hgt}" role="img" aria-label="{esc(title)}">',
             f'<text x="0" y="18" class="ct">{esc(title)}</text>']
    lx = 0
    for s in series:
        parts.append(f'<rect x="{lx}" y="32" width="12" height="12" rx="2" fill="{CHANNEL_COLORS[s]}"/>'
                     f'<text x="{lx + 18}" y="42" class="cl">{esc(CHANNEL_NAMES[s])}</text>')
        lx += 30 + 7 * len(CHANNEL_NAMES[s])
    for gi, g in enumerate(groups):
        y0 = top + gi * (gh + ggap)
        parts.append(f'<text x="{lab - 10}" y="{y0 + gh / 2 + 4:.1f}" text-anchor="end" class="cl b">{esc(g)}</text>')
        for si, s in enumerate(series):
            v = value.get((g, s), 0.0)
            y = y0 + si * (bh + gap)
            w = v / vmax * plot
            parts.append(_bar(lab, y, w, bh, CHANNEL_COLORS[s],
                              f"{g} · {CHANNEL_NAMES[s]}: {num(v)} USD"))
            parts.append(f'<text x="{lab + w + 6:.1f}" y="{y + 11}" class="cv">{num(v)}</text>')
    parts.append(f'<line x1="{lab}" x2="{lab}" y1="{top - 4}" y2="{hgt - 8}" class="ax"/></svg>')
    return "".join(parts)


def table(header: list[str], rows: list[list], num_cols: set[int] = frozenset()) -> str:
    th = "".join(f'<th class="{"n" if i in num_cols else ""}">{esc(h)}</th>'
                 for i, h in enumerate(header))
    body = "".join("<tr>" + "".join(
        f'<td class="{"n" if i in num_cols else ""}">{c}</td>' for i, c in enumerate(r)) + "</tr>"
        for r in rows)
    return f"<table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"


def build_html(rep: dict, gold: dict, files: list[tuple[str, int]]) -> str:
    run, ing, tr = rep["pipeline"], rep["ingest"], rep["transform"]
    st, recon = run["stages"], tr.get("reconciliation", {})
    cols, rows = gold["daily_sales"]
    ix = {c: i for i, c in enumerate(cols)}

    # sales by country x channel and by category
    names, by_cc, by_cat = {}, {}, {}
    for r in rows:
        cc, ch, cat, usd = (r[ix["country_code"]], r[ix["source_channel"]],
                            r[ix["category"]], float(r[ix["sales_usd"]] or 0))
        names[cc] = r[ix["country_name"]] or cc
        by_cc[(names[cc], ch)] = by_cc.get((names[cc], ch), 0.0) + usd
        by_cat[cat] = by_cat.get(cat, 0.0) + usd
    totals = {}
    for (g, _), v in by_cc.items():
        totals[g] = totals.get(g, 0.0) + v
    groups = sorted(totals, key=totals.get, reverse=True)
    series = [s for s in CHANNEL_COLORS if any(k[1] == s for k in by_cc)]

    sales = recon.get("fact_sales_usd") or sum(totals.values())
    tx = recon.get("silver_transactions")
    i = st.get("ingest", {})
    late = ing.get("dq", {}).get("late_lines_in_silver")
    kpis = [("Ventas totales", f"{num(sales)} USD"),
            ("Transacciones válidas", num(tx)),
            ("Ticket promedio", f"{num(sales / tx, 2)} USD" if tx else "–"),
            ("Líneas rechazadas", f"{num((i.get('reject_rate') or 0) * 100, 2)} %"),
            ("Líneas tardías", f"{num(late / i['silver_lines'] * 100, 2)} %"
             if late is not None and i.get("silver_lines") else "–"),
            ("Duración del pipeline", f"{num(run.get('duration_s'), 0)} s")]
    kpi_html = "".join(f'<div class="k"><div class="kl">{esc(a)}</div><div class="kv">{esc(b)}</div></div>'
                       for a, b in kpis)

    stage_rows = [[esc(s), esc(v.get("status")), num(v.get("duration_s"), 1)]
                  for s, v in st.items()]
    steps = []
    for stage in ("ingest", "transform"):
        steps.append((stage.capitalize(), None))
        steps += [(p, float(sec)) for s, p, sec in timing_rows(rep) if s == stage]

    dq_rows = [[esc(r["rule"]), esc(r["name"]), num(r.get("injected")), num(r["transactions"]),
                num(r["lines"]), num((r.get("recall") or 0) * 100, 1) + " %",
                num((r.get("precision") or 0) * 100, 1) + " %"]
               for r in ing.get("dq", {}).get("rules", [])]
    check_rows = [[("✔ OK" if ok else "✘ FALLA"), esc(c)]
                  for c, ok in recon.get("checks", {}).items()]

    pivot = [[esc(g)] + [num(by_cc.get((g, s), 0)) for s in series] + [num(totals[g])]
             for g in groups]
    cat_items = sorted(by_cat.items(), key=lambda kv: kv[1], reverse=True)

    def top(name, key, n, pick):
        c, rr = gold[name]
        j = {x: k for k, x in enumerate(c)}
        rr = sorted(rr, key=lambda r: float(r[j[key]] or 0), reverse=True)[:n]
        return [[esc(r[j[p]]) if not isinstance(r[j[p]], (int, float, Decimal)) or p in ("store_id",)
                 else num(r[j[p]], 2 if "usd" in p or p == "quantity_sold" else 0)
                 for p in pick] for r in rr]

    stores = top("store_daily_kpis", "sales_usd", 10,
                 ["store_id", "city", "country_code", "tickets", "sales_usd", "avg_ticket_usd"])
    demand = top("warehouse_daily_demand", "sales_usd", 10,
                 ["warehouse_id", "product_name", "unit_of_measure", "quantity_sold", "sales_usd"])

    file_rows = [[f"<code>{esc(f)}</code>", num(n) if n >= 0 else "–"] for f, n in files]
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DATAMARCA · Resultados del pipeline {esc(run['run_id'])}</title>
<style>
:root{{--bg:#fcfcfb;--fg:#0b0b0b;--fg2:#52514e;--rule:#e4e3df;--tint:#f3f2ef;
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--good:#0ca30c;--bad:#d03b3b}}
@media (prefers-color-scheme:dark){{:root{{--bg:#1a1a19;--fg:#f0efec;--fg2:#c3c2b7;
--rule:#383835;--tint:#232322;--s1:#3987e5;--s2:#d95926;--s3:#199e70}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:880px;margin:0 auto;padding:32px 24px 64px}}
h1{{font-size:26px;margin:0 0 4px}} h2{{font-size:19px;margin:40px 0 8px}}
.sub{{color:var(--fg2);margin:0 0 24px}} p{{max-width:72ch}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}}
.k{{background:var(--tint);border-radius:8px;padding:12px 14px}}
.kl{{color:var(--fg2);font-size:13px}} .kv{{font-size:22px;font-weight:600}}
table{{border-collapse:collapse;width:100%;margin:8px 0 4px;font-size:14px}}
th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid var(--rule)}}
th{{color:var(--fg2);font-weight:600}} .n{{text-align:right;font-variant-numeric:tabular-nums}}
svg{{width:100%;height:auto;margin:12px 0}} .ct{{font-size:15px;font-weight:600;fill:var(--fg)}}
.cl{{font-size:12.5px;fill:var(--fg)}} .cv{{font-size:12px;fill:var(--fg2)}} .b{{font-weight:600}}
.ax{{stroke:var(--fg2);stroke-width:1}} code{{font-size:13px}}
footer{{color:var(--fg2);font-size:13px;margin-top:40px}}
</style></head><body><main>
<h1>DATAMARCA S.A. · Resultados del pipeline</h1>
<p class="sub">Ejecución <code>{esc(run['run_id'])}</code> · estado <b>{esc(run['status'])}</b> ·
inicio {esc(run.get('started_at_utc', '')[:19].replace('T', ' '))} UTC · formato {esc(ing.get('format', ''))}</p>
<div class="kpis">{kpi_html}</div>

<h2>Ventas por país y canal</h2>
<p>{esc(groups[0]) if groups else ''} concentra la mayor parte de las ventas; los montos están en USD
con la tasa de cambio de <code>dim_countries</code>.</p>
{grouped_hbars("Ventas (USD) por país y canal", groups, series, by_cc)}
{table(["País"] + [CHANNEL_NAMES[s] for s in series] + ["Total"], pivot, set(range(1, len(series) + 2)))}

<h2>Ventas por categoría</h2>
{hbars("Ventas (USD) por categoría", cat_items, "USD")}

<h2>Calidad de datos</h2>
<p>Cada regla se compara con los defectos que el generador inyectó (<code>_truth.jsonl</code>).
Recall = defectos encontrados / inyectados; precisión = encontrados correctos / marcados.</p>
{table(["Regla", "Nombre", "Inyectadas (tx)", "Detectadas (tx)", "Líneas", "Recall", "Precisión"], dq_rows, {2, 3, 4, 5, 6})}

<h2>Reconciliación de la capa gold</h2>
{table(["Resultado", "Verificación"], check_rows)}

<h2>Tiempos de ejecución</h2>
{table(["Etapa", "Estado", "Duración (s)"], stage_rows, {2})}
{hbars("Segundos por paso (ingest y transform)", steps, "s", 1)}

<h2>Top 10 tiendas por ventas</h2>
{table(["Tienda", "Ciudad", "País", "Tickets", "Ventas (USD)", "Ticket prom. (USD)"], stores, {3, 4, 5})}

<h2>Top 10 demanda por CEDI y producto</h2>
{table(["CEDI", "Producto", "Unidad", "Cantidad", "Ventas (USD)"], demand, {3, 4})}

<h2>Archivos exportados</h2>
{table(["Archivo", "Filas"], file_rows, {1})}
<footer>Generado por <code>export_results.py</code> el {generated}.</footer>
</main></body></html>"""


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

README = """DATAMARCA S.A. - resultados de la ejecución {run_id}

01_resumen_ejecucion.csv     estado, duraciones y conteos de la ejecución
02_tiempos_por_etapa.csv     segundos por paso de ingest y transform
03_calidad_reglas.csv        reglas DQ01-DQ07: detectadas vs inyectadas, recall, precisión
04_reconciliacion.csv        verificaciones de la capa gold
gold_daily_sales.csv         ventas por país, día local, canal y categoría
gold_hourly_sales.csv        ventas por país, día y hora local, canal
gold_store_daily_kpis.csv    tickets, ventas y ticket promedio por tienda y día
gold_warehouse_daily_demand.csv  demanda por CEDI, día y SKU
gold_dq_daily_rejections.csv     líneas rechazadas por regla, canal y día
reporte_datamarca_{run_id}.html  reporte con KPIs, gráficas y tablas

Montos *_usd en dólares; *_local en la moneda del país. CSV en UTF-8, separador "{sep}".
"""


def main() -> None:
    p = argparse.ArgumentParser(description="Export DATAMARCA results to CSV + HTML")
    p.add_argument("--lake", default="lake", help="lake root (local or /Volumes/...)")
    p.add_argument("--tables", default=None,
                   help="<catalog>.<schema> where the pipeline wrote its tables")
    p.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    p.add_argument("--out", default=None, help="default: <lake>/exports/<run_id>")
    p.add_argument("--run-id", default=None, help="pipeline run id (default: latest successful)")
    p.add_argument("--sep", default=",", help='CSV separator (";" for Excel in Spanish)')
    a = p.parse_args()

    lake = a.lake.rstrip("/")
    ingest.configure_tables(a.tables)
    rep = load_reports(lake, a.run_id)
    run_id = rep["pipeline"]["run_id"]
    out = _local(a.out) if a.out else _local(lake) / "exports" / run_id
    print(f"Exporting run {run_id} -> {out}")

    spark = ingest.build_spark(a.format)
    gold = read_gold(spark, lake, a.format)

    # Write on local disk first (zip needs random access, Volumes do not), then copy
    tmp = Path(tempfile.mkdtemp(prefix="datamarca_export_"))
    files: list[tuple[str, int]] = []

    def put(name, header, rows):
        files.append((name, write_csv(tmp / name, header, rows, a.sep)))

    put("01_resumen_ejecucion.csv", ["metrica", "valor"], summary_rows(rep))
    put("02_tiempos_por_etapa.csv", ["etapa", "paso", "segundos"], timing_rows(rep))
    rules = rep["ingest"].get("dq", {}).get("rules", [])
    rcols = ["rule", "name", "detects", "lines", "transactions", "injected",
             "true_positives", "recall", "precision"]
    put("03_calidad_reglas.csv", rcols, [[r.get(c) for c in rcols] for r in rules])
    checks = rep["transform"].get("reconciliation", {}).get("checks", {})
    put("04_reconciliacion.csv", ["verificacion", "ok"], [[c, ok] for c, ok in checks.items()])
    for name in GOLD:
        cols, rows = gold[name]
        put(f"gold_{name}.csv", cols, rows)

    report = f"reporte_datamarca_{run_id}.html"
    files.append((report, -1))
    (tmp / report).write_text(build_html(rep, gold, files), encoding="utf-8")
    (tmp / "LEEME.txt").write_text(README.format(run_id=run_id, sep=a.sep), encoding="utf-8")

    bundle = tmp / f"datamarca_resultados_{run_id}.zip"
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(tmp.iterdir()):
            if f != bundle:
                z.write(f, f.name)

    out.mkdir(parents=True, exist_ok=True)
    for f in tmp.iterdir():
        shutil.copyfile(f, out / f.name)
    shutil.rmtree(tmp, ignore_errors=True)

    print("\nFiles:")
    for name, n in files + [(bundle.name, -1), ("LEEME.txt", -1)]:
        print(f"  {out / name}" + (f"  ({n} rows)" if n >= 0 else ""))


if __name__ == "__main__":
    main()
