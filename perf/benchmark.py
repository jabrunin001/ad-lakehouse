"""Run three representative queries against perf.events_bad and perf.events_optimized
through Trino, capture median wall-clock and bytes scanned, read each table's file
layout from Iceberg metadata, and write docs/performance.md with the comparison.

Run on the host (it shells out to the trino container). The optimized table wins by
partition pruning (user/date queries read only matching files) and by having fewer
files than the bad table's flat pile of tiny unpartitioned files.
"""
import re
import statistics
import subprocess
import time
from pathlib import Path

TABLES = {"bad": "iceberg.perf.events_bad", "optimized": "iceberg.perf.events_optimized"}
RUNS = 5


def _trino(sql: str) -> str:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True, stderr=subprocess.STDOUT,
    )
    # The Trino CLI emits jline "dumb terminal" noise on stderr (folded into stdout
    # above so a real failure still surfaces). Drop those lines before any parsing.
    return "\n".join(
        ln for ln in out.splitlines()
        if "org.jline" not in ln and not ln.startswith("WARNING:")
    )


def _median_wall_ms(sql: str) -> float:
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        _trino(sql)
        times.append((time.perf_counter() - t0) * 1000.0)
    return round(statistics.median(times), 1)


def _bytes_scanned(sql: str) -> int:
    """Best-effort: parse the largest physical-input byte figure from EXPLAIN ANALYZE.
    Verified against Trino 451: the leaf scan prints 'Physical input: 8.45MB' and the
    pruned optimized scan prints 'Physical input: 52.77kB'. The regex also matches the
    'Input: N rows (X.YMB)' figures; max() picks the physical scan size.
    Returns -1 if the format differs on some other Trino version (wall-clock still stands)."""
    out = _trino(f"EXPLAIN ANALYZE {sql}")
    sizes = []
    for m in re.finditer(r"(?:Physical input|Input):[^\n]*?([\d.]+)\s*([kKmMgG]?)B", out):
        val, unit = float(m.group(1)), m.group(2).lower()
        mult = {"": 1, "k": 1e3, "m": 1e6, "g": 1e9}[unit]
        sizes.append(int(val * mult))
    return max(sizes) if sizes else -1


def _layout(table: str) -> dict:
    # Iceberg metadata table must be referenced with the full schema prefix:
    # iceberg.perf."events_bad$files" (a bare "events_bad$files" fails — no session schema).
    schema_prefix = ".".join(table.split(".")[:-1])  # 'iceberg.perf'
    name = table.split(".")[-1]                        # 'events_bad'
    out = _trino(
        f'SELECT count(*), sum(file_size_in_bytes), avg(file_size_in_bytes) '
        f'FROM {schema_prefix}."{name}$files"'
    ).strip().strip('"').split('","')
    files, total, avg = (out + ["0", "0", "0"])[:3]
    return {"files": int(files), "total_bytes": int(float(total)),
            "avg_file_bytes": int(float(avg))}


def queries(table: str) -> dict:
    uid = _trino(
        f"SELECT user_id FROM {table} GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    ).strip().strip('"').replace("'", "''")
    day = _trino(f"SELECT CAST(min(event_ts) AS DATE) FROM {table}").strip().strip('"')
    return {
        "user-filtered scan": f"SELECT count(*) FROM {table} WHERE user_id = '{uid}'",
        "date-range scan": (
            f"SELECT count(*) FROM {table} "
            f"WHERE event_ts >= TIMESTAMP '{day} 00:00:00' "
            f"AND event_ts < TIMESTAMP '{day} 00:00:00' + INTERVAL '2' DAY"
        ),
        "campaign rollup": (
            f"SELECT campaign_id, count(*) FROM {table} "
            f"WHERE event_type = 'impression' GROUP BY campaign_id"
        ),
    }


def run() -> dict:
    results = {}
    layouts = {name: _layout(t) for name, t in TABLES.items()}
    table_queries = {name: queries(t) for name, t in TABLES.items()}  # resolve uid/day once
    for qname in table_queries["bad"]:
        results[qname] = {}
        for name in TABLES:
            sql = table_queries[name][qname]
            results[qname][name] = {
                "wall_ms": _median_wall_ms(sql),
                "bytes_scanned": _bytes_scanned(sql),
            }
            print(f"[bench] {qname:20s} {name:10s} "
                  f"{results[qname][name]['wall_ms']:8.1f} ms  "
                  f"scanned={results[qname][name]['bytes_scanned']}")
    _write_report(layouts, results)
    return {"layouts": layouts, "results": results}


def _mb(n: int) -> str:
    """Human-readable byte count. Small pruned scans (bytes/kB) would round to
    '0.00 MB' and hide the win, so scale the unit to the magnitude."""
    if n < 0:
        return "n/a"
    if n < 1e3:
        return f"{n} B"
    if n < 1e6:
        return f"{n/1e3:.2f} kB"
    return f"{n/1e6:.2f} MB"


def _write_report(layouts: dict, results: dict) -> None:
    lines = ["# Performance: deliberately-bad vs optimized layout", ""]
    lines += [
        "Same ~312k-row dataset, two Iceberg layouts. `events_bad` is unpartitioned with",
        "many tiny files, so every query scans the whole pile. `events_optimized` is",
        "hidden-partitioned by `days(event_ts)` and `bucket(16, user_id)`, so user- and",
        "date-filtered queries prune to only the matching files. Laptop-scale data, so the",
        "absolute times are small; the ratios and the data-scanned reduction are the point.",
        "", "## Table layout", "",
        "| table | files | avg file size | total |",
        "|---|--:|--:|--:|",
    ]
    for name in ("bad", "optimized"):
        lo = layouts[name]
        lines.append(f"| events_{name} | {lo['files']} | {_mb(lo['avg_file_bytes'])} "
                     f"| {_mb(lo['total_bytes'])} |")
    lines += ["", "At this data scale the optimized table's files are actually *smaller* on",
              "average (it spreads the same rows across day + user-bucket partitions). The",
              "win is not bigger/compacted files — it is fewer files overall and, decisively,",
              "a layout that lets queries skip the irrelevant ones (see scanned bytes below).",
              "", "## Query before/after", "",
              "| query | bad (ms) | optimized (ms) | speedup | bad scanned | optimized scanned |",
              "|---|--:|--:|--:|--:|--:|"]
    for qname, r in results.items():
        b, o = r["bad"], r["optimized"]
        speed = f"{b['wall_ms']/o['wall_ms']:.1f}x" if o["wall_ms"] else "n/a"
        lines.append(f"| {qname} | {b['wall_ms']} | {o['wall_ms']} | {speed} "
                     f"| {_mb(b['bytes_scanned'])} | {_mb(o['bytes_scanned'])} |")
    lines += ["", "Wall-clock is the median of 5 runs through the Trino CLI (the fixed",
              "docker-exec overhead is the same for both tables, so the comparison holds).",
              "Bytes scanned is the physical input reported by `EXPLAIN ANALYZE`; the",
              "optimized table reads far less on the user- and date-filtered queries because",
              "partition pruning skips the non-matching files. The campaign rollup is a full",
              "aggregate over all rows, so it reads everything either way — its only edge is",
              "the optimized table's smaller file count, not pruning.",
              ""]
    out_path = Path(__file__).resolve().parent.parent / "docs" / "performance.md"
    out_path.write_text("\n".join(lines) + "\n")
    print("[bench] wrote docs/performance.md")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
