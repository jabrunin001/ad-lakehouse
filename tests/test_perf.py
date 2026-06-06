# tests/test_perf.py
import re
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    # filter the jline 'dumb terminal' warning the Trino CLI prints to stderr/stdout
    line = [ln for ln in out.splitlines() if not ln.startswith(("WARNING", "org.jline"))]
    return "\n".join(line).strip().strip('"').split('","')


def _scanned_bytes(table: str, uid: str) -> int:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute",
         f"EXPLAIN ANALYZE SELECT count(*) FROM {table} WHERE user_id = '{uid}'"],
        text=True,
    )
    sizes = []
    for m in re.finditer(r"(?:Physical input|Input):[^\n]*?([\d.]+)\s*([kKmMgG]?)B", out):
        mult = {"": 1, "k": 1e3, "m": 1e6, "g": 1e9}[m.group(2).lower()]
        sizes.append(int(float(m.group(1)) * mult))
    return max(sizes) if sizes else -1


@pytest.mark.integration
def test_same_data_both_tables():
    bad, opt = _trino(
        "SELECT (SELECT count(*) FROM iceberg.perf.events_bad), "
        "(SELECT count(*) FROM iceberg.perf.events_optimized)"
    )
    assert int(bad) == int(opt) and int(bad) > 0


@pytest.mark.integration
def test_optimized_has_fewer_prunable_files():
    (bad_files,) = _trino("SELECT count(*) FROM iceberg.perf.\"events_bad$files\"")
    (opt_files,) = _trino("SELECT count(*) FROM iceberg.perf.\"events_optimized$files\"")
    assert int(opt_files) < int(bad_files)


@pytest.mark.integration
def test_pruning_scans_less_on_a_user_query():
    (uid,) = _trino(
        "SELECT user_id FROM iceberg.perf.events_optimized GROUP BY user_id "
        "ORDER BY count(*) DESC LIMIT 1"
    )
    bad = _scanned_bytes("iceberg.perf.events_bad", uid)
    opt = _scanned_bytes("iceberg.perf.events_optimized", uid)
    # the bucketed table prunes the user predicate to one bucket -> scans far less
    assert bad > 0 and opt > 0 and opt < bad
