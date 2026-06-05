# tests/test_dags.py
import importlib.util
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("*_dag.py"))


@pytest.fixture(autouse=True)
def _dags_on_path():
    # DAG modules import their sibling `_spark` helper; Airflow puts the dags dir
    # on sys.path at runtime, so replicate that for the test.
    sys.path.insert(0, str(DAGS_DIR))
    yield
    sys.path.remove(str(DAGS_DIR))


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.stem)
def test_dag_file_imports_and_defines_a_dag(dag_file):
    pytest.importorskip("airflow")  # only runs where the airflow extra is installed
    from airflow.models import DAG

    spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dags = [v for v in vars(module).values() if isinstance(v, DAG)]
    assert len(dags) == 1, f"{dag_file.name} should define exactly one DAG"
    assert dags[0].dag_id  # non-empty id


def test_three_dag_files_present():
    assert {p.stem for p in DAG_FILES} == {
        "campaign_pull_dag", "medallion_dag", "maintenance_dag",
    }


def test_medallion_builds_silver_before_gold():
    # The dependency direction is the one piece of real logic in the DAG layer;
    # a typo could silently invert it, so assert it explicitly.
    pytest.importorskip("airflow")
    from airflow.models import DAG

    path = DAGS_DIR / "medallion_dag.py"
    spec = importlib.util.spec_from_file_location("medallion_dag", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = next(v for v in vars(module).values() if isinstance(v, DAG))
    assert "build_gold" in dag.get_task("build_silver").downstream_task_ids
