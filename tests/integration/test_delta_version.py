"""Delta Lake version / ABI gate (PR #13 / T-1.11, I-02).

    I-02  Delta 4.3.1 on Spark 4.1.0 / Java 21: catalog-managed Delta
          (delta.feature.catalogManaged) write + append + read + time-travel
          through the Unity Catalog Spark connector, with no ABI break
          (NoSuchMethodError / AbstractMethodError). Also pins the version:
          4.3.0 NPEs in the connector (AbstractDeltaCatalogClient reads a null
          catalog-options map) and 4.2.0 cannot do catalog-managed Delta — both
          are exercised here, which is what makes the T4 negative gate real
          (a path-based `spark_catalog` write passes on ANY Delta 4.x).

Runs inside spark-master-41 (which mounts the pinned jars and spark-defaults),
so it exercises the exact classpath the stack ships. Skips when the container
is not running.
"""

from __future__ import annotations

import json
import subprocess
import textwrap
import urllib.request
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MASTER = "spark-master-41"
UC_API = "http://localhost:8081/api/2.1/unity-catalog"

pytestmark = [pytest.mark.integration]


def _master_running() -> bool:
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.split()
        return MASTER in out
    except Exception:
        return False


def _spark_submit(script: str, tmp_path: Path) -> str:
    """docker cp a helper into spark-master and run it via spark-submit."""
    f = tmp_path / "helper.py"
    f.write_text(textwrap.dedent(script))
    subprocess.run(
        ["docker", "cp", str(f), f"{MASTER}:/tmp/_ivtest.py"],
        check=True,
        capture_output=True,
    )
    r = subprocess.run(
        ["docker", "exec", MASTER, "/opt/spark/bin/spark-submit", "/tmp/_ivtest.py"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    # Return BOTH streams: spark-submit writes the driver's print() to stdout but
    # every error (stack trace, ABI NoSuchMethodError, S3A/connectivity failure)
    # to stderr. Capturing stdout only made a failed run look like empty output,
    # which is undiagnosable and lets the negative gate "pass" for the wrong
    # reason. The I02_PASS / error-signature assertions scan the combined text.
    return f"{r.stdout}\n--- stderr ---\n{r.stderr}"


@pytest.fixture(autouse=True)
def _require_master():
    if not _master_running():
        pytest.skip(f"{MASTER} not running")


def test_i02_delta_431_pinned_in_config():
    conf = (REPO_ROOT / "config" / "spark" / "spark-defaults.conf.example").read_text()
    assert "delta-spark_2.13-4.3.1.jar" in conf, "stack must pin Delta 4.3.1"
    assert "delta-spark_2.13-4.2.0.jar" not in conf
    assert "delta-spark_2.13-4.3.0.jar" not in conf


def _uc_post(path: str, body: dict) -> None:
    """Best-effort UC REST create (409/already-exists is fine)."""
    req = urllib.request.Request(
        f"{UC_API}/{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=8)
    except Exception:
        pass


def test_i02_delta_write_read_timetravel(tmp_path):
    # Catalog-managed Delta (no LOCATION) through the UC connector is the path
    # the pin rationale is about: 4.3.1 works, 4.3.0 NPEs in the connector,
    # 4.2.0 cannot do catalog-managed Delta. A catalog with a storage_root is the
    # prerequisite (mirrors tests/integration/sdp/test_sdp_table_formats.py I-45).
    sfx = uuid.uuid4().hex[:8]
    cat, sch, tbl = "managed_demo", f"i02_{sfx}", "t"
    _uc_post(
        "catalogs",
        {"name": cat, "storage_root": "s3://lakehouse/warehouse/managed"},
    )
    _uc_post("schemas", {"name": sch, "catalog_name": cat})

    out = _spark_submit(
        f"""
        from pyspark.sql import SparkSession
        s = (SparkSession.builder.appName("i02").master("local[2]")
             .config("spark.ui.enabled","false")
             .config("spark.sql.catalog.{cat}","io.unitycatalog.spark.UCSingleCatalog")
             .config("spark.sql.catalog.{cat}.uri","http://unity-catalog:8080")
             .config("spark.sql.catalog.{cat}.token","not_used").getOrCreate())
        s.sparkContext.setLogLevel("ERROR")
        t = "{cat}.{sch}.{tbl}"
        # No LOCATION — the catalog assigns it (catalog-managed Delta). Both
        # writeStats* props are required for catalog-managed tables.
        s.sql(f"CREATE TABLE IF NOT EXISTS {{t}} (id BIGINT) USING delta "
              "TBLPROPERTIES ('delta.feature.catalogManaged'='supported',"
              "'delta.checkpoint.writeStatsAsJson'='true',"
              "'delta.checkpoint.writeStatsAsStruct'='true')")           # v0
        s.sql(f"INSERT INTO {{t}} VALUES (0),(1),(2),(3),(4)")
        s.sql(f"INSERT INTO {{t}} VALUES (5),(6),(7),(8),(9)")            # v1
        latest = s.sql(f"SELECT count(*) FROM {{t}}").collect()[0][0]
        v0 = s.sql(f"SELECT count(*) FROM {{t}} VERSION AS OF 0").collect()[0][0]
        print(f"I02 latest={{latest}} v0={{v0}}")
        assert latest==10 and v0==5, "time-travel/counts wrong"
        print("I02_PASS")
        s.stop()
        """,
        tmp_path,
    )
    assert "I02_PASS" in out, f"Delta write/read/time-travel failed:\n{out[-2000:]}"
    assert "NoSuchMethodError" not in out and "AbstractMethodError" not in out
