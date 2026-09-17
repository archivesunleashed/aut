import glob
import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
PYTHON_SRC = os.path.join(REPO_ROOT, "src", "main", "python")
RESOURCES_DIR = os.path.join(REPO_ROOT, "src", "test", "resources")

if PYTHON_SRC not in sys.path:
    sys.path.insert(0, PYTHON_SRC)

# Let `import pyspark` succeed using a real Spark distribution's bundled
# copy (set SPARK_HOME to point at one, e.g.
# /home/nruest/bin/spark-3.5.9-bin-hadoop3) instead of requiring a
# separate `pip install pyspark`, which has to match the project's Spark
# version exactly to be safe to test against.
spark_home = os.environ.get("SPARK_HOME")
if spark_home:
    sys.path.insert(0, os.path.join(spark_home, "python"))
    py4j_zips = glob.glob(os.path.join(spark_home, "python", "lib", "py4j-*.zip"))
    if py4j_zips:
        sys.path.insert(0, py4j_zips[0])


def _find_fat_jar():
    target_dir = os.path.join(REPO_ROOT, "target")
    matches = glob.glob(os.path.join(target_dir, "aut-*-fatjar.jar"))
    return matches[0] if matches else None


@pytest.fixture(scope="session")
def resources_dir():
    return RESOURCES_DIR


@pytest.fixture(scope="session")
def fat_jar():
    jar = _find_fat_jar()
    if jar is None:
        pytest.skip(
            "aut fat jar not found in target/ -- run `mvn package` (or `install`/"
            "`verify`) first"
        )
    return jar


@pytest.fixture(scope="session")
def spark(fat_jar):
    pyspark = pytest.importorskip(
        "pyspark",
        reason=(
            "pyspark is not importable -- either `pip install pyspark` (matching "
            "the project's Spark version) or set SPARK_HOME to a real Spark "
            "distribution before running pytest"
        ),
    )
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("aut-python-tests")
        .config("spark.jars", fat_jar)
        .config("spark.driver.allowMultipleContexts", "true")
        .getOrCreate()
    )
    yield session
    session.stop()
