# Python tests

Tests for `src/main/python/aut/`. Two kinds live side by side here:

- Pure-Python tests (`SaveBytes`, `WriteGEXF`, `WriteGraphML`) need nothing
  beyond `pytest` and always run.
- Spark-dependent tests (`ExtractPopularImages`, `WebArchive`, the `udfs.py`
  wrappers) need `pyspark` importable and the built fat jar. They skip
  themselves automatically when either is missing, so it's always safe to
  just run the whole suite.

## Running just the pure-Python tests

```sh
pip install -r src/test/python/requirements-test.txt
python -m pytest src/test/python
```

## Running the full suite, including Spark-dependent tests

You need `mvn package` (or `install`/`verify`) run at least once so
`target/aut-*-fatjar.jar` exists, and `pyspark` importable -- either
`pip install pyspark` (matching this project's Spark version), or point
`SPARK_HOME` at a real Spark install and let `conftest.py` use its bundled
copy:

```sh
mvn package -DskipTests
SPARK_HOME=/path/to/spark-3.5.9-bin-hadoop3 python -m pytest src/test/python -v
```
