# Wiring smoke tests: these exist to catch a mistyped Scala method name or
# wrong Py4J call shape, not to re-test matchbox's own logic (that's
# thoroughly covered by the Scala test suite already).
#
# aut.udfs is imported inside each test, not at module level, so collecting
# this test module doesn't require pyspark to be importable -- only running
# it does (by which point the `spark` fixture has already skipped if it's
# missing).


def test_extract_domain_wiring(spark):
    from aut.udfs import extract_domain

    df = spark.createDataFrame([("http://www.example.com/page",)], ["url"])
    result = df.select(extract_domain(df.url).alias("domain")).collect()
    assert result[0]["domain"] == "example.com"


def test_remove_html_wiring(spark):
    from aut.udfs import remove_html

    df = spark.createDataFrame([("<p>Hello</p>",)], ["content"])
    result = df.select(remove_html(df.content).alias("text")).collect()
    assert result[0]["text"] == "Hello"


def test_remove_http_header_wiring(spark):
    from aut.udfs import remove_http_header

    header = "HTTP/1.0 200 OK\r\n\r\n"
    df = spark.createDataFrame([(header + "body text",)], ["content"])
    result = df.select(remove_http_header(df.content).alias("body")).collect()
    assert result[0]["body"] == "body text"
