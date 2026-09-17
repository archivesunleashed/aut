import os

# One (method name, at least one expected column) pair per WebArchive method,
# to catch Py4J wiring mistakes (typo'd method name, wrong argument shape)
# that Scala-side tests can't see since they never call through the bridge.
METHOD_EXPECTED_COLUMNS = [
    ("all", "crawl_date"),
    ("audio", "sha1"),
    ("css", "content"),
    ("html", "content"),
    ("imagegraph", "image_url"),
    ("images", "width"),
    ("js", "content"),
    ("json", "content"),
    ("pdfs", "sha1"),
    ("plain_text", "content"),
    ("presentation_program", "sha1"),
    ("spreadsheets", "sha1"),
    ("video", "sha1"),
    ("webgraph", "anchor"),
    ("webpages", "language"),
    ("word_processor", "sha1"),
    ("xml", "content"),
]


def test_web_archive_methods_are_wired_to_scala(spark, resources_dir):
    # Imported here, not at module level, so collecting this test module
    # doesn't require pyspark to be importable -- only running it does (by
    # which point the `spark` fixture has already skipped if it's missing).
    from aut.common import WebArchive

    archive_path = os.path.join(resources_dir, "arc", "example.arc.gz")
    archive = WebArchive(spark.sparkContext, spark, archive_path)

    for method_name, expected_column in METHOD_EXPECTED_COLUMNS:
        df = getattr(archive, method_name)()
        assert expected_column in df.columns, (
            f"WebArchive.{method_name}() is missing expected column "
            f"'{expected_column}' -- got {df.columns}"
        )
