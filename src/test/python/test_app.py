import base64
import hashlib
import os

from aut.app import ExtractPopularImages, SaveBytes, WriteGEXF, WriteGraphML

# (crawl_date, src, dest, weight) -- matches the format WriteGEXF/WriteGraphML
# expect, documented on their Scala counterparts (WriteGEXF.scala,
# WriteGraphML.scala) as Array[Row] of (crawl_date, src_domain, dest_domain,
# count). Two distinct weights so a test that mixes them up (e.g. by reusing
# a stale loop variable) can't pass by accident.
SAMPLE_EDGES = [
    ("20200101000000", "example.com", "example.org", 3),
    ("20200101000100", "example.org", "example.net", 5),
]


def test_write_gexf_edge_weight_matches_its_own_row(tmp_path):
    out_file = tmp_path / "out.gexf"
    WriteGEXF(SAMPLE_EDGES, str(out_file))
    content = out_file.read_text()

    for _, _, _, weight in SAMPLE_EDGES:
        assert f'weight="{weight}"' in content


def test_write_graphml_edge_weight_and_date_match_their_own_row(tmp_path):
    # Regression test: WriteGraphML used to write str(row[3])/str(row[0])
    # for every edge's weight/crawlDate, where `row` was left over from an
    # earlier loop over the same data used to collect nodes -- so every
    # edge silently got the same (wrong) weight and date.
    out_file = tmp_path / "out.graphml"
    WriteGraphML(SAMPLE_EDGES, str(out_file))
    content = out_file.read_text()

    for crawl_date, _, _, weight in SAMPLE_EDGES:
        assert f'<data key="weight">{weight}</data>' in content
        assert f'<data key="crawlDate">{crawl_date}</data>' in content


def test_save_bytes_writes_file_named_by_md5_and_extension(tmp_path):
    raw = b"hello world"
    encoded = base64.b64encode(raw).decode("ascii")

    SaveBytes([("txt", encoded)], str(tmp_path))

    expected_path = tmp_path / (hashlib.md5(raw).hexdigest() + ".txt")
    assert expected_path.exists()
    assert expected_path.read_bytes() == raw


def test_extract_popular_images_applies_min_size_filter(spark, resources_dir):
    # Regression test: ExtractPopularImages accepted min_width/min_height
    # but never applied them as a filter, so results were identical
    # regardless of what was passed in.
    from aut.common import WebArchive

    archive_path = os.path.join(resources_dir, "arc", "example.arc.gz")
    images_df = WebArchive(spark.sparkContext, spark, archive_path).images()

    unfiltered_count = ExtractPopularImages(images_df, 100, 0, 0).count()
    filtered_count = ExtractPopularImages(images_df, 100, 30, 30).count()

    assert filtered_count < unfiltered_count
