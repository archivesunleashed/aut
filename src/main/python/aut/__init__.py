from aut.app import ExtractPopularImages, SaveBytes, WriteGEXF, WriteGraphML

__all__ = [
    "ExtractPopularImages",
    "SaveBytes",
    "WriteGEXF",
    "WriteGraphML",
]

# common.py and udfs.py need pyspark for everything they do (unlike app.py,
# which is mostly pure Python), so importing them without pyspark installed
# is never going to work either way -- but letting that failure happen here
# means `import aut` (and anything downstream of it, like `from aut.app
# import WriteGEXF`) fails too, even for the parts that don't need Spark.
try:
    from aut.common import WebArchive
    from aut.udfs import (
        compute_image_size,
        compute_md5,
        compute_sha1,
        detect_language,
        detect_mime_type_tika,
        extract_boilerplate,
        extract_date,
        extract_domain,
        extract_image_links,
        extract_links,
        get_extension_mime,
        has_content,
        has_date,
        has_domains,
        has_http_status,
        has_images,
        has_languages,
        has_mime_types,
        has_mime_types_tika,
        has_url_patterns,
        has_urls,
        remove_html,
        remove_http_header,
        remove_prefix_www,
    )
except ImportError:
    pass
else:
    __all__ += [
        "WebArchive",
        "compute_image_size",
        "compute_md5",
        "compute_sha1",
        "detect_language",
        "detect_mime_type_tika",
        "extract_boilerplate",
        "extract_date",
        "extract_domain",
        "extract_image_links",
        "extract_links",
        "get_extension_mime",
        "has_content",
        "has_date",
        "has_domains",
        "has_http_status",
        "has_images",
        "has_languages",
        "has_mime_types",
        "has_mime_types_tika",
        "has_url_patterns",
        "has_urls",
        "remove_http_header",
        "remove_html",
        "remove_prefix_www",
    ]
