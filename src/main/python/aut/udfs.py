from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col


def compute_image_size(col):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.computeImageSize().apply
    )
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def compute_md5(col):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.computeMD5().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def compute_sha1(col):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.computeSHA1().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def detect_language(col):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.detectLanguage().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def detect_mime_type_tika(col):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()
        ._jvm.io.archivesunleashed.udfs.package.detectMimeTypeTika()
        .apply
    )
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def extract_boilerplate(col):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()
        ._jvm.io.archivesunleashed.udfs.package.extractBoilerpipeText()
        .apply
    )
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def extract_date(col, dates):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.extractDate().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def extract_domain(col):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.extractDomain().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def extract_image_links(col, image_links):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()
        ._jvm.io.archivesunleashed.udfs.package.extractImageLinks()
        .apply
    )
    return Column(udf(_to_seq(sc, [col, image_links], _to_java_column)))


def extract_links(col, links):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.extractLinks().apply
    return Column(udf(_to_seq(sc, [col, links], _to_java_column)))


def get_extension_mime(col, mime):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.getExtensionMime().apply
    )
    return Column(udf(_to_seq(sc, [col, mime], _to_java_column)))


def remove_http_header(col):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.removeHTTPHeader().apply
    )
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def remove_html(col):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.removeHTML().apply
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


def remove_prefix_www(col):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.removePrefixWWW().apply
    )
    return Column(udf(_to_seq(sc, [col], _to_java_column)))


# Filters


def has_content(col, content_res):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasContent().apply
    return Column(udf(_to_seq(sc, [col, content_res], _to_java_column)))


def has_date(col, dates):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasDate().apply
    return Column(udf(_to_seq(sc, [col, dates], _to_java_column)))


def has_domains(col, domains):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasDomains().apply
    return Column(udf(_to_seq(sc, [col, domains], _to_java_column)))


def has_http_status(col, status_codes):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasHTTPStatus().apply
    return Column(udf(_to_seq(sc, [col, status_codes], _to_java_column)))


def has_images(col, mime_type):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasImages().apply
    return Column(udf(_to_seq(sc, [col, mime_type], _to_java_column)))


def has_languages(col, languages):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasLanguages().apply
    return Column(udf(_to_seq(sc, [col, languages], _to_java_column)))


def has_mime_types(col, mime_types):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasMIMETypes().apply
    return Column(udf(_to_seq(sc, [col, mime_types], _to_java_column)))


def has_mime_types_tika(col, mime_types_tika):
    sc = SparkContext.getOrCreate()
    udf = (
        sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasMIMETypesTika().apply
    )
    return Column(udf(_to_seq(sc, [col, mime_types_tika], _to_java_column)))


def has_url_patterns(col, url_res):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasUrlPatterns().apply
    return Column(udf(_to_seq(sc, [col, url_res], _to_java_column)))


def has_urls(col, urls):
    sc = SparkContext.getOrCreate()
    udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.hasUrls().apply
    return Column(udf(_to_seq(sc, [col, urls], _to_java_column)))
