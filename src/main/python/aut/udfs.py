from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col


class Udf:
    def compute_image_size(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.computeImageSize()
            .apply
        )
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def compute_md5(col):
        sc = SparkContext.getOrCreate()
        udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.computeMD5().apply
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def compute_sha1(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.computeSHA1().apply
        )
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def detect_language(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.detectLanguage()
            .apply
        )
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
        udf = (
            sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.extractDate().apply
        )
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def extract_domain(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.extractDomain()
            .apply
        )
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
        udf = (
            sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.extractLinks().apply
        )
        return Column(udf(_to_seq(sc, [col, links], _to_java_column)))

    def get_extension_mime(col, mime):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.getExtensionMime()
            .apply
        )
        return Column(udf(_to_seq(sc, [col, mime], _to_java_column)))

    def remove_http_header(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.removeHTTPHeader()
            .apply
        )
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def remove_html(col):
        sc = SparkContext.getOrCreate()
        udf = sc.getOrCreate()._jvm.io.archivesunleashed.udfs.package.removeHTML().apply
        return Column(udf(_to_seq(sc, [col], _to_java_column)))

    def remove_prefix_www(col):
        sc = SparkContext.getOrCreate()
        udf = (
            sc.getOrCreate()
            ._jvm.io.archivesunleashed.udfs.package.removePrefixWWW()
            .apply
        )
        return Column(udf(_to_seq(sc, [col], _to_java_column)))
