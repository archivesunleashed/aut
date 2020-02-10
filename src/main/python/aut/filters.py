from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, explode, udf
from pyspark.sql.types import ArrayType, StringType


def __init__(self, sc, sqlContext, df):
    self.sc = sc
    self.sqlContext = sqlContext
    self.df = df


def keep_valid_pages(df):
    return (
        df.filter("crawl_date is not NULL")
        .filter(
            ~(col("url").rlike(".*robots\\.txt$"))
            & (
                col("mime_type_web_server").rlike("text/html")
                | col("mime_type_web_server").rlike("application/xhtml+xml")
                | col("url").rlike("(?i).*htm$")
                | col("url").rlike("(?i).*html$")
            )
        )
        .filter(col("http_status_code") == 200)
    )


def filter_mime_types(mime_types):
    return df.filter(filtered_mime_type(col("mime_type_web_server")))


filter_mime_types = udf(filter_mime_types.contains(mime_types), ArrayType())


def discard_mime_types(mime_types):
    return df.filter(filtered_mime_type(col("mime_type_web_server")))


discard_mime_types = udf(~discard_mime_types.contains(mime_types), ArrayType())
