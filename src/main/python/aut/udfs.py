from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def extract_domain_func(url):
    url = url.replace("http://", "").replace("https://", "")
    if "/" in url:
        return url.split("/")[0].replace("www.", "")
    else:
        return url.replace("www.", "")


extract_domain = udf(extract_domain_func, StringType())
