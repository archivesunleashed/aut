from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def extract_domain(url):
    url = url.replace("http://", "").replace("https://", "")
    if "/" in url:
        return url.split("/")[0].replace("www.", "")
    else:
        return url.replace("www.", "")


extract_domain = udf(extract_domain, StringType())


def remove_prefix_www(url):
    url = url.replace("www.", "")
    return url


remove_prefix_www = udf(remove_prefix_www, StringType())


def remove_http_header(content):
    header_end = "\r\n\r\n"

    if content.startswith("HTTP/"):
        header_length = content.rindex(header_end) + len(header_end)
        return content[header_length:]
    else:
        return content


remove_http_header = udf(remove_http_header, StringType())
