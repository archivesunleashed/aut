from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def extract_domain_func(url):
    url = url.replace('http://', '').replace('https://', '')
    if '/' in url:
        return url.split('/')[0].replace('www.', '')
    else:
        return url.replace('www.', '')

extract_domain = udf(extract_domain_func, StringType())

class web_archive:
    def __init__(self, sc, sqlContext, path):
        self.sc = sc
        self.sqlContext = sqlContext
        self.loader = sc._jvm.io.archivesunleashed.DataFrameLoader(sc._jsc.sc())
        self.path = path

    def pages(self):
        return DataFrame(self.loader.extractValidPages(self.path), self.sqlContext)

    def links(self):
        return DataFrame(self.loader.extractHyperlinks(self.path), self.sqlContext)

