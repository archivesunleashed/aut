from pyspark.sql import DataFrame


class WebArchive:
    def __init__(self, sc, sqlContext, path):
        self.sc = sc
        self.sqlContext = sqlContext
        self.loader = sc._jvm.io.archivesunleashed.DataFrameLoader(sc._jsc.sc())
        self.path = path

    def pages(self):
        return DataFrame(self.loader.extractValidPages(self.path), self.sqlContext)

    def links(self):
        return DataFrame(self.loader.extractHyperlinks(self.path), self.sqlContext)
