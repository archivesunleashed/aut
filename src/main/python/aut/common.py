from pyspark.sql import DataFrame


class WebArchive:
    def __init__(self, sc, sqlContext, path):
        self.sc = sc
        self.sqlContext = sqlContext
        self.loader = sc._jvm.io.archivesunleashed.df.DataFrameLoader(sc._jsc.sc())
        self.path = path

    def all(self):
        return DataFrame(self.loader.all(self.path), self.sqlContext)

    def audio(self):
        return DataFrame(self.loader.audio(self.path), self.sqlContext)

    def css(self):
        return DataFrame(self.loader.css(self.path), self.sqlContext)

    def html(self):
        return DataFrame(self.loader.html(self.path), self.sqlContext)

    def imagegraph(self):
        return DataFrame(self.loader.imagegraph(self.path), self.sqlContext)

    def images(self):
        return DataFrame(self.loader.images(self.path), self.sqlContext)

    def js(self):
        return DataFrame(self.loader.js(self.path), self.sqlContext)

    def json(self):
        return DataFrame(self.loader.json(self.path), self.sqlContext)

    def pdfs(self):
        return DataFrame(self.loader.pdfs(self.path), self.sqlContext)

    def plain_text(self):
        return DataFrame(self.loader.plainText(self.path), self.sqlContext)

    def presentation_program(self):
        return DataFrame(
            self.loader.presentationProgramFiles(self.path), self.sqlContext
        )

    def spreadsheets(self):
        return DataFrame(self.loader.spreadsheets(self.path), self.sqlContext)

    def video(self):
        return DataFrame(self.loader.videos(self.path), self.sqlContext)

    def webgraph(self):
        return DataFrame(self.loader.webgraph(self.path), self.sqlContext)

    def webpages(self):
        return DataFrame(self.loader.webpages(self.path), self.sqlContext)

    def word_processor(self):
        return DataFrame(self.loader.wordProcessorFiles(self.path), self.sqlContext)

    def xml(self):
        return DataFrame(self.loader.xml(self.path), self.sqlContext)
