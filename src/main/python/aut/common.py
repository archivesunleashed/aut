from pyspark.sql import DataFrame


class WebArchive:
    def __init__(self, sc, sqlContext, path):
        self.sc = sc
        self.sqlContext = sqlContext
        self.loader = sc._jvm.io.archivesunleashed.DataFrameLoader(sc._jsc.sc())
        self.path = path

    def all(self):
        return DataFrame(self.loader.all(self.path), self.sqlContext)

    def webpages(self):
        return DataFrame(self.loader.webpages(self.path), self.sqlContext)

    def webgraph(self):
        return DataFrame(self.loader.webgraph(self.path), self.sqlContext)

    def images(self):
        return DataFrame(self.loader.images(self.path), self.sqlContext)

    def image_links(self):
        return DataFrame(self.loader.imageLinks(self.path), self.sqlContext)

    def pdfs(self):
        return DataFrame(self.loader.pdfs(self.path), self.sqlContext)

    def audio(self):
        return DataFrame(self.loader.audio(self.path), self.sqlContext)

    def video(self):
        return DataFrame(self.loader.videos(self.path), self.sqlContext)

    def spreadsheets(self):
        return DataFrame(self.loader.spreadsheets(self.path), self.sqlContext)

    def presentation_program(self):
        return DataFrame(
            self.loader.presentationProgramFiles(self.path), self.sqlContext
        )

    def word_processor(self):
        return DataFrame(self.loader.wordProcessorFiles(self.path), self.sqlContext)

    def text_files(self):
        return DataFrame(self.loader.textFiles(self.path), self.sqlContext)
