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

    def images(self):
        return DataFrame(self.loader.extractImages(self.path), self.sqlContext)

    def image_links(self):
        return DataFrame(self.loader.extractImageLinks(self.path), self.sqlContext)

    def pdfs(self):
        return DataFrame(self.loader.extractPDFs(self.path), self.sqlContext)

    def audio(self):
        return DataFrame(self.loader.extractAudio(self.path), self.sqlContext)

    def video(self):
        return DataFrame(self.loader.extractVideo(self.path), self.sqlContext)

    def spreadsheets(self):
        return DataFrame(self.loader.extractSpreadsheets(self.path), self.sqlContext)

    def presentation_program(self):
        return DataFrame(self.loader.extractPresentationProgram(self.path), self.sqlContext)

    def word_processor(self):
        return DataFrame(self.loader.extractWordProcessor(self.path), self.sqlContext)

    def text_files(self):
        return DataFrame(self.loader.extractTextFiles(self.path), self.sqlContext)
