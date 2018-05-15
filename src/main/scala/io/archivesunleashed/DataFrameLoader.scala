package io.archivesunleashed

import org.apache.spark.SparkContext
import org.apache.spark.sql._

class DataFrameLoader(sc: SparkContext) {
  def extractValidPages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractValidPagesDF()
  }

  def extractHyperlinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractHyperlinksDF()
  }

  def extractImageLinks(path: String): DataFrame = {
  	RecordLoader.loadArchives(path, sc)
  		.extractImageLinksDF()
  }
}
