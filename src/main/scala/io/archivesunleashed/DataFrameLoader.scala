package io.archivesunleashed

import org.apache.spark.SparkContext
// scalastyle:off underscore.import
import org.apache.spark.sql._
// scalastyle:on underscore.import

class DataFrameLoader(sc: SparkContext) {
  def extractValidPages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractValidPagesDF()
  }

  def extractHyperlinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractHyperlinksDF()
  }

  /* Create a dataframe with (source page, image url) pairs */
  def extractImageLinks(path: String): DataFrame = {
  	RecordLoader.loadArchives(path, sc)
  		.extractImageLinksDF()
  }

  /** Create a dataframe with (image url, type, width, height, md5, raw bytes) pairs */
  def extractImages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractImageDetailsDF()
  }
}
