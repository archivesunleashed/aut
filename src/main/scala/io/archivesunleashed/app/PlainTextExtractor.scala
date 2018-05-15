package io.archivesunleashed.app

import io.archivesunleashed.app.DomainFrequencyExtractor.{Conf, applyAndSave, log}
import io.archivesunleashed.{ArchiveRecord, RecordLoader}
import io.archivesunleashed.matchbox.RemoveHTML
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PlainTextExtractor {
  def apply(records: RDD[ArchiveRecord]) = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, r.getDomain, r.getUrl, RemoveHTML(r.getContentString)))
  }

  def main(argv: Array[String]): Unit = {
    var args = new Conf(argv)

    log.info("Plain text extractor")
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Plain text extractor")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    RecordLoader.loadArchives(args.input(), sc).saveAsTextFile(args.output())
  }
}
