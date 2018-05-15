package io.archivesunleashed.app

import io.archivesunleashed._
import io.archivesunleashed.app.DomainFrequencyExtractor.{Conf, applyAndSave, log}
import io.archivesunleashed.matchbox.{ExtractDomain, ExtractLinks}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DomainGraphExtractor {
  def apply(records: RDD[ArchiveRecord]) = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).replaceAll("^\\\\s*www\\\\.", ""), ExtractDomain(f._2).replaceAll("^\\\\s*www\\\\.", ""))))
      .filter(r => r._2 != "" && r._3 != "")
      .countItems()
      .filter(r => r._2 > 5)
  }

  def main(argv: Array[String]): Unit = {
    var args = new Conf(argv)

    log.info("Domain graph extractor")
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Domain graph extractor")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    RecordLoader.loadArchives(args.input(), sc).saveAsTextFile(args.output())
  }
}
