package io.archivesunleashed.app

import java.nio.file.Path

import io.archivesunleashed._
import io.archivesunleashed.matchbox.ExtractDomain
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

object DomainFrequencyExtractor {
  val log = Logger.getLogger(getClass().getName())

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def apply(records: RDD[ArchiveRecord]) = {
      records
        .keepValidPages()
        .map(r => ExtractDomain(r.getUrl))
        .countItems()
  }

  def main(argv: Array[String]): Unit = {
    var args = new Conf(argv)

    log.info("Domain frequency extractor")
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Domain frequency extractor")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    RecordLoader.loadArchives(args.input(), sc).saveAsTextFile(args.output())
  }
}
