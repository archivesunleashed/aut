/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  /*
   * Usage:
   *
   * PATH_TO_SPARK_SUBMIT
   *   --class io.archivesunleashed.app.DomainFrequencyExtractor
   *   AUT_EXECUTABLE
   *   --input INPUT_WEB_ARCHIVE_FILE
   *   --output OUTPUT_DIRECTORY
   *
   * Example:
   *
   * ./spark/bin/spark-submit
   *   --class io.archivesunleashed.app.DomainFrequencyExtractor
   *   ./aut/aut-0.16.1-SNAPSHOT-fatjar.jar
   *   --input example.warc.gz
   *   --output extractedDomainFrequency
   */
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
