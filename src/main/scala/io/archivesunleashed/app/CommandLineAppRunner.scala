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

import io.archivesunleashed.RecordLoader
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/* Usage:
 *
 * PATH_TO_SPARK 
 *   --class io.archivesunleashed.app.CommandLineAppRunner 
 *   PATH_TO_AUT_JAR
 *   EXTRACTOR
 *   INPUT_FILE
 *   OUTPUT_DIRECTORY
 *   
 * where EXTRACTOR is one of domainFreq, domainGraph or plainText
 */

object CommandLineAppRunner {
  var log = Logger.getLogger(getClass().getName())

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output)
    var extractor = opt[String](descr =
      "extractor, one of domainFreq, domainGraph or plainText", required = true)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def main(argv: Array[String]): Unit = {
    print(argv(0))
    var args = new Conf(argv)

    log.info("Extractor: " + args.extractor())
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName(args.extractor() + " extractor")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    var archive = RecordLoader.loadArchives(args.input(), sc)

    args.input() match {
      case "domainFreq" =>
        DomainFrequencyExtractor(archive).saveAsTextFile(args.output())
      case "domainGraph" =>
        DomainGraphExtractor(archive).saveAsTextFile(args.output())
      case "plainText" =>
        PlainTextExtractor(archive).saveAsTextFile(args.output())
      case _ => log.error(args.extractor() + " not supported")
    }
  }
}
