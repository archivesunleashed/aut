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

import java.io.File
import java.nio.file.{Files, Paths}

import io.archivesunleashed.{ArchiveRecord, RecordLoader}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/* Usage:
 *
 * PATH_TO_SPARK 
 *   --class io.archivesunleashed.app.CommandLineAppRunner 
 *   PATH_TO_AUT_JAR
 *   --extractor EXTRACTOR
 *   --input INPUT_FILE ...
 *   --output OUTPUT_DIRECTORY
 *   [--output-format FORMAT]
 *   
 * where EXTRACTOR is one of
 * DomainFrequencyExtractor, DomainGraphExtractor or PlainTextExtractor
 *
 * INPUT_FILE is a list of input files separated by space (or path containing wildcard)
 * OUTPUT_DIRECTORY is the directory to put result in
 *
 * FORMAT is meant to work with DomainGraphExtractor
 * Two supported options are TEXT (default) or GEXF
 */

object CommandLineAppRunner {
  var log = Logger.getLogger(getClass().getName())

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output)
    var extractor = opt[String](descr =
      "extractor", required = true)
    val input = opt[List[String]](descr = "input file path", required = true)
    val output = opt[String](descr = "output directory path", required = true)
    val outputFormat = opt[String](descr =
      "output format for DomainGraphExtractor, one of TEXT or GEXF")
    verify()
  }

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)

    val extractors = Map[String, (RDD[ArchiveRecord], String) => Any](
      "DomainFrequencyExtractor" ->
        ((rdd: RDD[ArchiveRecord], subFolder: String) => {
          DomainFrequencyExtractor(rdd).saveAsTextFile(subFolder)}),
    "DomainGraphExtractor" ->
      ((rdd: RDD[ArchiveRecord], subFolder: String) => {
        if (!args.outputFormat.isEmpty && args.outputFormat() == "GEXF") {
          new File(subFolder).mkdirs()
          WriteGEXF(DomainGraphExtractor(rdd), Paths.get(subFolder).toString + "/GEXF.xml")
        } else {
          DomainGraphExtractor(rdd).saveAsTextFile(subFolder)
        }
      }),
    "PlainTextExtractor" ->
      ((rdd: RDD[ArchiveRecord], subFolder: String) => {
        PlainTextExtractor(rdd).saveAsTextFile(subFolder)}))

    if (!(extractors contains args.extractor())) {
      log.error(args.extractor() + " not supported. The following extractors are supported: ")
      extractors foreach { tuple => log.error(tuple._1) }
      System.exit(1)
    }

    args.input() foreach { f =>
      if (!Files.exists(Paths.get(f))) {
      log.error(f + " not found")
      System.exit(1)
    }}

    if (Files.exists(Paths.get(args.output()))) {
      log.error(args.output() + " already exists")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(args.extractor() + " extractor")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val extractFunction: (RDD[ArchiveRecord], String) => Any =
      extractors get args.extractor() match {
      case Some(func) => func
      case None =>
        throw new InternalError()
    }

    args.input() foreach { f =>
      var archive = RecordLoader.loadArchives(f, sc)
      extractFunction(archive, Paths.get(args.output(), Paths.get(f).getFileName.toString).toString)
    }
  }
}
