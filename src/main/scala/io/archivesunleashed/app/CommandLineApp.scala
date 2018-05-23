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
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/* Usage:
 *
 * PATH_TO_SPARK 
 *   --class io.archivesunleashed.app.CmdAppMain
 *   PATH_TO_AUT_JAR
 *   --extractor EXTRACTOR
 *   --input INPUT_FILE ...
 *   --output OUTPUT_DIRECTORY
 *   [--output-format FORMAT]
 *   [--use-df]
 *   
 * where EXTRACTOR is one of
 * DomainFrequencyExtractor, DomainGraphExtractor or PlainTextExtractor
 *
 * INPUT_FILE is a list of input files separated by space (or path containing wildcard)
 * OUTPUT_DIRECTORY is the directory to put result in
 *
 * FORMAT is meant to work with DomainGraphExtractor
 * Two supported options are TEXT (default) or GEXF
 *
 * If --use-df is present, the program will use data frame to carry out analysis
 */

class CmdAppConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  var extractor = opt[String](descr = "extractor", required = true)
  val input = opt[List[String]](descr = "input file path", required = true)
  val output = opt[String](descr = "output directory path", required = true)
  val outputFormat = opt[String](descr =
    "output format for DomainGraphExtractor, one of TEXT or GEXF")
  val useDF = opt[Boolean]()
  verify()
}

class CommandLineApp(conf: CmdAppConf, ctx: SparkContext) {
  private val logger = Logger.getLogger(getClass().getName())
  private val configuration = conf
  private var saveTarget = ""
  private val sparkCtx = ctx

  private val rddExtractors = Map[String, RDD[ArchiveRecord] => Any](
    "DomainFrequencyExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        DomainFrequencyExtractor(rdd).saveAsTextFile(saveTarget)}),
    "DomainGraphExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        if (!configuration.outputFormat.isEmpty && configuration.outputFormat() == "GEXF") {
          new File(saveTarget).mkdirs()
          WriteGEXF(DomainGraphExtractor(rdd), Paths.get(saveTarget).toString + "/GEXF.xml")
        } else {
          DomainGraphExtractor(rdd).saveAsTextFile(saveTarget)
        }
      }),
    "PlainTextExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        PlainTextExtractor(rdd).saveAsTextFile(saveTarget)}))

  private var dfExtractors = Map[String, String => Any](
    "DomainFrequencyExtractor" ->
      ((inputFile: String) => {
        val df = RecordLoader.loadArchives(inputFile, sparkCtx).extractValidPagesDF()
        DomainFrequencyExtractor(df).write.csv(saveTarget)
      }),
    "DomainGraphExtractor" ->
      ((inputFile: String) => {
        val df = RecordLoader.loadArchives(inputFile, sparkCtx).extractHyperlinksDF()

        if (!configuration.outputFormat.isEmpty && configuration.outputFormat() == "GEXF") {
          new File(saveTarget).mkdirs()
          WriteGEXF(DomainGraphExtractor(df), Paths.get(saveTarget).toString + "/GEXF.xml")
        } else {
          DomainGraphExtractor(df).write.csv(saveTarget)
        }
      }),
    "PlainTextExtractor" ->
      ((inputFile: String) => {
        val df = RecordLoader.loadArchives(inputFile, sparkCtx).extractValidPagesDF()
        PlainTextExtractor(df)}))

  def verifyArgumentsOrExit() = {
    configuration.input() foreach { f =>
      if (!Files.exists(Paths.get(f))) {
        logger.error(f + " not found")
        System.exit(1)
      }}

    if (Files.exists(Paths.get(configuration.output()))) {
      logger.error(configuration.output() + " already exists")
      System.exit(1)
    }
  }

  def dfHandler() = {
    if (!(dfExtractors contains configuration.extractor())) {
      logger.error(configuration.extractor() + " not supported with data frame. " +
        "The following extractors are supported: ")
      dfExtractors foreach { tuple => logger.error(tuple._1) }
      System.exit(1)
    }

    val extractFunction: String => Any =
      dfExtractors get configuration.extractor() match {
        case Some(func) => func
        case None =>
          throw new InternalError()
      }

    configuration.input() foreach { f =>
      saveTarget =  Paths.get(configuration.output(), Paths.get(f).getFileName.toString).toString
      extractFunction(f)
    }
  }

  def rddHandler() = {
    if (!(rddExtractors contains configuration.extractor())) {
      logger.error(configuration.extractor() + " not supported with RDD. The following extractors are supported: ")
      rddExtractors foreach { tuple => logger.error(tuple._1) }
      System.exit(1)
    }

    val extractFunction: RDD[ArchiveRecord] => Any =
      rddExtractors get configuration.extractor() match {
        case Some(func) => func
        case None =>
          throw new InternalError()
      }

    configuration.input() foreach { f =>
      val archive = RecordLoader.loadArchives(f, sparkCtx)
      saveTarget =  Paths.get(configuration.output(), Paths.get(f).getFileName.toString).toString
      extractFunction(archive)
    }
  }

  def process() = {
    if (!configuration.useDF.isEmpty && configuration.useDF()) {
      dfHandler()
    } else {
      rddHandler()
    }
  }
}

object CmdAppMain {
  def main(argv: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AUTCommandLineApp")
    conf.set("spark.driver.allowMultipleContexts", "true")

    val app = new CommandLineApp(new CmdAppConf(argv), new SparkContext(conf))

    app.verifyArgumentsOrExit()
    app.process()
  }
}
