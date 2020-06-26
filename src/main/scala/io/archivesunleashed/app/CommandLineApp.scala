/*
 * Copyright © 2017 The Archives Unleashed Project
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
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.ScallopConf

/* Usage:
 *
 * PATH_TO_SPARK
 *   --class io.archivesunleashed.app.CommandLinAppRunner
 *   PATH_TO_AUT_JAR
 *   --extractor EXTRACTOR
 *   --input INPUT_FILE ...
 *   --output OUTPUT_DIRECTORY
 *   [--output-format FORMAT]
 *   [--split]
 *   [--partiton]
 *
 * where EXTRACTOR is one of
 * AudioInformationExtractor, DomainFrequencyExtractor, DomainGraphExtractor,
 * ImageGraphExtractor, ImageInformationExtractor, PDFInformationExtractor,
 * PlainTextExtractor, PresentationProgramInformationExtractor,
 * SpreadsheetInformationExtractor, VideoInformationExtractor,
 * WebGraphExtractor, WebPagesExtractor, or WordProcessorInformationExtractor.
 *
 * INPUT_FILE is a list of input files separated by space (or path containing wildcard)
 * OUTPUT_DIRECTORY is the directory to put result in
 *
 * FORMAT is meant to work with DomainGraphExtractor
 * Four supported options are csv (default), and gexf, graphml as
 * additional options for DomainGraphExtractor.
 *
 * If --split is present, the program will put results for each input file in its own folder.
 * Otherwise they will be merged.
 *
 * If --partition N is present, the program will partition the DataFrame according
 * to N before writing results. Otherwise, the partition is left as is.
 */

/** Construct a Scallop option reader from command line argument string list.
  *
  * @param args list of command line arguments passed as is from argv
  */
class CmdAppConf(args: Seq[String]) extends ScallopConf(args) {

  /** Register callbacks when Scallop detects errors.
    * Useful in unit tests.
    *
    * @param e exception that Scallop throws
    */
  // scalastyle:off regex
  override def onError(e: Throwable): Unit =
    e match {
      case ScallopException(message) =>
        println(message)
        throw new IllegalArgumentException()
      case other: Any => throw other
    }
  // scalastyle:on regex

  mainOptions = Seq(input, output)
  var extractor = opt[String](descr = "extractor", required = true)
  val input = opt[List[String]](descr = "input file path", required = true)
  val output = opt[String](descr = "output directory path", required = true)
  val outputFormat = opt[String](descr =
    "output format for DomainGraphExtractor, one of csv, gexf, or graphml"
  )
  val split = opt[Boolean]()
  val partition = opt[Int]()
  verify()
}

/** Main application that parse command line arguments and invoke appropriate extractor.
  *
  * @param conf Scallop option reader constructed with class CmdAppConf
  */
class CommandLineApp(conf: CmdAppConf) {
  private val logger = Logger.getLogger(getClass().getName())
  private val configuration = conf
  private var saveTarget = ""
  private var sparkCtx: Option[SparkContext] = None

  /** Maps extractor type string to DataFrame Extractors.
    *
    * Each closure takes a list of file names to be extracted, loads them using RecordLoader,
    * performs the extraction, and saves results to file by calling save method of
    * CommandLineApp class.
    * Closures return nothing.
    */
  private val extractors = Map[String, List[String] => Any](
    "AudioInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).audio()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).audio())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(AudioInformationExtractor(df))
        } else {
          saveCsv(AudioInformationExtractor(df))
        }
      }),
    "DomainFrequencyExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).webpages()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).webpages())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(DomainFrequencyExtractor(df))
        } else {
          saveCsv(DomainFrequencyExtractor(df))
        }
      }),
    "DomainGraphExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).webgraph()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).webgraph())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "gexf") {
          new File(saveTarget).mkdirs()
          WriteGEXF(
            DomainGraphExtractor(df).collect(),
            Paths.get(saveTarget).toString + "/GEXF.gexf"
          )
        } else if (!configuration.outputFormat.isEmpty && configuration
                     .outputFormat() == "parquet") {
          saveParquet(DomainGraphExtractor(df))
        } else if (!configuration.outputFormat.isEmpty && configuration
                     .outputFormat() == "graphml") {
          new File(saveTarget).mkdirs()
          WriteGraphML(
            DomainGraphExtractor(df).collect(),
            Paths.get(saveTarget).toString + "/GRAPHML.graphml"
          )
        } else {
          saveCsv(DomainGraphExtractor(df))
        }
      }),
    "ImageInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).images()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).images())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(ImageInformationExtractor(df))
        } else {
          saveCsv(ImageInformationExtractor(df))
        }
      }),
    "ImageGraphExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).imagegraph()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).imagegraph())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(ImageGraphExtractor(df))
        } else {
          saveCsv(ImageGraphExtractor(df))
        }
      }),
    "PDFInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).pdfs()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).pdfs())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(PDFInformationExtractor(df))
        } else {
          saveCsv(PDFInformationExtractor(df))
        }
      }),
    "PlainTextExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).webpages()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).webpages())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(PlainTextExtractor(df))
        } else {
          saveCsv(PlainTextExtractor(df))
        }
      }),
    "PresentationProgramInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader
          .loadArchives(inputFiles.head, sparkCtx.get)
          .presentationProgramFiles()
        inputFiles.tail foreach { f =>
          df = df.union(
            RecordLoader
              .loadArchives(f, sparkCtx.get)
              .presentationProgramFiles()
          )
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(PresentationProgramInformationExtractor(df))
        } else {
          saveCsv(PresentationProgramInformationExtractor(df))
        }
      }),
    "SpreadsheetInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader
          .loadArchives(inputFiles.head, sparkCtx.get)
          .spreadsheets()
        inputFiles.tail foreach { f =>
          df =
            df.union(RecordLoader.loadArchives(f, sparkCtx.get).spreadsheets())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(SpreadsheetInformationExtractor(df))
        } else {
          saveCsv(SpreadsheetInformationExtractor(df))
        }
      }),
    "VideoInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).videos()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).videos())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(VideoInformationExtractor(df))
        } else {
          saveCsv(VideoInformationExtractor(df))
        }
      }),
    "WebGraphExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).webgraph()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).webgraph())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(WebGraphExtractor(df))
        } else {
          saveCsv(WebGraphExtractor(df))
        }
      }),
    "WebPagesExtractor" ->
      ((inputFiles: List[String]) => {
        var df =
          RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).webpages()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).webpages())
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(WebPagesExtractor(df))
        } else {
          saveCsv(WebPagesExtractor(df))
        }
      }),
    "WordProcessorInformationExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader
          .loadArchives(inputFiles.head, sparkCtx.get)
          .wordProcessorFiles()
        inputFiles.tail foreach { f =>
          df = df.union(
            RecordLoader.loadArchives(f, sparkCtx.get).wordProcessorFiles()
          )
        }
        if (!configuration.outputFormat.isEmpty && configuration
              .outputFormat() == "parquet") {
          saveParquet(WordProcessorInformationExtractor(df))
        } else {
          saveCsv(WordProcessorInformationExtractor(df))
        }
      })
  )

  /** Routine for saving Dataset obtained from querying DataFrames to CSV.
    * Files may be merged according to options specified in 'partition' setting.
    *
    * @param d generic dataset obtained from querying DataFrame
    * @return Unit
    */
  def saveCsv(d: Dataset[Row]): Unit = {
    if (!configuration.partition.isEmpty) {
      d.coalesce(configuration.partition())
        .write
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .option("header", "true")
        .csv(saveTarget)
    } else {
      d.write
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .csv(saveTarget)
    }
  }

  /** Routine for saving Dataset obtained from querying DataFrames to Parquet.
    * Files may be merged according to options specified in 'partition' setting.
    *
    * @param d generic dataset obtained from querying DataFrame
    * @return Unit
    */
  def saveParquet(d: Dataset[Row]): Unit = {
    if (!configuration.partition.isEmpty) {
      d.coalesce(configuration.partition())
        .write
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .parquet(saveTarget)
    } else {
      d.write
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .parquet(saveTarget)
    }
  }

  /** Verify the validity of command line arguments regarding input and output files.
    *
    * All input files need to exist, and ouput files should not exist, for this to pass.
    * Throws exception if condition is not met.
    * @return Unit
    * @throws IllegalArgumentException exception thrown
    */
  def verifyArgumentsOrExit(): Unit = {
    configuration.input() foreach { f =>
      if (!Files.exists(Paths.get(f))) {
        logger.error(f + " not found")
        throw new IllegalArgumentException()
      }
    }

    if (Files.exists(Paths.get(configuration.output()))) {
      logger.error(configuration.output() + " already exists")
      throw new IllegalArgumentException()
    }
  }

  /** Set the app name.
    *
    * @return String
    */
  def setAppName(): String = {
    "aut - " + configuration.extractor()
  }

  /** Prepare for invoking extractors.
    *
    * @return Any
    */
  def handler(): Any = {
    if (!(extractors contains configuration.extractor())) {
      logger.error(
        configuration.extractor() + " not supported. " +
          "The following extractors are supported: "
      )
      extractors foreach { tuple =>
        logger.error(tuple._1)
      }
      throw new IllegalArgumentException()
    }

    val extractFunction: List[String] => Any =
      extractors get configuration.extractor() match {
        case Some(func) => func
        case None =>
          throw new InternalError()
      }

    if (!configuration.split.isEmpty && configuration.split()) {
      configuration.input() foreach { f =>
        saveTarget = Paths
          .get(configuration.output(), Paths.get(f).getFileName.toString)
          .toString
        extractFunction(List[String](f))
      }
    } else {
      saveTarget = Paths.get(configuration.output()).toString
      extractFunction(configuration.input())
    }
  }

  /** Process the handler.
    *
    * @return Any
    */
  def process(): Any = {
    handler()
  }

  /** Set Spark context to be used.
    *
    * @param sc either a brand new or existing Spark context
    */
  def setSparkContext(sc: SparkContext): Unit = {
    sparkCtx = Some(sc)
  }
}

object CommandLineAppRunner {

  /** Entry point for command line application.
    *
    * @param argv command line arguments passed by the OS
    */
  def main(argv: Array[String]): Unit = {
    val app = new CommandLineApp(new CmdAppConf(argv))

    try {
      app.verifyArgumentsOrExit()
    } catch {
      case e: IllegalArgumentException => System.exit(1)
      case x: Throwable                => throw x
    }

    val appName = app.setAppName()
    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true")
    app.setSparkContext(new SparkContext(conf))
    app.process()
  }

  /** Entry point for testing.
    * Takes an existed spark session to prevent new ones from being created.
    *
    * @param argv command line arguments (array of strings).
    * @param sc spark context to be used for this session
    */
  def test(argv: Array[String], sc: SparkContext): Unit = {
    val app = new CommandLineApp(new CmdAppConf(argv))

    app.verifyArgumentsOrExit()
    app.setSparkContext(sc)
    app.process()
  }
}
