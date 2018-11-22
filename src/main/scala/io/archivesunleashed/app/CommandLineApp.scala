/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
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
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.ScallopException

/* Usage:
 *
 * PATH_TO_SPARK
 *   --class io.archivesunleashed.app.CommandLinAppRunner
 *   PATH_TO_AUT_JAR
 *   --extractor EXTRACTOR
 *   --input INPUT_FILE ...
 *   --output OUTPUT_DIRECTORY
 *   [--output-format FORMAT]
 *   [--df]
 *   [--split]
 *   [--partiton]
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
 * If --df is present, the program will use data frame to carry out analysis
 *
 * If --split is present, the program will put results for each input file in its own folder. Otherwise they will be merged.
 *
 * If --partition N is present, the program will partition RDD or Data Frame according to N before writing results.
 * Otherwise, the partition is left as is.
 */

/** Construct a Scallop option reader from command line argument string list
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
  override def onError(e: Throwable): Unit = e match {
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
    "output format for DomainGraphExtractor, one of TEXT or GEXF")
  val split = opt[Boolean]()
  val df = opt[Boolean]()
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
  private var sparkCtx : Option[SparkContext] = None

  /** Maps extractor type string to RDD extractors.
    *
    * Each closure takes a RDD[ArchiveRecord] obtained from RecordLoader, performs the extraction, and
    * saves results to file by calling save method of CommandLineApp class.
    * Closures return nothing.
    */

  private val rddExtractors = Map[String, RDD[ArchiveRecord] => Any](
    "DomainFrequencyExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        save(DomainFrequencyExtractor(rdd))
      }),
    "DomainGraphExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        if (!configuration.outputFormat.isEmpty && configuration.outputFormat() == "GEXF") {
          new File(saveTarget).mkdirs()
          WriteGEXF(DomainGraphExtractor(rdd), Paths.get(saveTarget).toString + "/GEXF.xml")
        } else {
          save(DomainGraphExtractor(rdd))
        }
      }),
    "PlainTextExtractor" ->
      ((rdd: RDD[ArchiveRecord]) => {
        save(PlainTextExtractor(rdd))
      })
  )

  /** Maps extractor type string to Data Frame Extractors.
    *
    * Each closure takes a list of file names to be extracted, loads them using RecordLoader,
    * performs the extraction, and saves results to file by calling save method of
    * CommandLineApp class.
    * Closures return nothing.
    */

  private val dfExtractors = Map[String, List[String] => Any](
    "DomainFrequencyExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).extractValidPagesDF()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).extractValidPagesDF())
        }
        save(DomainFrequencyExtractor(df))
      }),
    "DomainGraphExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).extractHyperlinksDF()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).extractHyperlinksDF())
        }
        if (!configuration.outputFormat.isEmpty && configuration.outputFormat() == "GEXF") {
          new File(saveTarget).mkdirs()
          WriteGEXF(DomainGraphExtractor(df).collect(), Paths.get(saveTarget).toString + "/GEXF.xml")
        } else {
          save(DomainGraphExtractor(df))
        }
      }),
    "PlainTextExtractor" ->
      ((inputFiles: List[String]) => {
        var df = RecordLoader.loadArchives(inputFiles.head, sparkCtx.get).extractValidPagesDF()
        inputFiles.tail foreach { f =>
          df = df.union(RecordLoader.loadArchives(f, sparkCtx.get).extractValidPagesDF())
        }
        save(PlainTextExtractor(df))
      })
  )

  /** Generic routine for saving Dataset obtained from querying Data Frames to file.
    * Files may be merged according to options specified in 'partition' setting.
    *
    * @param d generic dataset obtained from querying Data Frame
    * @return Unit
    */

  def save(d: Dataset[Row]): Unit = {
    if (!configuration.partition.isEmpty) {
      d.coalesce(configuration.partition()).write.csv(saveTarget)
    } else {
      d.write.csv(saveTarget)
    }
  }

  /** Generic routine for saving RDD obtained from Map Reduce operation of extractors.
    *
    * @param r RDD obtained by applying RDD extractors to original RDD
    * @tparam T template class name for RDD. Not used.
    * @return Unit
    */

  def save[T](r: RDD[T]): Unit = {
    if (!configuration.partition.isEmpty) {
      r.coalesce(configuration.partition()).saveAsTextFile(saveTarget)
    } else {
      r.saveAsTextFile(saveTarget)
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
      }}

    if (Files.exists(Paths.get(configuration.output()))) {
      logger.error(configuration.output() + " already exists")
      throw new IllegalArgumentException()
    }
  }

  /** Prepare for invoking Data Frame implementation of extractors.
    *
    * @return Any
    */

  def dfHandler(): Any = {
    if (!(dfExtractors contains configuration.extractor())) {
      logger.error(configuration.extractor() + " not supported with data frame. " +
        "The following extractors are supported: ")
      dfExtractors foreach { tuple => logger.error(tuple._1) }
      throw new IllegalArgumentException()
    }

    val extractFunction: List[String] => Any =
      dfExtractors get configuration.extractor() match {
        case Some(func) => func
        case None =>
          throw new InternalError()
      }

    if (!configuration.split.isEmpty && configuration.split()) {
      configuration.input() foreach { f =>
        saveTarget =  Paths.get(configuration.output(), Paths.get(f).getFileName.toString).toString
        extractFunction(List[String](f))
      }
    } else {
      saveTarget = Paths.get(configuration.output()).toString
      extractFunction(configuration.input())
    }
  }

  /** Prepare for invoking RDD implementation of extractors.
    *
    * @return Any
    */

  def rddHandler(): Any = {
    if (!(rddExtractors contains configuration.extractor())) {
      logger.error(configuration.extractor() +
      " not supported with RDD. The following extractors are supported: ")
      rddExtractors foreach { tuple => logger.error(tuple._1) }
      throw new IllegalArgumentException()
    }

    val extractFunction: RDD[ArchiveRecord] => Any =
      rddExtractors get configuration.extractor() match {
        case Some(func) => func
        case None =>
          throw new InternalError()
      }

    if (!configuration.split.isEmpty && configuration.split()) {
      configuration.input() foreach { f =>
        val archive = RecordLoader.loadArchives(f, sparkCtx.get)
        saveTarget =  Paths.get(configuration.output(), Paths.get(f).getFileName.toString).toString
        extractFunction(archive)
      }
    } else {
      var nameList = configuration.input()
      var rdd =  RecordLoader.loadArchives(nameList.head, sparkCtx.get)
      nameList.tail foreach { f =>
        rdd = rdd.union(RecordLoader.loadArchives(f, sparkCtx.get))
      }
      saveTarget = Paths.get(configuration.output()).toString
      extractFunction(rdd)
    }
  }

  /** Choose either Data Frame implementation or RDD implementation of extractors
    * depending on the option specified in command line arguments.
    *
    * @return Any
    */
  def process(): Any = {
    if (!configuration.df.isEmpty && configuration.df()) {
      dfHandler()
    } else {
      rddHandler()
    }
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
      case x: Throwable => throw x
    }

    val conf = new SparkConf().setAppName("AUTCommandLineApp")
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
