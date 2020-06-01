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

package io.archivesunleashed

import java.io.File
import java.lang.IllegalArgumentException
import java.nio.file.{Files, Paths}

import com.google.common.io.Resources
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class CommandLineAppTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private var outputDir = "_AUTCmdTestOutputDir"
  private val master = "local[4]"
  private val appName = "example-df"
  private val inputOpt = "--input"
  private val outputOpt = "--output"
  private val extractOpt = "--extractor"
  private val plainTextOpt = "PlainTextExtractor"
  private val domainGraphOpt = "DomainGraphExtractor"
  private val imageGraphOpt = "ImageGraphExtractor"
  private val webPagesOpt = "WebPagesExtractor"
  private var sc: SparkContext = _
  private val testSuccessCmds = Array(
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, "--output-format", "gexf"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, "--output-format", "graphml"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, "--partition", "1", extractOpt, plainTextOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, imageGraphOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, imageGraphOpt, "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, imageGraphOpt, "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, imageGraphOpt, "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, imageGraphOpt, "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, webPagesOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, webPagesOpt, "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, webPagesOpt, "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, webPagesOpt, "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, webPagesOpt, "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "AudioInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "AudioInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "AudioInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "AudioInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "AudioInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "ImageInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "ImageInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "ImageInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "ImageInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "ImageInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PDFInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PDFInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PDFInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PDFInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PDFInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PresentationProgramInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PresentationProgramInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PresentationProgramInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PresentationProgramInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "PresentationProgramInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "SpreadsheetInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "SpreadsheetInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "SpreadsheetInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "SpreadsheetInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "SpreadsheetInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "VideoInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "VideoInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "VideoInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "VideoInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "VideoInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WordProcessorInformationExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WordProcessorInformationExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WordProcessorInformationExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WordProcessorInformationExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WordProcessorInformationExtractor", "--output-format", "parquet", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WebGraphExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WebGraphExtractor", "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WebGraphExtractor", "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WebGraphExtractor", "--output-format", "parquet"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "WebGraphExtractor", "--output-format", "parquet", "--partition", "1")
  )

  private val testFailCmds = Array(
    Array(inputOpt, "_abracadabra", outputOpt, outputDir),
    Array(outputOpt, outputDir),
    Array(inputOpt, "_abracadabra"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "abracadabra")
  )

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Command line app functionality tests") {
    for {a <- testSuccessCmds} {
      app.CommandLineAppRunner.test(a, sc)
      assert(Files.exists(Paths.get(outputDir)))
      FileUtils.deleteDirectory(new File(outputDir))
    }

    for {a <- testFailCmds}  {
      try {
        app.CommandLineAppRunner.test(a, sc)
        assert(false)
      } catch {
        case e: IllegalArgumentException => assert(true)
        case _: Throwable => assert(false)
      } finally {
        assert(!Files.exists(Paths.get(outputDir)))
      }
    }
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    if (Files.exists(Paths.get(outputDir))) {
      FileUtils.deleteDirectory(new File(outputDir))
    }
  }
}
