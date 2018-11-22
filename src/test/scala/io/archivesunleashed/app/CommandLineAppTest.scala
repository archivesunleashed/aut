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
  private val dfOpt = "--df"
  private val plainTextOpt = "PlainTextExtractor"
  private val domainGraphOpt = "DomainGraphExtractor"
  private var sc: SparkContext = _
  private val testSuccessCmds = Array(
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, "--output-format", "GEXF"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, "DomainFrequencyExtractor", dfOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, dfOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, domainGraphOpt, dfOpt, "--output-format", "GEXF"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, dfOpt),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, dfOpt, "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, dfOpt, "--partition", "1"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, extractOpt, plainTextOpt, "--split"),
    Array(inputOpt, arcPath, warcPath, outputOpt, outputDir, "--partition", "1", extractOpt, plainTextOpt)
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

  test("command line app tests") {
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
