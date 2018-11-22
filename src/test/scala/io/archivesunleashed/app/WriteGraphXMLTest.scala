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

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
// scalastyle:off underscore.import
import org.apache.spark.graphx._
// scalastyle:on underscore.import
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WriteGraphXMLTest extends FunSuite with BeforeAndAfter{
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"
  private val network = Seq(("Source1", "Destination1"),
                         ("Source2", "Destination2"),
                         ("Source3", "Destination3"))
  private val testFile = "temporaryTestFile.txt"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      conf.set("spark.driver.allowMultipleContexts", "true");
      sc = new SparkContext(conf)
    }

  test("creates the file") {
    val headerLocation = 0
    val expectedLine = 13
    val networkrdd = ExtractGraphX.extractGraphX(sc.parallelize(network))
    val pRank = ExtractGraphX.runPageRankAlgorithm(networkrdd)
    WriteGraphXML(pRank, testFile)
    assert(Files.exists(Paths.get(testFile)))
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(headerLocation) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(expectedLine) == """<nodes>""")
  }

  test ("returns a Bool depending on pass or failure") {
    val networkrdd = ExtractGraphX.extractGraphX(sc.parallelize(network))
    val pRank = ExtractGraphX.runPageRankAlgorithm(networkrdd)
    val graphml = WriteGraphXML(pRank, testFile)
    assert(graphml)
    assert(!WriteGraphXML(pRank, ""))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    if (Files.exists(Paths.get(testFile))) {
      new File(testFile).delete()
    }
  }
}
