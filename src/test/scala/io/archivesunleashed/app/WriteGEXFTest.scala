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

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WriteGEXFTest extends FunSuite with BeforeAndAfter{
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"
  private val network = Seq((("Date1", "Source1", "Destination1"), 3),
                         (("Date2", "Source2", "Destination2"), 4),
                         (("Date3", "Source3", "Destination3"), 100))
  private val testFile = "temporaryTestFile.txt"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      conf.set("spark.driver.allowMultipleContexts", "true");
      sc = new SparkContext(conf)
    }

  test("creates the file") {
    val networkrdd = sc.parallelize(network)
    WriteGEXF(networkrdd, testFile)
    assert(Files.exists(Paths.get(testFile)) == true)
    val lines = Source.fromFile(testFile).getLines.toList
    println(lines)
    assert(lines(0) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(12) == """<node id="f61def1ec71cd27401b8c821f04b7c27" label="Destination1" />""")
    assert(lines(22) == """</attvalues>""")
    assert(lines(34) == """</edges>""")
  }

  test ("returns a Bool depending on pass or failure") {
    val networkrdd = sc.parallelize(network)
    val gexf = WriteGEXF(networkrdd, testFile)
    println (gexf)
    assert(gexf == true)
    assert(WriteGEXF(networkrdd, "") == false)
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
