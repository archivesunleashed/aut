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
import org.apache.spark.sql.Row

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WriteGEXFTest extends FunSuite with BeforeAndAfter{
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"
  private val network = Seq((("Date1", "Source1", "Destination1"), 3),
                         (("Date2", "Source2", "Destination2"), 4),
                         (("Date3", "Source3", "Destination3"), 100))
  private val networkDf = Seq(("Date1", "Source1", "Destination1", 3),
                         ("Date2", "Source2", "Destination2", 4),
                         ("Date3", "Source3", "Destination3", 100))
  private val networkWithDuplication = Seq((("Date1", "Source1", "Destination1"), 3),
                         (("Date2", "Source2", "Source2"), 4),
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
    val testLines = (0, 12, 22, 34)
    val networkrdd = sc.parallelize(network)
    WriteGEXF(networkrdd, testFile)
    assert(Files.exists(Paths.get(testFile)))
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<node id="3" label="Destination1" />""")
    assert(lines(testLines._3) == """</attvalues>""")
    assert(lines(testLines._4) == """</edges>""")
  }

  test("creates the file from Array[Row]") {
    val testLines = (0, 12, 22, 34)
    if (Files.exists(Paths.get(testFile))) {
      new File(testFile).delete()
    }
    val networkarray = Array(Row.fromTuple(networkDf(0)),
      Row.fromTuple(networkDf(1)), Row.fromTuple(networkDf(2)))
    val ret = WriteGEXF(networkarray, testFile)
    assert(ret)
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<node id="8d3ab53ec817a1e5bf9ffd6e749b3983" label="Destination2" />""")
    assert(lines(testLines._3) == """</attvalues>""")
    assert(lines(testLines._4) == """</edges>""")
    assert(!WriteGEXF(networkarray ,""))
  }

  test ("returns a Bool depending on pass or failure") {
    val networkrdd = sc.parallelize(network)
    val gexf = WriteGEXF(networkrdd, testFile)
    assert(gexf)
    assert(!WriteGEXF(networkrdd, ""))
  }

  test ("Nodes zip with ids") {
    val networkrdd = sc.parallelize(networkWithDuplication)
    val expected = WriteGEXF.nodesWithIds(networkrdd).collect
    assert (expected.length == 5)
    assert (expected(0) == ("Source3", 0))
  }

  test ("Nodelookup returns a option") {
    val networkrdd = sc.parallelize(network)
    val nodes = WriteGEXF.nodesWithIds(networkrdd)
    val lookup = "Source1"
    val badlookup = "NOTHERE"
    assert (WriteGEXF.nodeLookup(nodes, badlookup) == None)
    assert (WriteGEXF.nodeLookup(nodes, lookup) == Some(("Source1", 6)))
  }

  test ("Gets the id from a lookup") {
    val nodes = WriteGEXF.nodesWithIds(sc.parallelize(network))
    val empty = -1
    val expected = 6
    val lookup = WriteGEXF.nodeLookup(nodes, "Source1")
    val badlookup = WriteGEXF.nodeLookup(nodes, "NOTTHERE")
    assert (WriteGEXF.nodeIdFromLabel(lookup) == expected)
    assert (WriteGEXF.nodeIdFromLabel(badlookup) == -1)
  }

  test ("Edge ids are captured from lookup") {
    val edges = WriteGEXF.edgeNodes(sc.parallelize(network))
    assert(edges.collect.deep == Array(("Date1", 6, 3, 3),
      ("Date2", 7, 4, 4),
      ("Date3", 0, 5, 100)).deep)
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
