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
class WriteGraphTest extends FunSuite with BeforeAndAfter{
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
    WriteGraph.asGexf(networkrdd, testFile)
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
    val ret = WriteGraph.asGexf(networkarray, testFile)
    assert(ret)
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<node id="8d3ab53ec817a1e5bf9ffd6e749b3983" label="Destination2" />""")
    assert(lines(testLines._3) == """</attvalues>""")
    assert(lines(testLines._4) == """</edges>""")
    assert(!WriteGraph.asGexf(networkarray ,""))
  }

  test ("returns a Bool depending on pass or failure") {
    val networkrdd = sc.parallelize(network)
    val gexf = WriteGraph.asGexf(networkrdd, testFile)
    assert(gexf)
    assert(!WriteGraph.asGexf(networkrdd, ""))
  }

  test ("Nodes zip with ids") {
    val networkrdd = sc.parallelize(networkWithDuplication)
    val nodeIds = WriteGraph.nodesWithIds(networkrdd).collect
    val expected = ("Source3", 0)
    val expectedLength = 5
    assert (nodeIds.length == expectedLength)
    assert (nodeIds(0) == expected)
  }

  test ("Nodelookup returns a option") {
    val networkrdd = sc.parallelize(network)
    val nodes = WriteGraph.nodesWithIds(networkrdd)
    val lookup = "Source1"
    val badlookup = "NOTHERE"
    assert (WriteGraph.nodeLookup(nodes, badlookup) == None)
    assert (WriteGraph.nodeLookup(nodes, lookup) == Some((lookup, 6)))
  }

  test ("Gets the id from a lookup") {
    val nodes = WriteGraph.nodesWithIds(sc.parallelize(network))
    val empty = -1
    val expected = 6
    val lookup = WriteGraph.nodeLookup(nodes, "Source1")
    val badlookup = WriteGraph.nodeLookup(nodes, "NOTTHERE")
    assert (WriteGraph.nodeIdFromLabel(lookup) == expected)
    assert (WriteGraph.nodeIdFromLabel(badlookup) == empty)
  }

  test ("Edge ids are captured from lookup") {
    val edges = WriteGraph.edgeNodes(sc.parallelize(network))
    val expected = Array(("Date1", 6, 3, 3),
      ("Date2", 7, 4, 4),
      ("Date3", 0, 5, 100)).deep
    assert(edges.collect.deep == expected)
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
