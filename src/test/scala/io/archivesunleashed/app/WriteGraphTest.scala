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
  private val unescapedNetwork = Seq((("Date1", "Source1", "Destination1"), 3),
                        (("Date2", "Source2", "Destination2"), 4),
                        (("Date3", "Source<3", "Destination<3"), 100))
  private val networkDf = Seq(("Date1", "Source1", "Destination1", 3),
                         ("Date2", "Source2", "Destination2", 4),
                         ("Date3", "Source3", "Destination3", 100))
  private val networkWithDuplication = Seq((("Date1", "Source1", "Destination1"), 3),
                         (("Date2", "Source2", "Source2"), 4),
                         (("Date3", "Source3", "Destination3"), 100))
  private val testFile = "temporaryTestFile.txt"
  private val testFile2 = "temporaryTestFile2.txt"

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
    assert(!WriteGraph.asGexf(networkarray, ""))
    assert(!WriteGraph(networkarray, ""))
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
    assert (WriteGraph.nodeIdFromLabel(Option(null)) == -1)
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

  test ("Graphml produces correct output") {
    val testLines = (0, 12, 30, 37)
    val networkrdd = sc.parallelize(network)
    WriteGraph.asGraphml(networkrdd, testFile)
    assert(Files.exists(Paths.get(testFile)))
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<data key="label">Source3</data>""")
    assert(lines(testLines._3) == """<data key="weight">3</data>""")
    assert(lines(testLines._4) == """<edge source="0" target="5" type="directed">""")
  }

  test ("Graphml works with unescaped xml data") {
    val testLines = (0, 12, 30, 37)
    val networkrdd = sc.parallelize(unescapedNetwork)
    WriteGraph.asGraphml(networkrdd, testFile)
    assert(Files.exists(Paths.get(testFile)))
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<data key="label">Destination&lt;3</data>""")
    assert(lines(testLines._3) == """<data key="weight">100</data>""")
    assert(lines(testLines._4) == """<edge source="7" target="4" type="directed">""")
  }

  test( "Gexf works with unescaped xml data") {
    val testLines = (0, 12, 29, 31)
    val networkrdd = sc.parallelize(unescapedNetwork)
    WriteGraph(networkrdd, testFile2)
    assert(Files.exists(Paths.get(testFile2)))
    val lines = Source.fromFile(testFile2).getLines.toList
    assert(lines(testLines._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(testLines._2) == """<node id="3" label="Source&lt;3" />""")
    assert(lines(testLines._3) == """<edge source="7" target="4" weight="4" type="directed">""")
    assert(lines(testLines._4) == """<attvalue for="0" value="Date2" />""")
  }

  test( "False on empty path") {
    val networkrdd = sc.parallelize(network)
    val emptyString = ""
    assert(!WriteGraph(networkrdd, emptyString))
    assert(!WriteGraph.asGraphml(networkrdd, emptyString))
    assert(!WriteGraph.asGexf(networkrdd, emptyString))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    if (Files.exists(Paths.get(testFile)) || Files.exists(Paths.get(testFile2))) {
      new File(testFile).delete()
      new File(testFile2).delete()
    }
  }
}
