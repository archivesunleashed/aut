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
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WriteGraphMLTest extends FunSuite with BeforeAndAfter {
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"
  private val linkCountOne = 3
  private val linkCountTwo = 4
  private val linkCountThree = 100
  private val network = Seq(
    ("Date1", "Source1", "Destination1", linkCountOne),
    ("Date2", "Source2", "Destination2", linkCountTwo),
    ("Date3", "Source3", "Destination3", linkCountThree)
  )
  private val testFile = "temporaryTestFile.graphml"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("Create WriteGraphML file from Array[Row]") {
    val lineCheck = (0, 15, 22, 30)
    if (Files.exists(Paths.get(testFile))) {
      new File(testFile).delete()
    }
    val networkarray = Array(
      Row.fromTuple(network(0)),
      Row.fromTuple(network(1)),
      Row.fromTuple(network(2))
    )
    val ret = WriteGraphML(networkarray, testFile)
    assert(ret)
    assert(Files.exists(Paths.get(testFile)))
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(lineCheck._1) == """<?xml version="1.0" encoding="UTF-8"?>""")
    assert(lines(lineCheck._2) == """<data key="label">Destination2</data>""")
    assert(lines(lineCheck._3) == """</node>""")
    assert(lines(lineCheck._4) == """<data key="weight">3</data>""")
  }

  test("Test if GraphML path is empty") {
    val networkarray = Array(
      Row.fromTuple(network(0)),
      Row.fromTuple(network(1)),
      Row.fromTuple(network(2))
    )
    val graphml = WriteGraphML(networkarray, testFile)
    assert(graphml)
    assert(!WriteGraphML(networkarray, ""))
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
