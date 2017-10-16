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
package io.archivesunleashed.spark.matchbox


import org.apache.spark.{ SparkConf, SparkContext }
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfter, FunSuite }
import java.io.File
import java.nio.file.{Paths, Files}
import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.rdd.RecordRDD._
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class WriteGDFTest extends FunSuite with BeforeAndAfter{
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
      sc = new SparkContext(conf)
    }

  test("creates the file") {
    val networkrdd = sc.parallelize(network)
    WriteGDF(networkrdd, testFile)
    assert(Files.exists(Paths.get(testFile)) == true)
    assert(WriteGDF(networkrdd, "") == ())
    val lines = Source.fromFile(testFile).getLines.toList
    assert(lines(0) == "nodedef> name VARCHAR")
    assert(lines.contains("Destination1"))
    assert(lines.contains("Destination2"))
    assert(lines.contains("Destination3"))
    assert(lines.contains("Source1"))
    assert(lines.contains("Source2"))
    assert(lines.contains("Source3"))
    assert(lines(7) == "edgedef> source VARCHAR, target VARCHAR, weight DOUBLE, timeint VARCHAR")
    assert(lines.contains("Source3,Destination3,100,Date3"))
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
